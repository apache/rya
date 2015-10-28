package mvm.mmrts.rdf.partition.mr.compat;

import cloudbase.core.CBConstants;
import cloudbase.core.client.mapreduce.CloudbaseInputFormat;
import cloudbase.core.client.mapreduce.CloudbaseOutputFormat;
import cloudbase.core.data.Key;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.security.ColumnVisibility;
import mvm.mmrts.rdf.partition.PartitionConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * MMRTS-148 Need to move the shard index from the partition table to the shardIndex table
 * Class MoveShardIndexTool
 * Date: Dec 8, 2011
 * Time: 4:11:40 PM
 */
public class ChangeShardDateFormatTool implements Tool {
    public static final String CB_USERNAME_PROP = "cb.username";
    public static final String CB_PWD_PROP = "cb.pwd";
    public static final String CB_ZK_PROP = "cb.zk";
    public static final String CB_INSTANCE_PROP = "cb.instance";
    public static final String PARTITION_TABLE_PROP = "partition.table";
    public static final String OLD_DATE_FORMAT_PROP = "date.format.old";
    public static final String NEW_DATE_FORMAT_PROP = "date.format.new";
    public static final String OLD_DATE_SHARD_DELIM = "date.shard.delim.old";
    public static final String NEW_DATE_SHARD_DELIM = "date.shard.delim.new";


    private Configuration conf;

    private String userName = "root";
    private String pwd = "password";
    private String instance = "stratus";
    private String zk = "10.40.190.113:2181";
    private String partitionTable = "rdfPartition";
    private String oldDateFormat = "yyyy-MM";
    private String newDateFormat = "yyyyMMdd";
    private String oldDateDelim = "-";
    private String newDateDelim = "_";

    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new ChangeShardDateFormatTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        runJob(args);
        return 0;
    }

    public long runJob(String[] args) throws Exception {
        //faster
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("io.sort.mb", "256");

        zk = conf.get(CB_ZK_PROP, zk);
        instance = conf.get(CB_INSTANCE_PROP, instance);
        userName = conf.get(CB_USERNAME_PROP, userName);
        pwd = conf.get(CB_PWD_PROP, pwd);
        partitionTable = conf.get(PARTITION_TABLE_PROP, partitionTable);
        oldDateFormat = conf.get(OLD_DATE_FORMAT_PROP, oldDateFormat);
        newDateFormat = conf.get(NEW_DATE_FORMAT_PROP, newDateFormat);
        oldDateDelim = conf.get(OLD_DATE_SHARD_DELIM, oldDateDelim);
        newDateDelim = conf.get(NEW_DATE_SHARD_DELIM, newDateDelim);
        conf.set(NEW_DATE_FORMAT_PROP, newDateFormat);
        conf.set(OLD_DATE_FORMAT_PROP, oldDateFormat);
        conf.set(PARTITION_TABLE_PROP, partitionTable);
        conf.set(OLD_DATE_SHARD_DELIM, oldDateDelim);
        conf.set(NEW_DATE_SHARD_DELIM, newDateDelim);

        Job job = new Job(conf);
        job.setJarByClass(ChangeShardDateFormatTool.class);

        job.setInputFormatClass(CloudbaseInputFormat.class);
        //TODO: How should I send in Auths?
        CloudbaseInputFormat.setInputInfo(job, userName, pwd.getBytes(),
                partitionTable, CBConstants.NO_AUTHS);
        CloudbaseInputFormat.setZooKeeperInstance(job, instance, zk);

        job.setMapperClass(ChangeDateFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Mutation.class);

        job.setOutputFormatClass(CloudbaseOutputFormat.class);
        CloudbaseOutputFormat.setOutputInfo(job, userName, pwd.getBytes(), true, partitionTable);
        CloudbaseOutputFormat.setZooKeeperInstance(job, instance, zk);

        job.setNumReduceTasks(0);

        // Submit the job
        Date startTime = new Date();
        System.out.println("Job started: " + startTime);
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        if (exitCode == 0) {
            Date end_time = new Date();
            System.out.println("Job ended: " + end_time);
            System.out.println("The job took "
                    + (end_time.getTime() - startTime.getTime()) / 1000
                    + " seconds.");
            return job
                    .getCounters()
                    .findCounter("org.apache.hadoop.mapred.Task$Counter",
                            "REDUCE_OUTPUT_RECORDS").getValue();
        } else {
            System.out.println("Job Failed!!!");
        }

        return -1;
    }

    public static class ChangeDateFormatMapper extends Mapper<Key, Value, Text, Mutation> {
        private SimpleDateFormat oldDateFormat_df;
        private SimpleDateFormat newDateFormat_df;
        private Text partTableTxt;
        private String newDateDelim;
        private String oldDateDelim;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String oldDateFormat = context.getConfiguration().get(OLD_DATE_FORMAT_PROP);
            if (oldDateFormat == null)
                throw new IllegalArgumentException("Old Date Format property cannot be null");

            oldDateFormat_df = new SimpleDateFormat(oldDateFormat);

            String newDateFormat = context.getConfiguration().get(NEW_DATE_FORMAT_PROP);
            if (newDateFormat == null)
                throw new IllegalArgumentException("New Date Format property cannot be null");

            newDateFormat_df = new SimpleDateFormat(newDateFormat);

            String partTable = context.getConfiguration().get(PARTITION_TABLE_PROP);
            if (partTable == null)
                throw new IllegalArgumentException("Partition Table property cannot be null");

            partTableTxt = new Text(partTable);

            oldDateDelim = context.getConfiguration().get(OLD_DATE_SHARD_DELIM);
            if (oldDateDelim == null)
                throw new IllegalArgumentException("Old Date Shard Delimiter property cannot be null");

            newDateDelim = context.getConfiguration().get(NEW_DATE_SHARD_DELIM);
            if (newDateDelim == null)
                throw new IllegalArgumentException("New Date Shard Delimiter property cannot be null");

        }

        @Override
        protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
            try {
                String cf = key.getColumnFamily().toString();
                if ("event".equals(cf) || "index".equals(cf)) {
                    String shard = key.getRow().toString();
                    int shardIndex = shard.lastIndexOf(oldDateDelim);
                    if (shardIndex == -1)
                        return; //no shard?
                    String date_s = shard.substring(0, shardIndex);
                    String shardValue = shard.substring(shardIndex + 1, shard.length());

                    Date date = oldDateFormat_df.parse(date_s);
                    String newShard = newDateFormat_df.format(date) + newDateDelim + shardValue;

                    Mutation mutation = new Mutation(new Text(newShard));
                    mutation.put(key.getColumnFamily(), key.getColumnQualifier(),
                            new ColumnVisibility(key.getColumnVisibility()), System.currentTimeMillis(), value);
                    context.write(partTableTxt, mutation);

                    //delete
                    mutation = new Mutation(key.getRow());
                    mutation.putDelete(key.getColumnFamily(), key.getColumnQualifier(), System.currentTimeMillis());

                    context.write(partTableTxt, mutation);
                } else {
                    //shard index
                    String shard = key.getColumnFamily().toString();
                    int shardIndex = shard.lastIndexOf(oldDateDelim);
                    if (shardIndex == -1)
                        return; //no shard?

                    String date_s = shard.substring(0, shardIndex);
                    String shardValue = shard.substring(shardIndex + 1, shard.length());

                    Date date = oldDateFormat_df.parse(date_s);
                    String newShard = newDateFormat_df.format(date) + newDateDelim + shardValue;
                    
                    Mutation mutation = new Mutation(key.getRow());
                    mutation.put(new Text(newShard), key.getColumnQualifier(),
                            new ColumnVisibility(key.getColumnVisibility()), System.currentTimeMillis(), value);

                    //delete
                    mutation.putDelete(key.getColumnFamily(), key.getColumnQualifier(), System.currentTimeMillis());
                    context.write(partTableTxt, mutation);
                }
            } catch (ParseException pe) {
                //only do work for the rows that match the old date format
                //throw new IOException(pe);
            }
        }
    }
}
