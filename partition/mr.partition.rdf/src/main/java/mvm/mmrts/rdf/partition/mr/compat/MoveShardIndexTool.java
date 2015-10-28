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
import java.util.Collections;
import java.util.Date;

/**
 * MMRTS-148 Need to move the shard index from the partition table to the shardIndex table
 * Class MoveShardIndexTool
 * Date: Dec 8, 2011
 * Time: 4:11:40 PM
 */
public class MoveShardIndexTool implements Tool {
    public static final String CB_USERNAME_PROP = "cb.username";
    public static final String CB_PWD_PROP = "cb.pwd";
    public static final String CB_ZK_PROP = "cb.zk";
    public static final String CB_INSTANCE_PROP = "cb.instance";
    public static final String PARTITION_TABLE_PROP = "partition.table";
    public static final String SHARD_INDEX_TABLE_PROP = "shard.index.table";
    public static final String SHARD_INDEX_DELETE_PROP = "shard.index.delete";


    private Configuration conf;

    private String userName = "root";
    private String pwd = "password";
    private String instance = "stratus";
    private String zk = "10.40.190.113:2181";
    private String partitionTable = "rdfPartition";
    private String shardIndexTable = "rdfShardIndex";

    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new MoveShardIndexTool(), args);
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
        shardIndexTable = conf.get(SHARD_INDEX_TABLE_PROP, shardIndexTable);
        conf.set(SHARD_INDEX_TABLE_PROP, shardIndexTable);
        conf.set(PARTITION_TABLE_PROP, partitionTable);

        Job job = new Job(conf);
        job.setJarByClass(MoveShardIndexTool.class);

        job.setInputFormatClass(CloudbaseInputFormat.class);
        //TODO: How should I send in Auths?
        CloudbaseInputFormat.setInputInfo(job, userName, pwd.getBytes(),
                partitionTable, CBConstants.NO_AUTHS);
        CloudbaseInputFormat.setZooKeeperInstance(job, instance, zk);
        CloudbaseInputFormat.setRanges(job, Collections.singleton(
                new Range(
                        new Text(PartitionConstants.URI_MARKER_STR),
                        new Text(PartitionConstants.PLAIN_LITERAL_MARKER_STR))));

        job.setMapperClass(ShardKeyValueToMutationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Mutation.class);

        job.setOutputFormatClass(CloudbaseOutputFormat.class);
        CloudbaseOutputFormat.setOutputInfo(job, userName, pwd.getBytes(), true, shardIndexTable);
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

    public static class ShardKeyValueToMutationMapper extends Mapper<Key, Value, Text, Mutation> {
        private Text shardTableTxt;
        private Text partTableTxt;
        protected boolean deletePrevShardIndex;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String shardTable = context.getConfiguration().get(SHARD_INDEX_TABLE_PROP);
            if (shardTable == null)
                throw new IllegalArgumentException("Shard Table property cannot be null");

            shardTableTxt = new Text(shardTable);

            String partTable = context.getConfiguration().get(PARTITION_TABLE_PROP);
            if (partTable == null)
                throw new IllegalArgumentException("Partition Table property cannot be null");

            partTableTxt = new Text(partTable);

            deletePrevShardIndex = context.getConfiguration().getBoolean(SHARD_INDEX_DELETE_PROP, false);
            System.out.println("Deleting shard index from previous: " + deletePrevShardIndex + " Part: " + partTableTxt);
        }

        @Override
        protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
            Mutation mutation = new Mutation(key.getRow());
            mutation.put(key.getColumnFamily(), key.getColumnQualifier(),
                    new ColumnVisibility(key.getColumnVisibility()), System.currentTimeMillis(), value);

            context.write(shardTableTxt, mutation);

            if (deletePrevShardIndex) {
                mutation = new Mutation(key.getRow());
                mutation.putDelete(key.getColumnFamily(), key.getColumnQualifier(), System.currentTimeMillis());

                context.write(partTableTxt, mutation);
            }
        }
    }
}
