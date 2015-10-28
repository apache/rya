package mvm.mmrts.rdf.partition.mr.fileinput;

import cloudbase.core.client.mapreduce.CloudbaseOutputFormat;
import cloudbase.core.data.Mutation;
import com.google.common.io.ByteStreams;
import mvm.mmrts.rdf.partition.shard.DateHashModShardValueGenerator;
import mvm.mmrts.rdf.partition.utils.RdfIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.io.IOException;
import java.util.Date;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;
import static mvm.mmrts.rdf.partition.PartitionConstants.EMPTY_VALUE;
import static mvm.mmrts.rdf.partition.utils.RdfIO.writeStatement;
import static mvm.mmrts.rdf.partition.utils.RdfIO.writeValue;

/**
 * Do bulk import of rdf files
 * Class RdfFileInputToCloudbaseTool
 * Date: May 16, 2011
 * Time: 3:12:16 PM
 */
public class RdfFileInputToCloudbaseTool implements Tool {

    public static final String CB_USERNAME_PROP = "cb.username";
    public static final String CB_PWD_PROP = "cb.pwd";
    public static final String CB_SERVER_PROP = "cb.server";
    public static final String CB_PORT_PROP = "cb.port";
    public static final String CB_INSTANCE_PROP = "cb.instance";
    public static final String CB_TTL_PROP = "cb.ttl";
    public static final String CB_TABLE_PROP = "cb.table";


    private Configuration conf;

    private String userName = "root";
    private String pwd = "password";
    private String instance = "stratus";
    private String server = "10.40.190.113";
    private String port = "2181";
    private String table = "partitionRdf";


    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new RdfFileInputToCloudbaseTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long runJob(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //faster
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("io.sort.mb", "256");

        server = conf.get(CB_SERVER_PROP, server);
        port = conf.get(CB_PORT_PROP, port);
        instance = conf.get(CB_INSTANCE_PROP, instance);
        userName = conf.get(CB_USERNAME_PROP, userName);
        pwd = conf.get(CB_PWD_PROP, pwd);
        table = conf.get(CB_TABLE_PROP, table);
        conf.set(CB_TABLE_PROP, table);

        Job job = new Job(conf);
        job.setJarByClass(RdfFileInputToCloudbaseTool.class);

        // set up cloudbase input
        job.setInputFormatClass(RdfFileInputFormat.class);
        RdfFileInputFormat.addInputPath(job, new Path(args[0]));

        // set input output of the particular job
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);

        job.setOutputFormatClass(CloudbaseOutputFormat.class);
        CloudbaseOutputFormat.setOutputInfo(job, userName, pwd.getBytes(), true, table);
        CloudbaseOutputFormat.setZooKeeperInstance(job, instance, server + ":" + port);

        // set mapper and reducer classes
        job.setMapperClass(OutSubjStmtMapper.class);
        job.setReducerClass(StatementToMutationReducer.class);

        // set output
//        Path outputDir = new Path("/temp/sparql-out/testout");
//        FileSystem dfs = FileSystem.get(outputDir.toUri(), conf);
//        if (dfs.exists(outputDir))
//            dfs.delete(outputDir, true);
//
//        FileOutputFormat.setOutputPath(job, outputDir);

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

    @Override
    public int run(String[] args) throws Exception {
        runJob(args);
        return 0;
    }

    public static class OutSubjStmtMapper extends Mapper<LongWritable, BytesWritable, Text, BytesWritable> {

        public OutSubjStmtMapper() {
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            Statement statement = RdfIO.readStatement(ByteStreams.newDataInput(value.getBytes()), ValueFactoryImpl.getInstance());
            context.write(new Text(new String(writeValue(statement.getSubject())) + FAMILY_DELIM_STR), value);
        }

    }

    public static class StatementToMutationReducer extends Reducer<Text, BytesWritable, Text, Mutation> {
        private Text outputTable;
        private DateHashModShardValueGenerator gen;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            outputTable = new Text(context.getConfiguration().get(CB_TABLE_PROP, null));
            gen = new DateHashModShardValueGenerator();
        }

        @Override
        protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            Resource subject = (Resource) RdfIO.readValue(ByteStreams.newDataInput(key.getBytes()), ValueFactoryImpl.getInstance(), FAMILY_DELIM);
            byte[] subj_bytes = writeValue(subject);
            String shard = gen.generateShardValue(subject);
            Text shard_txt = new Text(shard);

            /**
             * Triple - >
             *- < subject ><shard >:
             *- < shard > event:<subject >\0 < predicate >\0 < object >\0
             *- < shard > index:<predicate >\1 < object >\0
             */
            Mutation m_subj = new Mutation(shard_txt);
            for (BytesWritable stmt_bytes : values) {
                Statement stmt = RdfIO.readStatement(ByteStreams.newDataInput(stmt_bytes.getBytes()), ValueFactoryImpl.getInstance());
                m_subj.put(DOC, new Text(writeStatement(stmt, true)), EMPTY_VALUE);
                m_subj.put(INDEX, new Text(writeStatement(stmt, false)), EMPTY_VALUE);
            }

            /**
             * TODO: Is this right?
             * If the subject does not have any authorizations specified, then anyone can access it.
             * But the true authorization check will happen at the predicate/object level, which means that
             * the set returned will only be what the person is authorized to see.  The shard lookup table has to
             * have the lowest level authorization all the predicate/object authorizations; otherwise,
             * a user may not be able to see the correct document.
             */
            Mutation m_shard = new Mutation(new Text(subj_bytes));
            m_shard.put(shard_txt, EMPTY_TXT, EMPTY_VALUE);

            context.write(outputTable, m_subj);
            context.write(outputTable, m_shard);
        }
    }
}

