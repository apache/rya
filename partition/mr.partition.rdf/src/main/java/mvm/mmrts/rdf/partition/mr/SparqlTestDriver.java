package mvm.mmrts.rdf.partition.mr;

import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;

/**
 * Class SparqlTestDriver
 * Date: Oct 28, 2010
 * Time: 2:53:39 PM
 */
public class SparqlTestDriver implements Tool {

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new SparqlTestDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Configuration conf;

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public int run(String[] args) throws IOException, InterruptedException,
            ClassNotFoundException {

        //query from file
        if(args.length < 2) {
            throw new IllegalArgumentException("Usage: hadoop jar mvm.mmrts.rdf.partition.mr.SparqlTestDriver <local query file> outputFile");
        }

        FileInputStream fis = new FileInputStream(args[0]);
        String query = new String(ByteStreams.toByteArray(fis));
        fis.close();

        Job job = new Job(conf);
        job.setJarByClass(SparqlTestDriver.class);

        // set up cloudbase input
        job.setInputFormatClass(SparqlPartitionStoreInputFormat.class);
        SparqlPartitionStoreInputFormat.setInputInfo(job, "root", "password".getBytes());
        SparqlPartitionStoreInputFormat.setZooKeeperInstance(job, "stratus", "10.40.190.113:2181");
        SparqlPartitionStoreInputFormat.setLongJob(job, null);
        SparqlPartitionStoreInputFormat.setTable(job, "partitionRdf");

        long startTime_l = 1303811164088l;
        long ttl = 86400000;

        //set query
//        String query = "PREFIX tdp: <http://here/2010/tracked-data-provenance/ns#>\n" +
//                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
//                "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
//                "SELECT * WHERE\n" +
//                "{\n" +
//                "?id tdp:reportedAt ?timestamp. \n" +
//                "FILTER(mvmpart:timeRange(?id, tdp:reportedAt, 1314380456900 , 1314384056900 , 'XMLDATETIME')).\n" +
//                "?id tdp:performedBy ?system.\n" +
//                "} \n";
//
//        String query2 = "PREFIX hb: <http://here/2010/tracked-data-provenance/heartbeat/ns#>\n" +
//                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
//                "PREFIX mvmpart: <urn:mvm.mmrts.partition.rdf/08/2011#>\n" +
//                "SELECT * WHERE\n" +
//                "{\n" +
//                "?id hb:timeStamp ?timestamp. \n" +
//                "FILTER(mvmpart:timeRange(?id, hb:timeStamp, 1314360009522 , 1314367209522 , 'TIMESTAMP')).\n" +
//                "?id hb:count ?count.\n" +
//                "?id hb:systemName ?systemName.\n" +
//                "} ";

        System.out.println(query);
        System.out.println();
//        System.out.println(query2);

        SparqlPartitionStoreInputFormat.setSparqlQueries(job, query);
//        SparqlCloudbaseStoreInputFormat.setStartTime(job, 1309956861000l + "");
//        SparqlCloudbaseStoreInputFormat.setTtl(job, 86400000 + "");

        // set input output of the particular job
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job.setOutputFormatClass(FileOutputFormat.class);


        // set mapper and reducer classes
        job.setMapperClass(MyTempMapper.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(1);

        // set output
        Path outputDir = new Path(args[1]);
        FileSystem dfs = FileSystem.get(outputDir.toUri(), conf);
        if (dfs.exists(outputDir))
            dfs.delete(outputDir, true);

        FileOutputFormat.setOutputPath(job, outputDir);

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
            return (int) job
                    .getCounters()
                    .findCounter("org.apache.hadoop.mapred.Task$Counter",
                            "REDUCE_OUTPUT_RECORDS").getValue();
        } else {
            System.out.println("Job Failed!!!");
        }

        return -1;
    }

    public static class MyTempMapper extends Mapper<LongWritable, MapWritable, Text, Text> {
        Text outKey = new Text();
        Text outValue = new Text("partition");
        @Override
        protected void map(LongWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
            outKey.set(value.values().toString());
            context.write(outKey, outValue);
        }
    }
}
