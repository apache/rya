package mvm.mmrts.rdf.partition.mr;

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;
import mvm.mmrts.rdf.partition.PartitionConstants;
import mvm.mmrts.rdf.partition.utils.RdfIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openrdf.model.Statement;

import java.io.IOException;
import java.util.Date;

/**
 * Class SparqlTestDriver
 * Date: Oct 28, 2010
 * Time: 2:53:39 PM
 */
public class TestDriver implements Tool {

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new TestDriver(), args);
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

        Job job = new Job(conf);
        job.setJarByClass(TestDriver.class);

        FileInputFormat.addInputPaths(job, "/temp/rpunnoose/results.txt");
        job.setInputFormatClass(TextInputFormat.class);

        // set input output of the particular job
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        // set mapper and reducer classes
        job.setMapperClass(SubjectMapWrMapper.class);
        job.setReducerClass(OutMapWrReducer.class);
        job.setNumReduceTasks(1);
//        job.setNumReduceTasks(0);

        // set output
        Path outputDir = new Path("/temp/rpunnoose/partBS");
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

    public static class SubjectMapWrMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        Text outKey = new Text();
        final String ID = "id";
        final Text ID_TXT = new Text(ID);
        final String PERF_AT = "performedBy";
        final Text PERF_AT_TXT = new Text("system");
        final String REPORT_AT = "reportedAt";
        final Text REPORT_AT_TXT = new Text("timestamp");
        final String TYPE = "type";
        final Text TYPE_TXT = new Text(TYPE);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            int i = s.lastIndexOf("\0");
            Statement stmt = RdfIO.readStatement(ByteStreams.newDataInput(s.substring(0, i).getBytes()), PartitionConstants.VALUE_FACTORY);
            String predStr = stmt.getPredicate().stringValue();
            if (!predStr.contains(PERF_AT) && !predStr.contains(REPORT_AT) && !predStr.contains(TYPE))
                return;

            outKey.set(stmt.getSubject().stringValue());
            MapWritable mw = new MapWritable();
            mw.put(ID_TXT, outKey);
            if (predStr.contains(PERF_AT))
                mw.put(PERF_AT_TXT, new Text(stmt.getObject().stringValue()));
            else if (predStr.contains(REPORT_AT))
                mw.put(REPORT_AT_TXT, new Text(stmt.getObject().stringValue()));
            else if (predStr.contains(TYPE))
                mw.put(TYPE_TXT, new Text(stmt.getObject().stringValue()));

            context.write(outKey, mw);
        }
    }

    public static class OutMapWrReducer extends Reducer<Text, MapWritable, Text, Text> {
        final Text PART = new Text("partitionBS");
        Text outKey = new Text();

        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            MapWritable mw = new MapWritable();
            for (MapWritable value : values) {
                mw.putAll(value);
            }
            outKey.set(mw.values().toString());
            context.write(outKey, PART);
        }
    }
}
