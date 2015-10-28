package mvm.mmrts.rdf.partition.mr.fileinput;

import com.google.common.io.ByteStreams;
import mvm.mmrts.rdf.partition.utils.RdfIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.io.IOException;
import java.util.Date;

import static mvm.mmrts.rdf.partition.PartitionConstants.FAMILY_DELIM_STR;
import static mvm.mmrts.rdf.partition.utils.RdfIO.writeValue;

/**
 * Do bulk import of rdf files
 * Class RdfFileInputToCloudbaseTool
 * Date: May 16, 2011
 * Time: 3:12:16 PM
 */
public class RdfFileInputToFileTool implements Tool {

    private Configuration conf;

    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new RdfFileInputToFileTool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long runJob(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 2)
            throw new IllegalArgumentException("Usage: RdfFileInputToFileTool <input directory> <output directory>");

        //faster
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("io.sort.mb", "256");

        Job job = new Job(conf);
        job.setJarByClass(RdfFileInputToFileTool.class);

        // set up cloudbase input
        job.setInputFormatClass(RdfFileInputFormat.class);
        RdfFileInputFormat.addInputPath(job, new Path(args[0]));

        // set input output of the particular job
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);


        // set mapper and reducer classes
        job.setMapperClass(StmtToBytesMapper.class);
        job.setReducerClass(StmtBytesReducer.class);

        // set output
        job.setOutputFormatClass(TextOutputFormat.class);
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

    public static class StmtToBytesMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {

        Text outKey = new Text();
        Text outValue = new Text();

        public StmtToBytesMapper() {
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            Statement statement = RdfIO.readStatement(ByteStreams.newDataInput(value.getBytes()), ValueFactoryImpl.getInstance());
            outKey.set(new String(writeValue(statement.getSubject())) + FAMILY_DELIM_STR);
            outValue.set(value.getBytes());
            context.write(outKey, outValue);
        }

    }

    public static class StmtBytesReducer extends Reducer<Text, Text, NullWritable, Text> {

        NullWritable outKey = NullWritable.get();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text stmt_txt : values) {
                context.write(outKey, stmt_txt);
            }
        }
    }
}

