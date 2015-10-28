package mvm.rya.cloudbase.giraph.format;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexOutputFormat;
import org.apache.giraph.graph.VertexWriter;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Date: 7/27/12
 * Time: 2:58 PM
 */
public class PrintVertexOutputFormat<
        I extends WritableComparable,
        V extends Writable,
        E extends Writable>
        extends VertexOutputFormat<I, V, E> implements Configurable {
    @Override
    public void setConf(Configuration entries) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Configuration getConf() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public VertexWriter<I, V, E> createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new VertexWriter<I, V, E>() {
            @Override
            public void initialize(TaskAttemptContext context) throws IOException {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void writeVertex(Vertex<I, V, E, ?> iveVertex) throws IOException, InterruptedException {
                System.out.println(iveVertex);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new OutputCommitter() {
            @Override
            public void setupJob(JobContext jobContext) throws IOException {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void cleanupJob(JobContext jobContext) throws IOException {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
                //To change body of implemented methods use File | Settings | File Templates.
            }
        };
    }
}
