package mvm.rya.cloudbase.mr.fileinput;

import mvm.rya.api.domain.utils.RyaStatementWritable;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.cloudbase.mr.utils.MRUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.openrdf.model.Statement;
import org.openrdf.rio.*;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Be able to input multiple rdf formatted files. Convert from rdf format to statements.
 * Class RdfFileInputFormat
 * Date: May 16, 2011
 * Time: 2:11:24 PM
 */
public class RdfFileInputFormat extends FileInputFormat<LongWritable, RyaStatementWritable> {

    @Override
    public RecordReader<LongWritable, RyaStatementWritable> createRecordReader(InputSplit inputSplit,
                                                                               TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new RdfFileRecordReader();
    }

    private class RdfFileRecordReader extends RecordReader<LongWritable, RyaStatementWritable> implements RDFHandler {

        boolean closed = false;
        long count = 0;
        BlockingQueue<RyaStatementWritable> queue = new LinkedBlockingQueue<RyaStatementWritable>();
        int total = 0;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) inputSplit;
            Configuration conf = taskAttemptContext.getConfiguration();
            String rdfForm_s = conf.get(MRUtils.FORMAT_PROP, RDFFormat.RDFXML.getName()); //default to RDF/XML
            RDFFormat rdfFormat = RDFFormat.valueOf(rdfForm_s);

            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream fileIn = fs.open(fileSplit.getPath());

            RDFParser rdfParser = Rio.createParser(rdfFormat);
            rdfParser.setRDFHandler(this);
            try {
                rdfParser.parse(fileIn, "");
            } catch (Exception e) {
                throw new IOException(e);
            }
            fileIn.close();
            total = queue.size();
            //TODO: Make this threaded so that you don't hold too many statements before sending them
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return queue.size() > 0;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return new LongWritable(count++);
        }

        @Override
        public RyaStatementWritable getCurrentValue() throws IOException, InterruptedException {
            return queue.poll();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return ((float) (total - queue.size())) / ((float) total);
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }

        @Override
        public void startRDF() throws RDFHandlerException {
        }

        @Override
        public void endRDF() throws RDFHandlerException {
        }

        @Override
        public void handleNamespace(String s, String s1) throws RDFHandlerException {
        }

        @Override
        public void handleStatement(Statement statement) throws RDFHandlerException {
            queue.add(new RyaStatementWritable(RdfToRyaConversions.convertStatement(statement)));
        }

        @Override
        public void handleComment(String s) throws RDFHandlerException {
        }
    }

}
