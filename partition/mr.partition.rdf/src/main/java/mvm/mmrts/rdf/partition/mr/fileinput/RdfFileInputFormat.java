package mvm.mmrts.rdf.partition.mr.fileinput;

import mvm.mmrts.rdf.partition.utils.RdfIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
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
public class RdfFileInputFormat extends FileInputFormat<LongWritable, BytesWritable> {

    public static final String RDF_FILE_FORMAT = "mvm.mmrts.rdf.cloudbase.sail.mr.fileinput.rdfformat";

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit inputSplit,
                                                                            TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new RdfFileRecordReader();
    }

    private class RdfFileRecordReader extends RecordReader<LongWritable, BytesWritable> implements RDFHandler {

        boolean closed = false;
        long count = 0;
        BlockingQueue<BytesWritable> queue = new LinkedBlockingQueue<BytesWritable>();
        int total = 0;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) inputSplit;
            Configuration conf = taskAttemptContext.getConfiguration();
            String rdfForm_s = conf.get(RDF_FILE_FORMAT, RDFFormat.RDFXML.getName());
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
        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
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
            try {
                byte[] stmt_bytes = RdfIO.writeStatement(statement, true);
                queue.add(new BytesWritable(stmt_bytes));
            } catch (IOException e) {
                throw new RDFHandlerException(e);
            }
        }

        @Override
        public void handleComment(String s) throws RDFHandlerException {
        }
    }
//
//    public static RDFParser createRdfParser(RDFFormat rdfFormat) {
//        if (RDFFormat.RDFXML.equals(rdfFormat)) {
//            return new RDFXMLParserFactory().getParser();
//        } else if (RDFFormat.N3.equals(rdfFormat)) {
//            return new N3ParserFactory().getParser();
//        } else if (RDFFormat.NTRIPLES.equals(rdfFormat)) {
//            return new NTriplesParserFactory().getParser();
//        } else if (RDFFormat.TRIG.equals(rdfFormat)) {
//            return new TriGParserFactory().getParser();
//        } else if (RDFFormat.TRIX.equals(rdfFormat)) {
//            return new TriXParserFactory().getParser();
//        } else if (RDFFormat.TURTLE.equals(rdfFormat)) {
//            return new TurtleParserFactory().getParser();
//        }
//        throw new IllegalArgumentException("Unknown RDFFormat[" + rdfFormat + "]");
//    }
//
//    public static RDFWriter createRdfWriter(RDFFormat rdfFormat, OutputStream os) {
//        if (RDFFormat.RDFXML.equals(rdfFormat)) {
//            return new RDFXMLWriterFactory().getWriter(os);
//        } else if (RDFFormat.N3.equals(rdfFormat)) {
//            return new N3WriterFactory().getWriter(os);
//        } else if (RDFFormat.NTRIPLES.equals(rdfFormat)) {
//            return new NTriplesWriterFactory().getWriter(os);
//        } else if (RDFFormat.TRIG.equals(rdfFormat)) {
//            return new TriGWriterFactory().getWriter(os);
//        } else if (RDFFormat.TRIX.equals(rdfFormat)) {
//            return new TriXWriterFactory().getWriter(os);
//        } else if (RDFFormat.TURTLE.equals(rdfFormat)) {
//            return new TurtleWriterFactory().getWriter(os);
//        }
//        throw new IllegalArgumentException("Unknown RDFFormat[" + rdfFormat + "]");
//    }

}
