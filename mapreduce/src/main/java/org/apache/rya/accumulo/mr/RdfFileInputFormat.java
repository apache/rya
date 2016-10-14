package org.apache.rya.accumulo.mr;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaTripleContext;

/**
 * {@link FileInputFormat} that can read multiple RDF files and convert into
 * statements.
 * <p>
 * Expects all files to use the same RDF serialization format, which must be
 * provided.
 * <p>
 * Reading and parsing is done asynchronously, so entire files need not be
 * loaded into memory at once. Reading will block when a character buffer is
 * full, waiting for the parser to consume more data. The parser will block when
 * a statement buffer is full, waiting for the client to consume the statements
 * generated so far. This enables large files, particularly useful for N-Triples
 * and N-Quads formats, which can be parsed one line at a time. The size of each
 * buffer can be configured. An error will be thrown if the parser takes too
 * long to respond, and this timeout can be configured.
 * <p>
 * Only N-Triples and N-Quads files may be split into multiple
 * {@link InputSplit}s per file, if large enough. Input is read line-by-line,
 * and each line of an N-Triples or N-Quads file is self-contained, so any
 * arbitrary split is valid. This means the number of input splits may be
 * greater than the number of input files if and only if N-Triples or N-Quads is
 * given as the RDF serialization format.
 */
public class RdfFileInputFormat extends FileInputFormat<LongWritable, RyaStatementWritable> {
    private static final Logger logger = Logger.getLogger(RdfFileInputFormat.class);
    private static final String PREFIX = RdfFileInputFormat.class.getSimpleName();
    private static final String CHAR_BUFFER_SIZE_PROP = PREFIX + ".char.buffer.size";
    private static final String STATEMENT_BUFFER_SIZE_PROP = PREFIX + ".statement.buffer.size";
    private static final String TIMEOUT_PROP = PREFIX + ".timeout";
    private static final String FORMAT_PROP = PREFIX + ".rdf.format";

    private static final RDFFormat DEFAULT_RDF_FORMAT = RDFFormat.RDFXML;
    private static final int DEFAULT_CHAR_BUFFER_SIZE = 1024*1024;
    private static final int DEFAULT_STATEMENT_BUFFER_SIZE = 1024;
    private static final int DEFAULT_TIMEOUT = 20;

    static final RyaStatementWritable DONE = new RyaStatementWritable(null, null); // signals the end of input
    static final RyaStatementWritable ERROR = new RyaStatementWritable(null, null); // signals some error

    /**
     * Set the RDF serialization format to parse. All input files must have the
     * same format.
     * @param   job     Job to apply the setting to
     * @param   format  Format of any and all input files
     */
    public static void setRDFFormat(Job job, RDFFormat format) {
        job.getConfiguration().set(FORMAT_PROP, format.getName());
    }

    /**
     * Specify the size, in characters, of the input buffer: hold this many
     * characters in memory before blocking file input.
     */
    public static void setCharBufferSize(Job job, int size) {
        job.getConfiguration().setInt(CHAR_BUFFER_SIZE_PROP, size);
    }

    /**
     * Specify the size, in statements, of the parser output buffer: hold this
     * many Statements in memory before blocking the parser.
     */
    public static void setStatementBufferSize(Job job, int size) {
        job.getConfiguration().setInt(STATEMENT_BUFFER_SIZE_PROP, size);
    }

    /**
     * Property to specify the timeout, in seconds:
     */
    public static void setTimeout(Job job, int seconds) {
        job.getConfiguration().setInt(TIMEOUT_PROP, seconds);
    }

    private RDFFormat getRDFFormat(JobContext context) {
        String name = context.getConfiguration().get(FORMAT_PROP);
        return RDFFormat.valueOf(name);
    }

    /**
     * Determine whether an input file can be split. If the input format is
     * configured to be anything other than N-Triples or N-Quads, then the
     * structure of the file is important and it cannot be split arbitrarily.
     * Otherwise, default to the superclass logic to determine whether splitting
     * is appropriate.
     * @return  true if configured to use a line-based input format and the
     *          superclass implementation returns true.
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        RDFFormat rdfFormat = getRDFFormat(context);
        if (RDFFormat.NTRIPLES.equals(rdfFormat) || RDFFormat.NQUADS.equals(rdfFormat)) {
            return super.isSplitable(context, filename);
        }
        return false;
    }

    /**
     * Instantiate a RecordReader for a given task attempt.
     * @param   inputSplit  Input split to handle, may refer to part or all of
     *                      an RDF file
     * @param   taskAttemptContext  Contains configuration options.
     * @return  A RecordReader that reads and parses RDF text.
     */
    @Override
    public RecordReader<LongWritable, RyaStatementWritable> createRecordReader(InputSplit inputSplit,
            TaskAttemptContext taskAttemptContext) {
        Configuration conf = taskAttemptContext.getConfiguration();
        RDFFormat format = getRDFFormat(taskAttemptContext);
        if (format == null) {
            format = DEFAULT_RDF_FORMAT;
        }
        int charBufferSize = conf.getInt(CHAR_BUFFER_SIZE_PROP, DEFAULT_CHAR_BUFFER_SIZE);
        int statementBufferSize = conf.getInt(STATEMENT_BUFFER_SIZE_PROP, DEFAULT_STATEMENT_BUFFER_SIZE);
        int timeoutSeconds = conf.getInt(TIMEOUT_PROP, DEFAULT_TIMEOUT);
        return new RdfFileRecordReader(format, charBufferSize, statementBufferSize, timeoutSeconds);
    }

    /**
     * Reads RDF files and generates RyaStatementWritables. Reads and parses
     * data in parallel, so the entire file need not be loaded at once.
     */
    class RdfFileRecordReader extends RecordReader<LongWritable, RyaStatementWritable> implements RDFHandler {
        private RecordReader<Text, Text> lineReader;
        private final PipedWriter pipeOut;
        private final PipedReader pipeIn;
        private final RDFParser rdfParser;
        final BlockingQueue<RyaStatementWritable> statementCache;

        private long lineCount = 0;
        private long statementCount = 0;
        private RyaTripleContext tripleContext;
        private RyaStatementWritable nextStatement = null;
        private int timeoutSeconds;
        private boolean noMoreStatements = false;

        Thread readerThread;
        Thread parserThread;
        private Exception threadException;

        /**
         * Instantiates the RecordReader.
         * @param format    RDF serialization format to parse.
         * @param charBufferSize    Number of input characters to hold in
         *                          memory; if exceeded, wait until the parser
         *                          thread consumes some text before proceeding
         *                          with reading input.
         * @param statementBufferSize   Number of output statements to hold in
         *                              memory; if exceeded, wait until the
         *                              client consumes data before proceeding
         *                              with parsing.
         * @param timeoutSeconds    Number of seconds to wait for the parser
         *                          thread to provide the next statement (or
         *                          state that there are none). If exceeded,
         *                          abort.
         */
        RdfFileRecordReader(RDFFormat format, int charBufferSize, int statementBufferSize, int timeoutSeconds) {
            rdfParser = Rio.createParser(format);
            rdfParser.setRDFHandler(this);
            statementCache = new LinkedBlockingQueue<RyaStatementWritable>(statementBufferSize);
            pipeOut = new PipedWriter();
            pipeIn = new PipedReader(charBufferSize);
            this.timeoutSeconds = timeoutSeconds;
            logger.info("Initializing RecordReader with parameters:");
            logger.info("\tRDF serialization format = " + format.getName());
            logger.info("\tinput buffer size = " + charBufferSize + " characters");
            logger.info("\tstatement cache size = " + statementBufferSize);
            logger.info("\tparser timeout = " + timeoutSeconds + " seconds");
        }

        /**
         * Starts up one thread for reading text data (via an internal line
         * based RecordReader) and one thread for receiving and parsing that
         * data, each blocking when their respective buffers are full.
         * @param   inputSplit          The section of data to read
         * @param   taskAttemptContext  Contains configuration variables
         * @throws  IOException if an error is encountered initializing the line
         *          RecordReader or piping its output to the parser thread.
         * @throws  InterruptedException if an error is encountered initializing
         *          the line RecordReader
         */
        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            Configuration conf = taskAttemptContext.getConfiguration();
            lineReader = new KeyValueLineRecordReader(conf);
            lineReader.initialize(inputSplit, taskAttemptContext);
            tripleContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(conf));
            pipeIn.connect(pipeOut);

            readerThread = new Thread(Thread.currentThread().getName() + " -- reader thread") {
                @Override
                public void run() {
                    try {
                        logger.info("Starting file reader");
                        while (lineReader.nextKeyValue()) {
                            Text key = lineReader.getCurrentKey();
                            Text value = lineReader.getCurrentValue();
                            pipeOut.write(key.toString());
                            if (value.getLength() > 0) {
                                pipeOut.write(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR);
                                pipeOut.write(value.toString());
                            }
                            pipeOut.write('\n');
                            lineCount++;
                        }
                        logger.info("Reached end of input text; read " + lineCount + " lines in total");
                    } catch (IOException | InterruptedException e) {
                        logger.error("Error processing line " + (lineCount+1) + " of input", e);
                        fail(e, this);
                        throw new RuntimeException(e.getMessage(), e);
                    }
                    finally {
                        try { lineReader.close(); } catch (IOException e) { logger.warn(e); }
                        try { pipeOut.close(); } catch (IOException e) { logger.warn(e); }
                    }
                }
            };

            parserThread = new Thread(Thread.currentThread().getName() + " -- parser thread") {
                @Override
                public void run() {
                    try {
                        logger.info("Starting parser");
                        rdfParser.parse(pipeIn, "");
                    }
                    catch (RDFHandlerException | RDFParseException | IOException e) {
                        logger.error(e.getMessage(), e);
                        fail(e, this);
                        throw new RuntimeException(e.getMessage(), e);
                    }
                    finally {
                        try { pipeIn.close(); } catch (IOException e) { logger.warn(e); }
                    }
                }
            };
            readerThread.start();
            parserThread.start();
        }

        private void fail(Exception e, Thread source) {
            // Notify the main RecordReader of the error
            statementCache.offer(ERROR);
            threadException = e;
            // Kill the reader thread if necessary
            if (source != readerThread && readerThread.isAlive()) {
                readerThread.interrupt();
            }
            // Kill the parser thread if necessary
            if (source != parserThread && parserThread.isAlive()) {
                parserThread.interrupt();
            }
        }

        /**
         * Loads the next statement, if there is one, and returns whether there
         * is one. Receives statements from the parser thread via a blocking
         * queue.
         * @throws  InterruptedException if interrupted while waiting for a
         *          statement to show up in the queue.
         * @throws  IOException if the parser thread doesn't respond after the
         *          configured timeout, or if any thread reports an error.
         * @return  true if a valid statement was loaded, or false if there are
         *          no more statements in this input split.
         */
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (noMoreStatements) {
                return false;
            }
            nextStatement = statementCache.poll(timeoutSeconds, TimeUnit.SECONDS);
            if (nextStatement == null) {
                    throw new IOException("Parser neither sending results nor signaling end of data after "
                        + timeoutSeconds + " seconds.");
            }
            else if (nextStatement == DONE) {
                logger.info("Reached end of parsed RDF; read " +  statementCount + " statements in total.");
                nextStatement = null;
                noMoreStatements = true;
                return false;
            }
            else if (nextStatement == ERROR) {
                nextStatement = null;
                noMoreStatements = true;
                throw new IOException("Error detected processing input.", threadException);
            }
            statementCount++;
            return true;
        }

        /**
         * Gets the current key.
         * @return  the number of statements read so far, or null if all input
         *          has been read.
         */
        @Override
        public LongWritable getCurrentKey() {
            if (noMoreStatements) {
                return null;
            }
            return new LongWritable(statementCount);
        }

        /**
         * Gets the current value.
         * @return  a RyaStatementWritable loaded from RDF data, or null if all
         *          input has been read.
         */
        @Override
        public RyaStatementWritable getCurrentValue() {
            return nextStatement;
        }

        /**
         * Gets the progress of the underlying line-based Record Reader. Does
         * not include any information about the progress of the parser.
         * @return  The proportion of text input that has been read.
         * @throws  IOException if thrown by the internal RecordReader.
         * @throws  InterruptedException if thrown by the internal RecordReader.
         */
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineReader.getProgress();
        }

        /**
         * Closes all the underlying resources.
         */
        @Override
        public void close() {
            if (parserThread.isAlive()) {
                parserThread.interrupt();
            }
            if (readerThread.isAlive()) {
                readerThread.interrupt();
            }
            try { lineReader.close(); } catch (IOException e) { logger.warn(e); }
            try { pipeOut.close(); } catch (IOException e) { logger.warn(e); }
            try { pipeIn.close(); } catch (IOException e) { logger.warn(e); }
        }

        /**
         * Has no effect.
         */
        @Override
        public void startRDF() throws RDFHandlerException {
        }

        /**
         * Add a dummy item to the queue to signal that there will be no more
         * statements.
         * @throws  RDFHandlerException     if interrupted while waiting for
         *          the blocking queue to be ready to accept the done signal.
         */
        @Override
        public void endRDF() throws RDFHandlerException {
            logger.info("Finished parsing RDF");
            try {
                statementCache.put(DONE);
            } catch (InterruptedException e) {
                throw new RDFHandlerException("Interrupted while waiting to add done signal to statement queue", e);
            }
        }

        /**
         * Has no effect.
         */
        @Override
        public void handleNamespace(String s, String s1) throws RDFHandlerException {
        }

        /**
         * Convert the {@link Statement} to a {@link RyaStatement}, wrap it in a
         * {@link RyaStatementWritable}, and add it to the queue.
         * @throws  RDFHandlerException     if interrupted while waiting for the
         *          blocking queue to be ready to accept statement data.
         */
        @Override
        public void handleStatement(Statement statement) throws RDFHandlerException {
            try {
                statementCache.put(new RyaStatementWritable(RdfToRyaConversions.convertStatement(statement), tripleContext));
            } catch (InterruptedException e) {
                throw new RDFHandlerException("Interrupted while waiting to add parsed statement to the statement queue", e);
            }
        }

        /**
         * Has no effect.
         */
        @Override
        public void handleComment(String s) {
        }
    }
}
