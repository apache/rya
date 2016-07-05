package mvm.rya.accumulo.mr.fileinput;

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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.mr.RyaStatementWritable;
import mvm.rya.accumulo.mr.utils.MRUtils;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaTripleContext;

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
		private RyaTripleContext tripleContext;
        

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) inputSplit;
            Configuration conf = taskAttemptContext.getConfiguration();
            String rdfForm_s = conf.get(MRUtils.FORMAT_PROP, RDFFormat.RDFXML.getName());
            RDFFormat rdfFormat = RDFFormat.valueOf(rdfForm_s);
            tripleContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(conf));

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
            queue.add(new RyaStatementWritable(RdfToRyaConversions.convertStatement(statement), tripleContext));
        }

        @Override
        public void handleComment(String s) throws RDFHandlerException {
        }
    }

}
