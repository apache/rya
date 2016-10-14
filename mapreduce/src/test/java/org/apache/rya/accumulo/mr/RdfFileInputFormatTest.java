package org.apache.rya.accumulo.mr;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFFormat;

import org.apache.rya.api.resolver.RyaToRdfConversions;

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

public class RdfFileInputFormatTest {
    static String NT_INPUT = "src/test/resources/test.ntriples";
    static String TRIG_INPUT = "src/test/resources/namedgraphs.trig";

    Configuration conf;
    Job job;
    FileSystem fs;
    RdfFileInputFormat.RdfFileRecordReader reader;

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Before
    public void before() throws IOException {
        conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        fs = FileSystem.get(conf);
        job = Job.getInstance(conf);
    }

    void init(String filename) throws IOException, InterruptedException {
        conf = job.getConfiguration();
        File inputFile = new File(filename);
        Path inputPath = new Path(inputFile.getAbsoluteFile().toURI());
        InputSplit split = new FileSplit(inputPath, 0, inputFile.length(), null);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        reader = (RdfFileInputFormat.RdfFileRecordReader) new RdfFileInputFormat().createRecordReader(split, context);
        reader.initialize(split, context);
    }

    @Test
    public void testStatementInput() throws Exception {
        RdfFileInputFormat.setRDFFormat(job, RDFFormat.NTRIPLES);
        init(NT_INPUT);
        String prefix = "urn:lubm:rdfts#";
        URI[] gs = {
                new URIImpl(prefix + "GraduateStudent01"),
                new URIImpl(prefix + "GraduateStudent02"),
                new URIImpl(prefix + "GraduateStudent03"),
                new URIImpl(prefix + "GraduateStudent04")
        };
        URI hasFriend = new URIImpl(prefix + "hasFriend");
        Statement[] statements = {
                new StatementImpl(gs[0], hasFriend, gs[1]),
                new StatementImpl(gs[1], hasFriend, gs[2]),
                new StatementImpl(gs[2], hasFriend, gs[3])
        };
        int count = 0;
        while (reader.nextKeyValue()) {
            Assert.assertEquals(statements[count],
                    RyaToRdfConversions.convertStatement(reader.getCurrentValue().getRyaStatement()));
            count++;
            Assert.assertEquals(count, reader.getCurrentKey().get());
        }
        Assert.assertEquals(3, count);
    }

    @Test
    public void testTrigInput() throws Exception {
        RdfFileInputFormat.setRDFFormat(job, RDFFormat.TRIG);
        init(TRIG_INPUT);
        Assert.assertTrue(reader.nextKeyValue());
        Assert.assertEquals(1, reader.getCurrentKey().get());
        Statement expected = new ContextStatementImpl(
            new URIImpl("http://www.example.org/exampleDocument#Monica"),
            new URIImpl("http://www.example.org/vocabulary#name"),
            new LiteralImpl("Monica Murphy"),
            new URIImpl("http://www.example.org/exampleDocument#G1"));
        Statement actual = RyaToRdfConversions.convertStatement(
            reader.getCurrentValue().getRyaStatement());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testBlockStatementQueue() throws Exception {
        RdfFileInputFormat.setRDFFormat(job, RDFFormat.NTRIPLES);
        RdfFileInputFormat.setStatementBufferSize(job, 2);
        init(NT_INPUT);
        // 3 statements in total, plus done signal: should fill up three times
        int interval = 100; // ms to sleep per iteration while waiting for statement cache to fill
        int maxSeconds = 120; // timeout that should never be reached
        for (int i = 0; i < 3; i++) {
            long t = 0;
            while (reader.statementCache.remainingCapacity() > 0) {
                if (t >= (maxSeconds*1000)) {
                    Assert.fail("Statement buffer still hasn't filled up after " + maxSeconds + " seconds.");
                }
                Assert.assertTrue(reader.statementCache.size() <= 2);
                Thread.sleep(interval);
                t += interval;
            }
            Assert.assertEquals(2, reader.statementCache.size());
            Assert.assertEquals(0, reader.statementCache.remainingCapacity());
            Assert.assertTrue(reader.nextKeyValue());
        }
        // Then the only thing in the queue should be the done signal
        Assert.assertSame(RdfFileInputFormat.DONE, reader.statementCache.peek());
        Assert.assertEquals(1, reader.statementCache.size());
        Assert.assertFalse(reader.nextKeyValue());
        Assert.assertTrue(reader.statementCache.isEmpty());
    }

    @Test
    public void testFailGracefully() throws Exception {
        // Pass the wrong RDF format and make sure all threads terminate
        int interval = 100; // ms to sleep per iteration while waiting for statement cache to fill
        int maxSeconds = 60; // timeout that should never be reached
        RdfFileInputFormat.setRDFFormat(job, RDFFormat.RDFXML);
        RdfFileInputFormat.setTimeout(job, maxSeconds*2);
        init(NT_INPUT);
        long t = 0;
        while (reader.statementCache.isEmpty()) {
            if (t >= (maxSeconds*1000)) {
                Assert.fail("Statement buffer still hasn't been sent error signal after " + maxSeconds + " seconds.");
            }
            Thread.sleep(interval);
            t += interval;
        }
        Assert.assertSame(RdfFileInputFormat.ERROR, reader.statementCache.peek());
        expected.expect(IOException.class);
        try {
            Assert.assertFalse(reader.nextKeyValue());
        }
        catch (Exception e) {
            Assert.assertNull(reader.getCurrentKey());
            Assert.assertNull(reader.getCurrentValue());
            Assert.assertFalse(reader.readerThread.isAlive());
            Assert.assertFalse(reader.parserThread.isAlive());
            throw e;
        }
    }
}
