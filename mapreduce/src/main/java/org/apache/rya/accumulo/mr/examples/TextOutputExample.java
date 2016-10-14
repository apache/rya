package mvm.rya.accumulo.mr.examples;

import java.io.BufferedReader;

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
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Date;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.accumulo.mr.AbstractAccumuloMRTool;
import mvm.rya.accumulo.mr.MRUtils;
import mvm.rya.accumulo.mr.RyaStatementWritable;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.resolver.RyaToRdfConversions;

/**
 * Example of using a MapReduce tool to get triples from a Rya instance and serialize them to a text file as RDF.
 */
public class TextOutputExample extends AbstractAccumuloMRTool {
    private static Logger logger = Logger.getLogger(TextOutputExample.class);
    private static RDFFormat rdfFormat = RDFFormat.NQUADS;
    private static String tempDir;

    // Connection information
    private static final String USERNAME = "root";
    private static final String PASSWORD = "";
    private static final String INSTANCE_NAME = "instanceName";
    private static final String PREFIX = "rya_example_";

    public static void main(String[] args) throws Exception {
        setUpRya();
        TextOutputExample tool = new TextOutputExample();
        ToolRunner.run(new Configuration(), tool, args);
    }

    static void setUpRya() throws AccumuloException, AccumuloSecurityException, RyaDAOException {
        MockInstance mock = new MockInstance(INSTANCE_NAME);
        Connector conn = mock.getConnector(USERNAME, new PasswordToken(PASSWORD));
        AccumuloRyaDAO dao = new AccumuloRyaDAO();
        dao.setConnector(conn);
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(PREFIX);
        dao.setConf(conf);
        dao.init();
        String ns = "http://example.com/";
        dao.add(new RyaStatement(new RyaURI(ns+"s1"), new RyaURI(ns+"p1"), new RyaURI(ns+"o1")));
        dao.add(new RyaStatement(new RyaURI(ns+"s1"), new RyaURI(ns+"p2"), new RyaURI(ns+"o2")));
        dao.add(new RyaStatement(new RyaURI(ns+"s2"), new RyaURI(ns+"p1"), new RyaURI(ns+"o3"),
                new RyaURI(ns+"g1")));
        dao.add(new RyaStatement(new RyaURI(ns+"s3"), new RyaURI(ns+"p3"), new RyaURI(ns+"o3"),
                new RyaURI(ns+"g2")));
        dao.destroy();
    }

    @Override
    public int run(String[] args) throws Exception {
        logger.info("Configuring tool to connect to mock instance...");
        MRUtils.setACUserName(conf, USERNAME);
        MRUtils.setACPwd(conf, PASSWORD);
        MRUtils.setACInstance(conf, INSTANCE_NAME);
        MRUtils.setACMock(conf, true);
        MRUtils.setTablePrefix(conf, PREFIX);

        logger.info("Initializing tool and checking configuration...");
        init();

        logger.info("Creating Job, setting Mapper class, and setting no Reducer...");
        Job job = Job.getInstance(conf);
        job.setJarByClass(TextOutputExample.class);
        job.setMapperClass(RyaToRdfMapper.class);
        job.setNumReduceTasks(0);

        logger.info("Configuring Job to take input from the mock Rya instance...");
        setupRyaInput(job);

        logger.info("Configuring Job to output Text to a new temporary directory...");
        tempDir = Files.createTempDirectory("rya-mr-example").toString();
        Path outputPath = new Path(tempDir, "rdf-output");
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, outputPath);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Date start = new Date();
        logger.info("Starting Job at: start");
        boolean success = job.waitForCompletion(true);

        if (!success) {
            System.out.println("Job Failed!!!");
            return 1;
        }

        Date end = new Date();
        logger.info("Job ended: " + end);
        logger.info("The job took " + (end.getTime() - start.getTime()) / 1000 + " seconds.");
        // Print output and then delete temp files:
        java.nio.file.Path tempPath = FileSystems.getDefault().getPath(tempDir);
        for (java.nio.file.Path subdir : Files.newDirectoryStream(tempPath)) {
            logger.info("");
            logger.info("Output files:");
            for (java.nio.file.Path outputFile : Files.newDirectoryStream(subdir)) {
                logger.info("\t" + outputFile);
            }
            for (java.nio.file.Path outputFile : Files.newDirectoryStream(subdir, "part*")) {
                logger.info("");
                logger.info("Contents of " + outputFile + ":");
                BufferedReader reader = Files.newBufferedReader(outputFile, Charset.defaultCharset());
                String line;
                while ((line = reader.readLine()) != null) {
                    logger.info("\t" + line);
                }
                reader.close();
            }
            for (java.nio.file.Path outputFile : Files.newDirectoryStream(subdir)) {
                Files.deleteIfExists(outputFile);
            }
            Files.deleteIfExists(subdir);
        }
        Files.deleteIfExists(tempPath);
        logger.info("");
        logger.info("Temporary directory " + tempDir + " deleted.");

        return 0;
    }

    static class RyaToRdfMapper extends Mapper<Text, RyaStatementWritable, NullWritable, Text> {
        Text textOut = new Text();
        @Override
        protected void map(Text key, RyaStatementWritable value, Context context) throws IOException, InterruptedException {
            // receives a RyaStatementWritable; convert to a Statement
            RyaStatement rstmt = value.getRyaStatement();
            Statement st = RyaToRdfConversions.convertStatement(rstmt);
            logger.info("Mapper receives: " + rstmt);
            // then convert to an RDF string
            StringWriter writer = new StringWriter();
            try {
                RDFWriter rdfWriter = Rio.createWriter(rdfFormat, writer);
                rdfWriter.startRDF();
                rdfWriter.handleStatement(st);
                rdfWriter.endRDF();
            } catch (RDFHandlerException e) {
                throw new IOException("Error writing RDF data", e);
            }
            // Write the string to the output
            String line = writer.toString().trim();
            logger.info("Serialized to RDF: " + line);
            textOut.set(line);
            context.write(NullWritable.get(), textOut);
        }
    }
}