/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mvm.rya.cloudbase.giraph.format;

import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.Connector;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.client.mapreduce.CloudbaseInputFormat;
import cloudbase.core.client.mock.MockInstance;
import cloudbase.core.data.Range;
import cloudbase.core.security.Authorizations;
import junit.framework.Test;
import junit.framework.TestSuite;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.cloudbase.CloudbaseRdfConfiguration;
import mvm.rya.cloudbase.CloudbaseRyaDAO;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/*
    Test class for Cloudbase vertex input/output formats.
 */
public class TestCloudbaseVertexFormat extends BspCase {

    private final String TABLE_NAME = "rya_spo";
    private final String INSTANCE_NAME = "stratus";
    private final Text FAMILY = new Text("cf");
    private final Text CHILDREN = new Text("children");
    private final String USER = "root";
    private final byte[] PASSWORD = new byte[]{};
    private final Text OUTPUT_FIELD = new Text("parent");


    private final Logger log = Logger.getLogger(TestCloudbaseVertexFormat.class);

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TestCloudbaseVertexFormat(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(TestCloudbaseVertexFormat.class);

    }

    /*
    Write a simple parent-child directed graph to Cloudbase.
    Run a job which reads the values
    into subclasses that extend CloudbaseVertex I/O formats.
    Check the output after the job.
    */
    public void testCloudbaseInputOutput() throws Exception {
//        if (System.getProperty("prop.mapred.job.tracker") != null) {
//            if(log.isInfoEnabled())
//                log.info("testCloudbaseInputOutput: " +
//                        "Ignore this test if not local mode.");
//            return;
//        }
//
//        System.setProperty("prop.jarLocation", "/temp/cloudbase.rya.giraph-3.0.0-SNAPSHOT.jar");
        File jarTest = new File(System.getProperty("prop.jarLocation"));
        if (!jarTest.exists()) {
            fail("Could not find Giraph jar at " +
                    "location specified by 'prop.jarLocation'. " +
                    "Make sure you built the main Giraph artifact?.");
        }

        //Write out vertices and edges out to a mock instance.
//        MockInstance mockInstance = new MockInstance(INSTANCE_NAME);
        Connector c = new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password".getBytes());
        CloudbaseRyaDAO ryaDAO = new CloudbaseRyaDAO();
        ryaDAO.setConnector(c);
        CloudbaseRdfConfiguration cloudbaseRdfConfiguration = new CloudbaseRdfConfiguration();
//        cloudbaseRdfConfiguration.setTablePrefix("test_");
        ryaDAO.init();
//        c.tableOperations().create(TABLE_NAME);
//        BatchWriter bw = c.createBatchWriter(TABLE_NAME, 10000L, 1000L, 4);

        ryaDAO.add(new RyaStatement(new RyaURI("urn:test#1234"),
                new RyaURI("urn:test#pred1"),
                new RyaURI("urn:test#obj1")));
        ryaDAO.add(new RyaStatement(new RyaURI("urn:test#1234"),
                new RyaURI("urn:test#pred2"),
                new RyaURI("urn:test#obj2")));
        ryaDAO.add(new RyaStatement(new RyaURI("urn:test#1234"),
                new RyaURI("urn:test#pred3"),
                new RyaURI("urn:test#obj3")));
        ryaDAO.add(new RyaStatement(new RyaURI("urn:test#1234"),
                new RyaURI("urn:test#pred4"),
                new RyaURI("urn:test#obj4")));
        ryaDAO.commit();

//        Mutation m1 = new Mutation(new Text("0001"));
//        m1.put(FAMILY, CHILDREN, new Value("0002".getBytes()));
//        bw.addMutation(m1);
//
//        Mutation m2 = new Mutation(new Text("0002"));
//        m2.put(FAMILY, CHILDREN, new Value("0003".getBytes()));
//        bw.addMutation(m2);
//        if(log.isInfoEnabled())
//            log.info("Writing mutations to Cloudbase table");
//        bw.close();

        Configuration conf = new Configuration();
//        conf.set(CloudbaseVertexOutputFormat.OUTPUT_TABLE, TABLE_NAME);

        /*
        Very important to initialize the formats before
        sending configuration to the GiraphJob. Otherwise
        the internally constructed Job in GiraphJob will
        not have the proper context initialization.
         */
        GiraphJob job = new GiraphJob(conf, getCallingMethodName());
        CloudbaseInputFormat.setInputInfo(job.getInternalJob(), USER, "password".getBytes(),
                TABLE_NAME, new Authorizations());
//        CloudbaseInputFormat.setMockInstance(job.getInternalJob(), INSTANCE_NAME);
        CloudbaseInputFormat.setZooKeeperInstance(job.getInternalJob(), "stratus", "stratus13:2181");
        CloudbaseInputFormat.setRanges(job.getInternalJob(), Collections.singleton(new Range()));

//        CloudbaseOutputFormat.setOutputInfo(job.getInternalJob(), USER, PASSWORD, true, null);
//        CloudbaseOutputFormat.setMockInstance(job.getInternalJob(), INSTANCE_NAME);

        setupConfiguration(job);
        job.setVertexClass(EdgeNotification.class);
        job.setVertexInputFormatClass(CloudbaseRyaVertexInputFormat.class);
        job.setVertexOutputFormatClass(PrintVertexOutputFormat.class);
        FileOutputFormat.setOutputPath(job.getInternalJob(), new Path("/temp/graphout"));

//        HashSet<Pair<Text, Text>> columnsToFetch = new HashSet<Pair<Text,Text>>();
//        columnsToFetch.add(new Pair<Text, Text>(FAMILY, CHILDREN));
//        CloudbaseInputFormat.fetchColumns(job.getInternalJob(), columnsToFetch);

        if (log.isInfoEnabled())
            log.info("Running edge notification job using Cloudbase input");
        assertTrue(job.run(true));

//        Scanner scanner = c.createScanner(TABLE_NAME, new Authorizations());
//        scanner.setRange(new Range("0002", "0002"));
//        scanner.fetchColumn(FAMILY, OUTPUT_FIELD);
//        boolean foundColumn = false;
//
//        if(log.isInfoEnabled())
//            log.info("Verify job output persisted correctly.");
//        //make sure we found the qualifier.
//        assertTrue(scanner.iterator().hasNext());
//
//
//        //now we check to make sure the expected value from the job persisted correctly.
//        for(Map.Entry<Key,Value> entry : scanner) {
//            Text row = entry.getKey().getRow();
//            assertEquals("0002", row.toString());
//            Value value = entry.getValue();
//            assertEquals("0001", ByteBufferUtil.toString(
//                    ByteBuffer.wrap(value.get())));
//            foundColumn = true;
//        }
    }

    /*
   Test compute method that sends each edge a notification of its parents.
   The test set only has a 1-1 parent-to-child ratio for this unit test.
    */
    public static class EdgeNotification
            extends EdgeListVertex<Text, Text, Text, Text> {
        @Override
        public void compute(Iterable<Text> messages) throws IOException {
            System.out.println("Edges: " + messages);
            for (Text message : messages) {
                getValue().set(message);
            }
            if (getSuperstep() == 0) {
//                sendMessageToAllEdges(getId());
            }
            voteToHalt();
        }
    }
}
