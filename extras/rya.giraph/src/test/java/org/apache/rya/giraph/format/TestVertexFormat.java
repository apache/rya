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

package org.apache.rya.giraph.format;

import java.io.IOException;

import org.apache.giraph.BspCase;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.sail.config.RyaSailFactory;
import org.junit.Test;

/*
    Test class for Rya vertex input formats.
 */
public class TestVertexFormat extends BspCase {

    private final Logger log = Logger.getLogger(TestVertexFormat.class);

    /**
     * Create the test case
     */
    public TestVertexFormat() {
        super(TestVertexFormat.class.getName());
        System.setProperty("java.io.tmpdir", "target/test");
}
      
    private static AccumuloRdfConfiguration getConf() {

        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        conf.set(ConfigUtils.USE_PCJ, "false");
        conf.set(ConfigUtils.USE_FREETEXT, "false");
        conf.set(ConfigUtils.USE_TEMPORAL, "false");
        conf.set(RdfTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");
        conf.set(ConfigUtils.CLOUDBASE_USER, "root");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "test");
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "");
        return conf;
    }

     /*
    Write a simple parent-child directed graph to Cloudbase.
    Run a job which reads the values
    into subclasses that extend CloudbaseVertex I/O formats.
    Check the output after the job.
    */
    @Test
    public void testRyaInput() throws Exception {

        AccumuloRdfConfiguration conf = getConf();
        AccumuloRyaDAO ryaDAO = RyaSailFactory.getAccumuloDAO(conf);

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
        ryaDAO.flush();

        GiraphJob job = new GiraphJob(conf, getCallingMethodName());

        setupConfiguration(job);
        GiraphConfiguration giraphConf = job.getConfiguration();
        giraphConf.setComputationClass(EdgeNotification.class);
        giraphConf.setVertexInputFormatClass(RyaVertexInputFormat.class);
        giraphConf.setVertexOutputFormatClass(TestTextOutputFormat.class);


        if (log.isInfoEnabled())
            log.info("Running edge notification job using Rya Vertex input");

    }
    
    /*
    Test compute method that sends each edge a notification of its parents.
    The test set only has a 1-1 parent-to-child ratio for this unit test.
     */
    public static class EdgeNotification
            extends BasicComputation<Text, Text, Text, Text> {
      @Override
      public void compute(Vertex<Text, Text, Text> vertex,
          Iterable<Text> messages) throws IOException {
          for (Text message : messages) {
            vertex.getValue().set(message);
          }
          if(getSuperstep() == 0) {
            sendMessageToAllEdges(vertex, vertex.getId());
          }
        vertex.voteToHalt();
      }
}
}
