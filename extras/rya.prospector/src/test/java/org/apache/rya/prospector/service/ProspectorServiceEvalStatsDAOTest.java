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
package org.apache.rya.prospector.service;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.apache.rya.api.persist.RdfEvalStatsDAO.CARDINALITY_OF;
import org.apache.rya.prospector.mr.Prospector;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;

/**
 * Tests that show when the {@link Prospector} job is run, the
 * {@link ProspectorServiceEvalStatsDAO} may be used to fetch cardinality
 * information from the prospect table.
 */
public class ProspectorServiceEvalStatsDAOTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Test
    public void testCount() throws Exception {
        // Load some data into a mock Accumulo and run the Prospector MapReduce job.
        final Instance mock = new MockInstance("accumulo");

        final Connector connector = mock.getConnector("user", new PasswordToken("pass"));
        final String outtable = "rya_prospects";
        if (connector.tableOperations().exists(outtable)) {
            connector.tableOperations().delete(outtable);
        }
        connector.tableOperations().create(outtable);

        final AccumuloRyaDAO ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        ryaDAO.init();

        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("mydata1")));
        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("mydata2")));
        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("12")));
        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1235"), new RyaIRI("urn:gem#pred"), new RyaType(XMLSchema.INTEGER, "12")));
        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1235"), new RyaIRI("urn:gem#pred1"), new RyaType("12")));

        final String confFile = "stats_cluster_config.xml";
        final Path confPath = new Path(getClass().getClassLoader().getResource(confFile).toString());
        final String[] args = { confPath.toString() };
        Assert.assertEquals("MapReduce job failed!", 0, ToolRunner.run(new Prospector(), args));

        ryaDAO.destroy();

        final Configuration conf = new Configuration();
        conf.addResource(confPath);

        final AccumuloRdfConfiguration rdfConf = new AccumuloRdfConfiguration(conf);
        rdfConf.setAuths("U","FOUO");
        final ProspectorServiceEvalStatsDAO evalDao = new ProspectorServiceEvalStatsDAO(connector, rdfConf);
        evalDao.init();

        // Get the cardinality of the 'urn:gem#pred' predicate.
        List<Value> values = new ArrayList<Value>();
        values.add( VF.createIRI("urn:gem#pred") );
        double count = evalDao.getCardinality(rdfConf, CARDINALITY_OF.PREDICATE, values);
        assertEquals(4.0, count, 0.001);

        // Get the cardinality of the 'mydata1' object.
        values = new ArrayList<Value>();
        values.add( VF.createLiteral("mydata1"));
        count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.OBJECT, values);
        assertEquals(1.0, count, 0.001);

        // Get the cardinality of the 'mydata3' object.
        values = new ArrayList<Value>();
        values.add( VF.createLiteral("mydata3"));
        count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.OBJECT, values);
        assertEquals(-1.0, count, 0.001);
    }

    @Test
    public void testNoAuthsCount() throws Exception {
        // Load some data into a mock Accumulo and run the Prospector MapReduce job.
        final Instance mock = new MockInstance("accumulo");

        final Connector connector = mock.getConnector("user", new PasswordToken("pass"));
        final String outtable = "rya_prospects";
        if (connector.tableOperations().exists(outtable)) {
            connector.tableOperations().delete(outtable);
        }
        connector.tableOperations().create(outtable);
        connector.securityOperations().createUser("user", "pass".getBytes(), new Authorizations("U", "FOUO"));

        final AccumuloRyaDAO ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        ryaDAO.init();

        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("mydata1")));
        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("mydata2")));
        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1234"), new RyaIRI("urn:gem#pred"), new RyaType("12")));
        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1235"), new RyaIRI("urn:gem#pred"), new RyaType(XMLSchema.INTEGER, "12")));
        ryaDAO.add(new RyaStatement(new RyaIRI("urn:gem:etype#1235"), new RyaIRI("urn:gem#pred1"), new RyaType("12")));

        final String confFile = "stats_cluster_config.xml";
        final Path confPath = new Path(getClass().getClassLoader().getResource(confFile).toString());
        final String[] args = { confPath.toString() };
        assertEquals("MapReduce job failed!", 0, ToolRunner.run(new Prospector(), args));

        ryaDAO.destroy();

        final Configuration conf = new Configuration();
        conf.addResource(confPath);

        final AccumuloRdfConfiguration rdfConf = new AccumuloRdfConfiguration(conf);
        final ProspectorServiceEvalStatsDAO evalDao = new ProspectorServiceEvalStatsDAO(connector, rdfConf);
        evalDao.init();

        // Get the cardinality of the 'urn:gem#pred' predicate.
        List<Value> values = new ArrayList<Value>();
        values.add( VF.createIRI("urn:gem#pred"));
        double count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.PREDICATE, values);
        assertEquals(4.0, count, 0.001);

        // Get the cardinality of the 'mydata1' object.
        values = new ArrayList<Value>();
        values.add( VF.createLiteral("mydata1"));
        count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.OBJECT, values);
        assertEquals(1.0, count, 0.001);

        // Get the cardinality of the 'mydata3' object.
        values = new ArrayList<Value>();
        values.add( VF.createLiteral("mydata3"));
        count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.OBJECT, values);
        assertEquals(-1.0, count, 0.001);
    }

    /**
     * Prints the content of an Accumulo table to standard out. Only use then when
     * debugging the test.
     */
    private void debugTable(Connector connector, String table) throws TableNotFoundException {
        final Iterator<Entry<Key, org.apache.accumulo.core.data.Value>> it = connector.createScanner(table, new Authorizations("U", "FOUO")).iterator();
        while(it.hasNext()) {
            System.out.println( it.next() );
        }
    }
}