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

package org.apache.rya.prospector.service

import com.google.common.collect.Iterators
import org.apache.rya.accumulo.AccumuloRdfConfiguration
import org.apache.rya.accumulo.AccumuloRyaDAO
import org.apache.rya.api.domain.RyaStatement
import org.apache.rya.api.domain.RyaType
import org.apache.rya.api.domain.RyaURI
import org.apache.rya.api.persist.RdfEvalStatsDAO
import org.apache.rya.prospector.mr.Prospector
import org.apache.accumulo.core.client.Instance
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ToolRunner
import org.junit.Test
import org.openrdf.model.impl.URIImpl
import org.openrdf.model.vocabulary.XMLSchema

import static org.junit.Assert.assertEquals
import org.openrdf.model.impl.LiteralImpl
import org.openrdf.model.Value

/**
 * Date: 1/26/13
 * Time: 3:00 PM
 */
class ProspectorServiceEvalStatsDAOTest {

    @Test
    public void testCount() throws Exception {

        Instance mock = new MockInstance("accumulo");

        def connector = mock.getConnector("user", "pass".bytes)
        def intable = "rya_spo"
        def outtable = "rya_prospects"
        if (connector.tableOperations().exists(outtable))
            connector.tableOperations().delete(outtable)
        connector.tableOperations().create(outtable)
        
        AccumuloRyaDAO ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        ryaDAO.init()

        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1234"), new RyaURI("urn:gem#pred"), new RyaType("mydata1")))
        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1234"), new RyaURI("urn:gem#pred"), new RyaType("mydata2")))
        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1234"), new RyaURI("urn:gem#pred"), new RyaType("12")))
        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1235"), new RyaURI("urn:gem#pred"), new RyaType(XMLSchema.INTEGER, "12")))
        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1235"), new RyaURI("urn:gem#pred1"), new RyaType("12")))

		def confFile = "stats_cluster_config.xml"
        def confPath = new Path(getClass().getClassLoader().getResource(confFile).toString())
        def args = (String[]) [confPath]; 
        ToolRunner.run(new Prospector(), args);
		debugTable(connector, outtable)
		
        def scanner = connector.createScanner(outtable, new Authorizations("U", "FOUO"))
        def iter = scanner.iterator()
//        assertEquals(11, Iterators.size(iter))

        ryaDAO.destroy()

        def conf = new Configuration()
        conf.addResource(confPath)
//        debugTable(connector, outtable)

        def rdfConf = new AccumuloRdfConfiguration(conf)
        rdfConf.setAuths("U","FOUO")
        def evalDao = new ProspectorServiceEvalStatsDAO(connector, rdfConf)
        evalDao.init()
		
		List<Value> values = new ArrayList<Value>();
		values.add( new URIImpl("urn:gem#pred"));

        def count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.PREDICATE, values)
        assertEquals(4.0, count, 0.001);

		values = new ArrayList<Value>();
		values.add( new LiteralImpl("mydata1"));

        count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.OBJECT, values);
        assertEquals(1.0, count, 0.001);

    values = new ArrayList<Value>();
    values.add( new LiteralImpl("mydata3"));

        count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.OBJECT, values);
        assertEquals(-1.0, count, 0.001);

                //should be in a teardown method
        connector.tableOperations().delete(outtable)
    }

    @Test
    public void testNoAuthsCount() throws Exception {

        Instance mock = new MockInstance("accumulo");
        def connector = mock.getConnector("user", "pass".bytes)
        def intable = "rya_spo"
        def outtable = "rya_prospects"
        if (connector.tableOperations().exists(outtable))
            connector.tableOperations().delete(outtable)
        connector.tableOperations().create(outtable)
        connector.securityOperations().createUser("user", "pass".bytes, new Authorizations("U", "FOUO"))

        AccumuloRyaDAO ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        ryaDAO.init()

        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1234"), new RyaURI("urn:gem#pred"), new RyaType("mydata1")))
        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1234"), new RyaURI("urn:gem#pred"), new RyaType("mydata2")))
        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1234"), new RyaURI("urn:gem#pred"), new RyaType("12")))
        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1235"), new RyaURI("urn:gem#pred"), new RyaType(XMLSchema.INTEGER, "12")))
        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1235"), new RyaURI("urn:gem#pred1"), new RyaType("12")))

		def confFile = "stats_cluster_config.xml"
        def confPath = new Path(getClass().getClassLoader().getResource(confFile).toString())
        def args = (String[]) [confPath]; 
        ToolRunner.run(new Prospector(), args);

        def scanner = connector.createScanner(outtable, new Authorizations("U", "FOUO"))
        def iter = scanner.iterator()
//        assertEquals(11, Iterators.size(iter))

        ryaDAO.destroy()

        def conf = new Configuration()
        conf.addResource(confPath)

        def rdfConf = new AccumuloRdfConfiguration(conf)
//        rdfConf.setAuths("U","FOUO")
        def evalDao = new ProspectorServiceEvalStatsDAO(connector, rdfConf)
        evalDao.init()

		
		List<Value> values = new ArrayList<Value>();
		values.add( new URIImpl("urn:gem#pred"));
        def count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.PREDICATE, values)
        assertEquals(4.0, count, 0.001);

		values = new ArrayList<Value>();
		values.add( new LiteralImpl("mydata1"));
        count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.OBJECT, values);
        assertEquals(1.0, count, 0.001);

    values = new ArrayList<Value>();
    values.add( new LiteralImpl("mydata3"));

        count = evalDao.getCardinality(rdfConf, RdfEvalStatsDAO.CARDINALITY_OF.OBJECT, values);
        assertEquals(-1.0, count, 0.001);

                //should be in a teardown method
        connector.tableOperations().delete(outtable)
    }

    private void debugTable(def connector, String table) {
        connector.createScanner(table, new Authorizations((String[]) ["U", "FOUO"])).iterator().each {
            println it
        }
    }
}
