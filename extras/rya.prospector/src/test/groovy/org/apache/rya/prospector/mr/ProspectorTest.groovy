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

package org.apache.rya.prospector.mr

import com.google.common.collect.Iterators
import com.google.common.collect.Lists
import org.apache.rya.accumulo.AccumuloRyaDAO
import org.apache.rya.accumulo.AccumuloRdfConfiguration
import org.apache.rya.api.persist.RdfEvalStatsDAO
import org.apache.rya.api.domain.RyaStatement
import org.apache.rya.api.domain.RyaType
import org.apache.rya.api.domain.RyaURI
import org.apache.rya.prospector.domain.IndexEntry
import org.apache.rya.prospector.domain.TripleValueType
import org.apache.rya.prospector.service.ProspectorService
import org.apache.rya.prospector.service.ProspectorServiceEvalStatsDAO
import org.apache.rya.prospector.utils.ProspectorConstants
import org.apache.accumulo.core.client.Instance
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.ToolRunner
import org.junit.Test
import org.openrdf.model.vocabulary.XMLSchema
import org.openrdf.model.impl.URIImpl

import static org.junit.Assert.assertEquals
import org.openrdf.model.impl.LiteralImpl
import org.openrdf.model.Value

/**
 * Date: 12/4/12
 * Time: 4:33 PM
 */
class ProspectorTest {

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
      //  debugTable(mrInfo, outtable)

        def service = new ProspectorService(connector, outtable)
        def auths = (String[]) ["U", "FOUO"]
        def prospects = service.getProspects(auths)
        def plist = Lists.newArrayList(prospects)
        assertEquals(1, plist.size())
		
		def rdfConf = new AccumuloRdfConfiguration(conf)
		rdfConf.setAuths("U","FOUO")

        prospects = service.getProspectsInRange(System.currentTimeMillis() - 100000, System.currentTimeMillis() + 10000, auths)
        plist = Lists.newArrayList(prospects)
        assertEquals(1, plist.size())
		
		List<String> queryTerms = new ArrayList<String>();
		queryTerms.add("urn:gem:etype");
        def query = service.query(plist, ProspectorConstants.COUNT, TripleValueType.entity.name(), queryTerms, XMLSchema.ANYURI.stringValue(), auths)
        assertEquals(1, query.size())
//        assertEquals(
//                new IndexEntry(index: ProspectorConstants.COUNT, data: "urn:gem:etype", dataType: XMLSchema.ANYURI.stringValue(),
//                        tripleValueType: TripleValueType.entity, visibility: "", count: -1, timestamp: plist.get(0)),
//                query.get(0))
		
		queryTerms = new ArrayList<String>();
		queryTerms.add("urn:gem:etype#1234");
        query = service.query(plist, ProspectorConstants.COUNT, TripleValueType.subject.name(), queryTerms, XMLSchema.ANYURI.stringValue(), auths)
        assertEquals(1, query.size())
		
		queryTerms = new ArrayList<String>();
		queryTerms.add("urn:gem#pred");
        query = service.query(plist, ProspectorConstants.COUNT, TripleValueType.predicate.name(), queryTerms, XMLSchema.ANYURI.stringValue(), auths)
        assertEquals(1, query.size())
        assertEquals(
                new IndexEntry(index: ProspectorConstants.COUNT, data: "urn:gem#pred", dataType: XMLSchema.ANYURI.stringValue(),
                        tripleValueType: TripleValueType.predicate, visibility: "", count: 4l, timestamp: plist.get(0)),
                query.get(0))
		
		queryTerms = new ArrayList<String>();
		queryTerms.add("mydata1");
        query = service.query(plist, ProspectorConstants.COUNT, TripleValueType.object.name(), queryTerms, XMLSchema.STRING.stringValue(), auths)
        assertEquals(1, query.size())
//        assertEquals(
//                new IndexEntry(index: ProspectorConstants.COUNT, data: "mydata1", dataType: XMLSchema.STRING.stringValue(),
//                        tripleValueType: TripleValueType.object, visibility: "", count: -1, timestamp: plist.get(0)),
//                query.get(0))
		
		queryTerms = new ArrayList<String>();
		queryTerms.add("urn:gem:etype#1234");
		queryTerms.add("urn:gem#pred");
		query = service.query(plist, ProspectorConstants.COUNT, TripleValueType.subjectpredicate.name(), queryTerms, XMLSchema.STRING.stringValue(), auths)
		assertEquals(1, query.size())
//		assertEquals(
//				new IndexEntry(index: ProspectorConstants.COUNT, data: "urn:gem:etype#1234" + "\u0000" + "urn:gem#pred", dataType: XMLSchema.STRING.stringValue(),
//						tripleValueType: TripleValueType.subjectpredicate, visibility: "", count: -1, timestamp: plist.get(0)),
//				query.get(0))

		queryTerms = new ArrayList<String>();
		queryTerms.add("urn:gem#pred");
		queryTerms.add("12");
		query = service.query(plist, ProspectorConstants.COUNT, TripleValueType.predicateobject.name(), queryTerms, XMLSchema.STRING.stringValue(), auths)
		assertEquals(1, query.size())
//		assertEquals(
//				new IndexEntry(index: ProspectorConstants.COUNT, data: "urn:gem#pred" + "\u0000" + "12", dataType: XMLSchema.STRING.stringValue(),
//						tripleValueType: TripleValueType.predicateobject, visibility: "", count: -1, timestamp: plist.get(0)),
//				query.get(0))

		queryTerms = new ArrayList<String>();
		queryTerms.add("urn:gem:etype#1234");
		queryTerms.add("mydata1");
		query = service.query(plist, ProspectorConstants.COUNT, TripleValueType.subjectobject.name(), queryTerms, XMLSchema.STRING.stringValue(), auths)
		
		assertEquals(1, query.size())
//		assertEquals(
//				new IndexEntry(index: ProspectorConstants.COUNT, data: "urn:gem:etype#1234" + "\u0000" + "mydata1", dataType: XMLSchema.STRING.stringValue(),
//						tripleValueType: TripleValueType.subjectobject, visibility: "", count: -1, timestamp: plist.get(0)),
//				query.get(0))

        //should be in a teardown method
        connector.tableOperations().delete(outtable)
    }

    private void debugTable(def connector, String table) {
        connector.createScanner(table, new Authorizations((String[]) ["U", "FOUO"])).iterator().each {
            println it
        }
    }
}
