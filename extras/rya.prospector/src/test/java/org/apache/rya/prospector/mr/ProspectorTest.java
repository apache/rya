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
package org.apache.rya.prospector.mr;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.prospector.domain.IndexEntry;
import org.apache.rya.prospector.domain.TripleValueType;
import org.apache.rya.prospector.service.ProspectorService;
import org.apache.rya.prospector.utils.ProspectorConstants;
import org.junit.Test;
import org.openrdf.model.vocabulary.XMLSchema;

import com.google.common.collect.Lists;

/**
 * Tests that show when the {@link Prospector} job is run, it creates a table
 * containing the correct count information derived from the statements that
 * have been stored within a Rya instance.
 */
public class ProspectorTest {

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

        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1234"), new RyaURI("urn:gem#pred"), new RyaType("mydata1")));
        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1234"), new RyaURI("urn:gem#pred"), new RyaType("mydata2")));
        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1234"), new RyaURI("urn:gem#pred"), new RyaType("12")));
        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1235"), new RyaURI("urn:gem#pred"), new RyaType(XMLSchema.INTEGER, "12")));
        ryaDAO.add(new RyaStatement(new RyaURI("urn:gem:etype#1235"), new RyaURI("urn:gem#pred1"), new RyaType("12")));

        final String confFile = "stats_cluster_config.xml";
        final Path confPath = new Path(getClass().getClassLoader().getResource(confFile).toString());
        final String[] args = { confPath.toString() };
        ToolRunner.run(new Prospector(), args);
        ryaDAO.destroy();

        // Interrogate the results of the Prospect job to ensure the correct results were created.
        final Configuration conf = new Configuration();
        conf.addResource(confPath);

        final ProspectorService service = new ProspectorService(connector, outtable);
        final String[] auths = {"U", "FOUO"};
        Iterator<Long> prospects = service.getProspects(auths);
        List<Long> plist = Lists.newArrayList(prospects);
        assertEquals(1, plist.size());

        final Long prospectTimestamp = plist.iterator().next();

        final AccumuloRdfConfiguration rdfConf = new AccumuloRdfConfiguration(conf);
        rdfConf.setAuths("U","FOUO");

        prospects = service.getProspectsInRange(System.currentTimeMillis() - 100000, System.currentTimeMillis() + 10000, auths);
        plist = Lists.newArrayList(prospects);
        assertEquals(1, plist.size());

        // Ensure one of the correct "entity" counts was created.
        List<String> queryTerms = new ArrayList<>();
        queryTerms.add("urn:gem:etype");
        final List<IndexEntry> entityEntries = service.query(plist, ProspectorConstants.COUNT, TripleValueType.ENTITY.getIndexType(), queryTerms, XMLSchema.ANYURI.stringValue(), auths);

        final List<IndexEntry> expectedEntityEntries = Lists.newArrayList(
                IndexEntry.builder()
                    .setIndex(ProspectorConstants.COUNT)
                    .setData("urn:gem:etype")
                    .setDataType(XMLSchema.ANYURI.stringValue())
                    .setTripleValueType( TripleValueType.ENTITY.getIndexType() )
                    .setVisibility("")
                    .setTimestamp(prospectTimestamp)
                    .setCount(new Long(5))
                    .build());

        assertEquals(expectedEntityEntries, entityEntries);

        // Ensure one of the correct "subject" counts was created.
        queryTerms = new ArrayList<String>();
        queryTerms.add("urn:gem:etype#1234");
        final List<IndexEntry> subjectEntries = service.query(plist, ProspectorConstants.COUNT, TripleValueType.SUBJECT.getIndexType(), queryTerms, XMLSchema.ANYURI.stringValue(), auths);

        final List<IndexEntry> expectedSubjectEntries = Lists.newArrayList(
                IndexEntry.builder()
                    .setIndex(ProspectorConstants.COUNT)
                    .setData("urn:gem:etype#1234")
                    .setDataType(XMLSchema.ANYURI.stringValue())
                    .setTripleValueType( TripleValueType.SUBJECT.getIndexType() )
                    .setVisibility("")
                    .setTimestamp(prospectTimestamp)
                    .setCount(new Long(3))
                    .build());

        assertEquals(expectedSubjectEntries, subjectEntries);

        // Ensure one of the correct "predicate" counts was created.
        queryTerms = new ArrayList<String>();
        queryTerms.add("urn:gem#pred");
        final List<IndexEntry> predicateEntries = service.query(plist, ProspectorConstants.COUNT, TripleValueType.PREDICATE.getIndexType(), queryTerms, XMLSchema.ANYURI.stringValue(), auths);

        final List<IndexEntry> expectedPredicateEntries = Lists.newArrayList(
                IndexEntry.builder()
                    .setIndex(ProspectorConstants.COUNT)
                    .setData("urn:gem#pred")
                    .setDataType(XMLSchema.ANYURI.stringValue())
                    .setTripleValueType( TripleValueType.PREDICATE.getIndexType() )
                    .setVisibility("")
                    .setTimestamp(prospectTimestamp)
                    .setCount(new Long(4))
                    .build());

        assertEquals(expectedPredicateEntries, predicateEntries);

        // Ensure one of the correct "object" counts was created.
        queryTerms = new ArrayList<String>();
        queryTerms.add("mydata1");
        final List<IndexEntry> objectEntries = service.query(plist, ProspectorConstants.COUNT, TripleValueType.OBJECT.getIndexType(), queryTerms, XMLSchema.STRING.stringValue(), auths);

        final List<IndexEntry> expectedObjectEntries = Lists.newArrayList(
                IndexEntry.builder()
                    .setIndex(ProspectorConstants.COUNT)
                    .setData("mydata1")
                    .setDataType(XMLSchema.STRING.stringValue())
                    .setTripleValueType( TripleValueType.OBJECT.getIndexType() )
                    .setVisibility("")
                    .setTimestamp(prospectTimestamp)
                    .setCount(new Long(1))
                    .build());

        assertEquals(expectedObjectEntries, objectEntries);

        // Ensure one of the correct "subjectpredicate" counts was created.
        queryTerms = new ArrayList<String>();
        queryTerms.add("urn:gem:etype#1234");
        queryTerms.add("urn:gem#pred");
        final List<IndexEntry> subjectPredicateEntries = service.query(plist, ProspectorConstants.COUNT, TripleValueType.SUBJECT_PREDICATE.getIndexType(), queryTerms, XMLSchema.STRING.stringValue(), auths);

        final List<IndexEntry> expectedSubjectPredicateEntries = Lists.newArrayList(
                IndexEntry.builder()
                    .setIndex(ProspectorConstants.COUNT)
                    .setData("urn:gem:etype#1234"+ "\u0000" + "urn:gem#pred")
                    .setDataType(XMLSchema.STRING.stringValue())
                    .setTripleValueType( TripleValueType.SUBJECT_PREDICATE.getIndexType() )
                    .setVisibility("")
                    .setTimestamp(prospectTimestamp)
                    .setCount(new Long(3))
                    .build());

        assertEquals(expectedSubjectPredicateEntries, subjectPredicateEntries);

        // Ensure one of the correct "predicateobject" counts was created.
        queryTerms = new ArrayList<String>();
        queryTerms.add("urn:gem#pred");
        queryTerms.add("12");
        final List<IndexEntry> predicateObjectEntries = service.query(plist, ProspectorConstants.COUNT, TripleValueType.PREDICATE_OBJECT.getIndexType(), queryTerms, XMLSchema.STRING.stringValue(), auths);

        final List<IndexEntry> expectedPredicateObjectEntries = Lists.newArrayList(
                IndexEntry.builder()
                    .setIndex(ProspectorConstants.COUNT)
                    .setData("urn:gem#pred" + "\u0000" + "12")
                    .setDataType(XMLSchema.STRING.stringValue())
                    .setTripleValueType( TripleValueType.PREDICATE_OBJECT.getIndexType() )
                    .setVisibility("")
                    .setTimestamp(prospectTimestamp)
                    .setCount(new Long(2)) // XXX This might be a bug. The object matching doesn't care about type.
                    .build());

        assertEquals(expectedPredicateObjectEntries, predicateObjectEntries);

        // Ensure one of the correct "" counts was created.
        queryTerms = new ArrayList<String>();
        queryTerms.add("urn:gem:etype#1234");
        queryTerms.add("mydata1");
        final List<IndexEntry> subjectObjectEntries = service.query(plist, ProspectorConstants.COUNT, TripleValueType.SUBJECT_OBJECT.getIndexType(), queryTerms, XMLSchema.STRING.stringValue(), auths);

        final List<IndexEntry> expectedSubjectObjectEntries = Lists.newArrayList(
                IndexEntry.builder()
                    .setIndex(ProspectorConstants.COUNT)
                    .setData("urn:gem:etype#1234" + "\u0000" + "mydata1")
                    .setDataType(XMLSchema.STRING.stringValue())
                    .setTripleValueType( TripleValueType.SUBJECT_OBJECT.getIndexType() )
                    .setVisibility("")
                    .setTimestamp(prospectTimestamp)
                    .setCount(new Long(1))
                    .build());

        assertEquals(expectedSubjectObjectEntries, subjectObjectEntries);
    }

    /**
     * Prints the content of an Accumulo table to standard out. Only use then when
     * debugging the test.
     */
    private void debugTable(Connector connector, String table) throws TableNotFoundException {
        final Iterator<Entry<Key, Value>> it = connector.createScanner(table, new Authorizations(new String[]{"U", "FOUO"})).iterator();
        while(it.hasNext()) {
            final Entry<Key, Value> entry = it.next();
            System.out.println( entry );
        }
    }
}