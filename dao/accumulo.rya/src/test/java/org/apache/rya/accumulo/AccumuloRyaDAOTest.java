package org.apache.rya.accumulo;

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

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.rya.accumulo.query.AccumuloRyaQueryEngine;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.StatementMetadata;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.utils.RyaDAOHelper;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaContext;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Class AccumuloRdfDAOTest
 * Date: Mar 7, 2012
 * Time: 9:42:28 AM
 */
public class AccumuloRyaDAOTest {

    private static final String AUTHS = "U";
    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    private static final String litdupsNS = "urn:test:litdups#";
    private static final long TIMESTAMP = 1587709670229L;

    private AccumuloRyaDAO dao;
    private AccumuloRdfConfiguration conf;
    private Connector connector;

    RyaIRI cpu = new RyaIRI(litdupsNS + "cpu");
    RyaIRI loadPerc = new RyaIRI(litdupsNS + "loadPerc");
    RyaIRI disk = new RyaIRI(litdupsNS + "disk");
    RyaIRI diskPerc = new RyaIRI(litdupsNS + "diskPerc");
    RyaIRI net = new RyaIRI(litdupsNS + "net");
    RyaIRI netPerc = new RyaIRI(litdupsNS + "netPerc");
    RyaIRI uri1 = new RyaIRI(litdupsNS + "uri1");
    RyaIRI uri2 = new RyaIRI(litdupsNS + "uri2");
    RyaIRI uri3 = new RyaIRI(litdupsNS + "uri3");
    RyaIRI uri4 = new RyaIRI(litdupsNS + "uri4");
    RyaIRI uri5 = new RyaIRI(litdupsNS + "uri5");
    RyaIRI uri6 = new RyaIRI(litdupsNS + "uri6");
    String qualifier = null;
    StatementMetadata metadata = null;

    @Before
    public void setUp() throws Exception {
        dao = new AccumuloRyaDAO();
        connector = new MockInstance().getConnector("root", new PasswordToken(""));
        connector.securityOperations().changeUserAuthorizations(connector.whoami(), new Authorizations(AUTHS));
        dao.setConnector(connector);
        conf = new AccumuloRdfConfiguration();
        dao.setConf(conf);
        conf.setAuths(AUTHS);
        conf.setFlush(true);
        dao.init();
    }

    @After
    public void tearDown() throws Exception {
        dao.purge(conf);
        dao.destroy();
    }

    @Test
    public void testAdd() throws Exception {
        RyaIRI cpu = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "cpu"));
        RyaIRI loadPerc = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "loadPerc"));
        RyaIRI uri1 = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "uri1"));
        dao.add(new RyaStatement(cpu, loadPerc, uri1));

        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), new RyaStatement(cpu, loadPerc, null), conf);
        int count = 0;
        while (iter.hasNext()) {
            assertEquals(uri1, iter.next().getObject());
            count++;
        }
        iter.close();
        assertEquals(1, count);

        dao.delete(new RyaStatement(cpu, loadPerc, null), conf);

        iter = RyaDAOHelper.query(dao.getQueryEngine(), new RyaStatement(cpu, loadPerc, null), conf);
        count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(0, count);
    }

    @Test
    public void testDeleteDiffVisibility() throws Exception {
        RyaIRI cpu = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "cpu"));
        RyaIRI loadPerc = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "loadPerc"));
        RyaIRI uri1 = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "uri1"));
        RyaStatement stmt1 = new RyaStatement(cpu, loadPerc, uri1, null, "1", new StatementMetadata(), "vis1".getBytes());
        dao.add(stmt1);
        RyaStatement stmt2 = new RyaStatement(cpu, loadPerc, uri1, null, "2", new StatementMetadata(), "vis2".getBytes());
        dao.add(stmt2);

        AccumuloRdfConfiguration cloneConf = conf.clone();
        cloneConf.setAuths("vis1", "vis2");

        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), new RyaStatement(cpu, loadPerc, null), cloneConf);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        assertEquals(2, count);

        dao.delete(stmt1, cloneConf);

        iter = RyaDAOHelper.query(dao.getQueryEngine(), new RyaStatement(cpu, loadPerc, null), cloneConf);
        count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        assertEquals(0, count);
    }

    @Test
    public void testDeleteDiffTimestamp() throws Exception {
        RyaIRI cpu = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "cpu"));
        RyaIRI loadPerc = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "loadPerc"));
        RyaIRI uri1 = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "uri1"));
        RyaStatement stmt1 = new RyaStatement(cpu, loadPerc, uri1, null, "1",
                (StatementMetadata) null, null, 100l);
        dao.add(stmt1);
        RyaStatement stmt2 = new RyaStatement(cpu, loadPerc, uri1, null, "2",
                (StatementMetadata) null, null, 100l);
        dao.add(stmt2);

        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(),
                new RyaStatement(cpu, loadPerc, null),
                dao.getQueryEngine().getConf());
        int resultSize = 0;
        while (iter.hasNext()) {
            resultSize++;
            iter.next();
        }
        iter.close();
        assertEquals(2, resultSize);

        final RyaStatement addStmt = new RyaStatement(cpu, loadPerc, uri1, null, "1",
                (StatementMetadata) null, null, 101l);
        dao.delete(stmt1, conf);
        dao.add(addStmt);
        dao.flush();

        iter = RyaDAOHelper.query(dao.getQueryEngine(),
                new RyaStatement(cpu, loadPerc, null),
                dao.getQueryEngine().getConf());
        resultSize = 0;
        while (iter.hasNext()) {
            resultSize++;
            iter.next();
        }
        iter.close();
        assertEquals(1, resultSize); //the delete marker should not delete the new stmt
    }

    @Test
    public void testDelete() throws Exception {
        RyaIRI predicate = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "pred"));
        RyaIRI subj = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "subj"));

        // create a "bulk load" of 10,000 statements
        int statement_count = 10000;
        for (int i = 0 ; i < statement_count ; i++){
            //make the RyaStatement very large so we will get a lot of random flushes
            RyaIRI obj = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, String.format("object%050d",i)));
            RyaStatement stmt = new RyaStatement(subj, predicate, obj);
            dao.add(stmt);
        }
        
        CloseableIteration<RyaStatement, RyaDAOException> iter;
        
        // check to see if all of the statements made it to the subj table
        // delete based on the data in the subj table
        RyaStatement subjQuery = new RyaStatement(subj, null, null);
        iter = RyaDAOHelper.query(dao.getQueryEngine(), subjQuery, conf);
        List<RyaStatement> stmts = new ArrayList<>();
        while (iter.hasNext()) {
            stmts.add(iter.next());
        }
        assertEquals(statement_count, stmts.size());
        dao.delete(stmts.iterator(), conf);

        // check statements in the predicate table
        RyaStatement predQuery = new RyaStatement(null, predicate, null);
        iter = RyaDAOHelper.query(dao.getQueryEngine(), predQuery, conf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
        }
        iter.close();
        assertEquals(0, count);
    }

    @Test
    public void testAddEmptyString() throws Exception {
        RyaIRI cpu = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "cpu"));
        RyaIRI loadPerc = RdfToRyaConversions.convertIRI(VF.createIRI(litdupsNS, "loadPerc"));
        RyaType empty = new RyaType("");
        dao.add(new RyaStatement(cpu, loadPerc, empty));

        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), new RyaStatement(cpu, loadPerc, null), conf);
        while (iter.hasNext()) {
            assertEquals("", iter.next().getObject().getData());
        }
        iter.close();
    }

    @Test
    public void testMaxResults() throws Exception {
        RyaIRI cpu = new RyaIRI(litdupsNS + "cpu");
        RyaIRI loadPerc = new RyaIRI(litdupsNS + "loadPerc");
        dao.add(new RyaStatement(cpu, loadPerc, new RyaIRI(litdupsNS + "uri1")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaIRI(litdupsNS + "uri2")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaIRI(litdupsNS + "uri3")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaIRI(litdupsNS + "uri4")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaIRI(litdupsNS + "uri5")));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();
        AccumuloRdfConfiguration queryConf = new AccumuloRdfConfiguration(conf);
        long limit = 3l;
        queryConf.setLimit(limit);

        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(queryEngine, new RyaStatement(cpu, loadPerc, null), queryConf);
        int count = 0;
        while (iter.hasNext()) {
            iter.next().getObject();
            count++;
        }
        iter.close();
        assertEquals(limit, count);
    }

    @Test
    public void testAddValue() throws Exception {
        RyaIRI cpu = new RyaIRI(litdupsNS + "cpu");
        RyaIRI loadPerc = new RyaIRI(litdupsNS + "loadPerc");
        RyaIRI uri1 = new RyaIRI(litdupsNS + "uri1");
        StatementMetadata myval = new StatementMetadata();
        myval.addMetadata(new RyaIRI("urn:example:name"), new RyaType("literal"));
        dao.add(new RyaStatement(cpu, loadPerc, uri1, null, null, myval));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(queryEngine, new RyaStatement(cpu, loadPerc, null), conf);
        assertTrue(iter.hasNext());
        assertEquals(myval.toString(), iter.next().getMetadata().toString());
        iter.close();
    }

    @Test
    public void testAddCv() throws Exception {
        RyaIRI cpu = new RyaIRI(litdupsNS + "cpu");
        RyaIRI loadPerc = new RyaIRI(litdupsNS + "loadPerc");
        RyaIRI uri1 = new RyaIRI(litdupsNS + "uri1");
        RyaIRI uri2 = new RyaIRI(litdupsNS + "uri2");
        RyaIRI uri3 = new RyaIRI(litdupsNS + "uri3");
        byte[] colVisABC = "A|B|C".getBytes();
        byte[] colVisAB = "A|B".getBytes();
        byte[] colVisA = "A".getBytes();
        dao.add(new RyaStatement(cpu, loadPerc, uri1, null, null, new StatementMetadata(), colVisABC));
        dao.add(new RyaStatement(cpu, loadPerc, uri2, null, null, new StatementMetadata(), colVisAB));
        dao.add(new RyaStatement(cpu, loadPerc, uri3, null, null, new StatementMetadata(), colVisA));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();

        //query with no auth
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(queryEngine, new RyaStatement(cpu, loadPerc, null), conf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        assertEquals(0, count);
        iter.close();

        AccumuloRdfConfiguration queryConf = new AccumuloRdfConfiguration();
        queryConf.setAuth("B");
        iter = RyaDAOHelper.query(queryEngine, new RyaStatement(cpu, loadPerc, null), queryConf);
        count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        assertEquals(2, count);

        queryConf.setAuth("A");
        iter = RyaDAOHelper.query(queryEngine, new RyaStatement(cpu, loadPerc, null), queryConf);
        count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        assertEquals(3, count);
    }

    @Test
    public void testTTL() throws Exception {
        RyaIRI cpu = new RyaIRI(litdupsNS + "cpu");
        RyaIRI loadPerc = new RyaIRI(litdupsNS + "loadPerc");
        long current = System.currentTimeMillis();
        dao.add(new RyaStatement(cpu, loadPerc, new RyaIRI(litdupsNS + "uri1"), null, null, (StatementMetadata) null, null, current));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaIRI(litdupsNS + "uri2"), null, null, (StatementMetadata) null, null, current - 1010l));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaIRI(litdupsNS + "uri3"), null, null, (StatementMetadata) null, null, current - 2010l));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaIRI(litdupsNS + "uri4"), null, null, (StatementMetadata) null, null, current - 3010l));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaIRI(litdupsNS + "uri5"), null, null, (StatementMetadata) null, null, current - 4010l));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();
        AccumuloRdfConfiguration queryConf = conf.clone();
        queryConf.setTtl(3000l);

        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(queryEngine, new RyaStatement(cpu, loadPerc, null), queryConf);
        int count = 0;
        while (iter.hasNext()) {
            iter.next().getObject();
            count++;
        }
        iter.close();
        assertEquals(3, count);

        queryConf.setStartTime(current - 3000l);
        iter = RyaDAOHelper.query(queryEngine, new RyaStatement(cpu, loadPerc, null), queryConf);
        count = 0;
        while (iter.hasNext()) {
            iter.next().getObject();
            count++;
        }
        iter.close();
        assertEquals(3, count);
    }

    @Test
    public void testGetNamespace() throws Exception {
        dao.addNamespace("ns", litdupsNS);
        assertEquals(litdupsNS, dao.getNamespace("ns"));
        dao.removeNamespace("ns");
        assertNull(dao.getNamespace("ns"));
    }

    @Test
    public void testQueryWithoutContext() throws Exception {
        testQuery(null);
    }

    @Test
    public void testQueryWithContext() throws Exception {
        RyaIRI metrics = new RyaIRI(litdupsNS + "metrics");
        testQuery(metrics);
    }

    public void setUpData(RyaIRI context) {
        dao.add(new RyaStatement(cpu, loadPerc, uri1, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(cpu, loadPerc, uri2, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(cpu, loadPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(cpu, loadPerc, uri4, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(cpu, loadPerc, uri5, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(cpu, loadPerc, uri6, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));

        dao.add(new RyaStatement(disk, diskPerc, uri1, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(disk, diskPerc, uri2, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(disk, diskPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(disk, diskPerc, uri4, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(disk, diskPerc, uri5, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(disk, diskPerc, uri6, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));

        dao.add(new RyaStatement(net, netPerc, uri1, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(net, netPerc, uri2, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(net, netPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(net, netPerc, uri4, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(net, netPerc, uri5, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        dao.add(new RyaStatement(net, netPerc, uri6, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
    }

    public void testQuery(RyaIRI context) {
        setUpData(context);

        // Use Scanner
        AccumuloRdfConfiguration queryConf = new AccumuloRdfConfiguration(conf);
        queryConf.setNumThreads(1);

        Collection<RyaStatement> coll = new ArrayList<>();
        coll.add(new RyaStatement(null, loadPerc, uri1, context));
        coll.add(new RyaStatement(null, loadPerc, uri2, context));
        coll.add(new RyaStatement(null, loadPerc, uri3, context));
        coll.add(new RyaStatement(null, loadPerc, uri4, context));
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), coll, conf);
        int count = 0;
        assertTrue(iter.hasNext()); //old code had a weird behaviour that could not perform hasNext consecutively
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(4, count);
    }

    @Test
    public void testQuerySPOC() {
        RyaIRI context = new RyaIRI(litdupsNS + "metrics");
        setUpData(context);

        Collection<RyaStatement> query = new ArrayList<>();
        query.add(new RyaStatement(disk, diskPerc, uri4, context));
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), query, conf);
        List<RyaStatement> actual = new ArrayList<>();
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        iter.close();
        List<RyaStatement> expected = new ArrayList<>();
        expected.add(new RyaStatement(disk, diskPerc, uri4, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        assertEquals(expected, actual);
    }

    @Test
    public void testQuerySPC() {
        RyaIRI context = new RyaIRI(litdupsNS + "metrics");
        setUpData(context);

        Collection<RyaStatement> query = new ArrayList<>();
        query.add(new RyaStatement(disk, diskPerc, null, context));
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), query, conf);
        List<RyaStatement> actual = new ArrayList<>();
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        iter.close();
        List<RyaStatement> expected = new ArrayList<>();
        expected.add(new RyaStatement(disk, diskPerc, uri1, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri2, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri4, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri5, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri6, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        assertEquals(expected, actual);
    }

    @Test
    public void testQuerySOC() {
        RyaIRI context = new RyaIRI(litdupsNS + "metrics");
        setUpData(context);

        Collection<RyaStatement> query = new ArrayList<>();
        query.add(new RyaStatement(disk, null, uri4, context));
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), query, conf);
        List<RyaStatement> actual = new ArrayList<>();
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        iter.close();
        List<RyaStatement> expected = new ArrayList<>();
        expected.add(new RyaStatement(disk, diskPerc, uri4, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        assertEquals(expected, actual);
    }

    @Test
    public void testQueryOPC() {
        RyaIRI context = new RyaIRI(litdupsNS + "metrics");
        setUpData(context);

        Collection<RyaStatement> query = new ArrayList<>();
        query.add(new RyaStatement(null, diskPerc, uri4, context));
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), query, conf);
        List<RyaStatement> actual = new ArrayList<>();
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        iter.close();
        List<RyaStatement> expected = new ArrayList<>();
        expected.add(new RyaStatement(disk, diskPerc, uri4, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        assertEquals(expected, actual);
    }

    @Test
    public void testQuerySC() {
        RyaIRI context = new RyaIRI(litdupsNS + "metrics");
        setUpData(context);

        Collection<RyaStatement> query = new ArrayList<>();
        query.add(new RyaStatement(disk, null, null, context));
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), query, conf);
        List<RyaStatement> actual = new ArrayList<>();
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        iter.close();
        List<RyaStatement> expected = new ArrayList<>();
        expected.add(new RyaStatement(disk, diskPerc, uri1, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri2, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri4, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri5, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri6, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        assertEquals(expected, actual);
    }

    @Test
    public void testQueryPC() {
        RyaIRI context = new RyaIRI(litdupsNS + "metrics");
        setUpData(context);

        Collection<RyaStatement> query = new ArrayList<>();
        query.add(new RyaStatement(null, diskPerc, null, context));
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), query, conf);
        List<RyaStatement> actual = new ArrayList<>();
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        iter.close();
        List<RyaStatement> expected = new ArrayList<>();
        expected.add(new RyaStatement(disk, diskPerc, uri1, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri2, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri4, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri5, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri6, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        assertEquals(expected, actual);
    }

    @Test
    public void testQueryOC() {
        RyaIRI context = new RyaIRI(litdupsNS + "metrics");
        setUpData(context);

        Collection<RyaStatement> query = new ArrayList<>();
        query.add(new RyaStatement(null, null, uri3, context));
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), query, conf);
        List<RyaStatement> actual = new ArrayList<>();
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        iter.close();
        List<RyaStatement> expected = new ArrayList<>();
        expected.add(new RyaStatement(cpu, loadPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(net, netPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        assertEquals(expected, actual);
    }

    @Test
    public void testQuerySPO() {
        RyaIRI context = null;
        setUpData(context);

        Collection<RyaStatement> query = new ArrayList<>();
        query.add(new RyaStatement(disk, diskPerc, uri3, context));
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), query, conf);
        List<RyaStatement> actual = new ArrayList<>();
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        iter.close();
        List<RyaStatement> expected = new ArrayList<>();
        expected.add(new RyaStatement(disk, diskPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        assertEquals(expected, actual);
    }

    @Test
    @Ignore("This doesn't work yet. Not sure yet if it needs to. It tests for multiple table layout support.")
    public void testQueryMultipleLayouts() {
        RyaIRI context = new RyaIRI(litdupsNS + "metrics");
        setUpData(context);

        Collection<RyaStatement> query = new ArrayList<>();
        query.add(new RyaStatement(cpu, loadPerc, null, context));
        query.add(new RyaStatement(null, diskPerc, null, context));
        query.add(new RyaStatement(null, null, uri3, context));
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), query, conf);
        Set<RyaStatement> actual = new HashSet<>();
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        iter.close();
        Set<RyaStatement> expected = new HashSet<>();
        expected.add(new RyaStatement(cpu, loadPerc, uri1, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(cpu, loadPerc, uri2, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(cpu, loadPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(cpu, loadPerc, uri4, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(cpu, loadPerc, uri5, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(cpu, loadPerc, uri6, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri6, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri1, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri2, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri4, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri5, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri6, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(disk, diskPerc, uri6, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        expected.add(new RyaStatement(net, netPerc, uri3, context, qualifier, metadata, AUTHS.getBytes(), TIMESTAMP));
        assertEquals(expected, actual);
    }

	@Test
	public void testQueryDates() throws Exception {
        RyaIRI cpu = new RyaIRI(litdupsNS + "cpu");
        RyaIRI loadPerc = new RyaIRI(litdupsNS + "loadPerc");
        RyaType uri0 = new RyaType(XMLSchema.DATETIME, "1960-01-01"); // How handles local time
        RyaType uri1 = new RyaType(XMLSchema.DATETIME, "1992-01-01T+10:00"); // See Magadan Time
        RyaType uri2 = new RyaType(XMLSchema.DATETIME, "2000-01-01TZ"); // How it handles UTC.
        RyaType uri3 = new RyaType(XMLSchema.DATETIME, "2000-01-01T00:00:01.111Z");
        RyaType uri4 = new RyaType(XMLSchema.DATETIME, "2000-01-01T00:00:01.111Z");  // duplicate
        RyaType uri5 = new RyaType(XMLSchema.DATETIME, "2000-01-01T00:00:01-00:00");
        RyaType uri6 = new RyaType(XMLSchema.DATETIME, "2000-01-01T00:00:01Z");  // duplicate
        RyaType uri7 = new RyaType(XMLSchema.DATETIME, "-2000-01-01T00:00:01Z");
        RyaType uri8 = new RyaType(XMLSchema.DATETIME, "111-01-01T00:00:01Z");
        RyaType uri9 = new RyaType(XMLSchema.DATETIME, "12345-01-01T00:00:01Z");
        long timestamp = 1592457339326L;

        dao.add(new RyaStatement(cpu, loadPerc, uri0, timestamp));
        dao.add(new RyaStatement(cpu, loadPerc, uri1, timestamp));
        dao.add(new RyaStatement(cpu, loadPerc, uri2, timestamp));
        dao.add(new RyaStatement(cpu, loadPerc, uri3, timestamp));
        dao.add(new RyaStatement(cpu, loadPerc, uri4, timestamp));
        dao.add(new RyaStatement(cpu, loadPerc, uri5, timestamp));
        dao.add(new RyaStatement(cpu, loadPerc, uri6, timestamp));
        dao.add(new RyaStatement(cpu, loadPerc, uri7, timestamp));
        dao.add(new RyaStatement(cpu, loadPerc, uri8, timestamp));
        dao.add(new RyaStatement(cpu, loadPerc, uri9, timestamp));

        {
            Collection<RyaStatement> coll = new ArrayList<>();
            coll.add(new RyaStatement(null, loadPerc, null));
            CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), coll, conf);

            int count = 0;
            while (iter.hasNext()) {
                count++;
                iter.next();
            }
            iter.close();
            assertEquals(
                    "There should be 8 unique items in the table.", // 2 duplicates
                    8, count);
        }

        {
            Collection<RyaStatement> coll = new ArrayList<>();
            coll.add(new RyaStatement(null, loadPerc, uri0));
            coll.add(new RyaStatement(null, loadPerc, uri1));
            coll.add(new RyaStatement(null, loadPerc, uri2));
            CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), coll, conf);

            Collection<RyaStatement> expected = new ArrayList<>();
            expected.add(new RyaStatement(cpu, loadPerc, new RyaType(XMLSchema.DATETIME, "2000-01-01T00:00:00.000Z"), timestamp));
            expected.add(new RyaStatement(cpu, loadPerc, new RyaType(XMLSchema.DATETIME, "1991-12-31T14:00:00.000Z"), timestamp));
            expected.add(new RyaStatement(cpu, loadPerc, new RyaType(XMLSchema.DATETIME, "1959-12-31T14:00:00.000Z"), timestamp));

            Collection<RyaStatement> actual = new ArrayList<>();
            while (iter.hasNext()) {
                actual.add(iter.next());
            }
            iter.close();
            assertEquals(
                    "Three time zones should be normalized when stored, then normalized same when queried.",
                    expected.size(), actual.size());
            // They will return in a random order
            for (RyaStatement e : expected) {
                assertTrue(e.toString(), actual.contains(e));
            }
        }

        {
            Collection<RyaStatement> coll = new ArrayList<>();
            coll.add(new RyaStatement(null, loadPerc, uri0));
            coll.add(new RyaStatement(null, loadPerc, uri1));
            coll.add(new RyaStatement(null, loadPerc, uri2));
            coll.add(new RyaStatement(null, loadPerc, uri3));
            coll.add(new RyaStatement(null, loadPerc, uri4));
            coll.add(new RyaStatement(null, loadPerc, uri5));
            coll.add(new RyaStatement(null, loadPerc, uri6));
            coll.add(new RyaStatement(null, loadPerc, uri7));
            coll.add(new RyaStatement(null, loadPerc, uri8));
            coll.add(new RyaStatement(null, loadPerc, uri9));
            CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), coll, conf);

            Collection<RyaStatement> expected = new ArrayList<>();
            expected.add(new RyaStatement(cpu, loadPerc, new RyaType(XMLSchema.DATETIME, "0111-01-01T00:00:01.000Z"), timestamp));
            expected.add(new RyaStatement(cpu, loadPerc, new RyaType(XMLSchema.DATETIME, "1959-12-31T14:00:00.000Z"), timestamp));
            expected.add(new RyaStatement(cpu, loadPerc, new RyaType(XMLSchema.DATETIME, "1991-12-31T14:00:00.000Z"), timestamp));
            expected.add(new RyaStatement(cpu, loadPerc, new RyaType(XMLSchema.DATETIME, "2000-01-01T00:00:00.000Z"), timestamp));
            expected.add(new RyaStatement(cpu, loadPerc, new RyaType(XMLSchema.DATETIME, "2000-01-01T00:00:01.000Z"), timestamp));
            expected.add(new RyaStatement(cpu, loadPerc, new RyaType(XMLSchema.DATETIME, "2000-01-01T00:00:01.000Z"), timestamp));
            expected.add(new RyaStatement(cpu, loadPerc, new RyaType(XMLSchema.DATETIME, "2000-01-01T00:00:01.111Z"), timestamp));
            expected.add(new RyaStatement(cpu, loadPerc, new RyaType(XMLSchema.DATETIME, "12345-01-01T00:00:01.000Z"), timestamp));
            // TODO: Is it correct to return "2000-01-01T00:00:01.000Z" multiple times?

            Collection<RyaStatement> actual = new ArrayList<>();
            while (iter.hasNext()) {
                RyaStatement s = iter.next();
                actual.add(s);
                System.out.println("Returned: " + s);
            }
            iter.close();
            assertEquals(
                    "Variety of time specs, including BC, pre-1970, duplicate pair ovewrite,future, 3 digit year.",
                    expected.size(), actual.size());
            // They will return in a random order
            for (RyaStatement e : expected) {
                assertTrue(e.toString(), actual.contains(e));
            }
            //assertEquals(new ArrayList<>(), actual); // For debugging
        }
    }

	@Test
    public void testQueryCollectionRegex() throws Exception {
        RyaIRI cpu = new RyaIRI(litdupsNS + "cpu");
        RyaIRI loadPerc = new RyaIRI(litdupsNS + "loadPerc");
        RyaIRI uri1 = new RyaIRI(litdupsNS + "uri1");
        RyaIRI uri2 = new RyaIRI(litdupsNS + "uri2");
        RyaIRI uri3 = new RyaIRI(litdupsNS + "uri3");
        RyaIRI uri4 = new RyaIRI(litdupsNS + "uri4");
        RyaIRI uri5 = new RyaIRI(litdupsNS + "uri5");
        RyaIRI uri6 = new RyaIRI(litdupsNS + "uri6");
        dao.add(new RyaStatement(cpu, loadPerc, uri1));
        dao.add(new RyaStatement(cpu, loadPerc, uri2));
        dao.add(new RyaStatement(cpu, loadPerc, uri3));
        dao.add(new RyaStatement(cpu, loadPerc, uri4));
        dao.add(new RyaStatement(cpu, loadPerc, uri5));
        dao.add(new RyaStatement(cpu, loadPerc, uri6));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();

        Collection<RyaStatement> coll = new ArrayList<>();
        coll.add(new RyaStatement(null, loadPerc, uri1));
        coll.add(new RyaStatement(null, loadPerc, uri2));
        conf.setRegexPredicate(loadPerc.getData());
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), coll, conf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(2, count);

        conf.setRegexPredicate("notLoadPerc");
        iter = RyaDAOHelper.query(dao.getQueryEngine(), coll, conf);
        count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(0, count);
    }

    @Test
    public void testQueryCollectionRegexWBatchScanner() throws Exception {
        RyaIRI cpu = new RyaIRI(litdupsNS + "cpu");
        RyaIRI loadPerc = new RyaIRI(litdupsNS + "loadPerc");
        RyaIRI uri1 = new RyaIRI(litdupsNS + "uri1");
        RyaIRI uri2 = new RyaIRI(litdupsNS + "uri2");
        RyaIRI uri3 = new RyaIRI(litdupsNS + "uri3");
        RyaIRI uri4 = new RyaIRI(litdupsNS + "uri4");
        RyaIRI uri5 = new RyaIRI(litdupsNS + "uri5");
        RyaIRI uri6 = new RyaIRI(litdupsNS + "uri6");
        dao.add(new RyaStatement(cpu, loadPerc, uri1));
        dao.add(new RyaStatement(cpu, loadPerc, uri2));
        dao.add(new RyaStatement(cpu, loadPerc, uri3));
        dao.add(new RyaStatement(cpu, loadPerc, uri4));
        dao.add(new RyaStatement(cpu, loadPerc, uri5));
        dao.add(new RyaStatement(cpu, loadPerc, uri6));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();
        AccumuloRdfConfiguration queryConf = new AccumuloRdfConfiguration(conf);
        queryConf.setNumThreads(1);

        Collection<RyaStatement> coll = new ArrayList<>();
        coll.add(new RyaStatement(null, loadPerc, uri1));
        coll.add(new RyaStatement(null, loadPerc, uri2));
        conf.setRegexPredicate(loadPerc.getData());
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), coll, queryConf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(2, count);

        queryConf.setRegexPredicate("notLoadPerc");
        iter = RyaDAOHelper.query(dao.getQueryEngine(), coll, queryConf);
        count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(0, count);
    }

    @Test
    public void testLiteralTypes() throws Exception {
        RyaIRI cpu = new RyaIRI(litdupsNS + "cpu");
        RyaIRI loadPerc = new RyaIRI(litdupsNS + "loadPerc");
        RyaType longLit = new RyaType(XMLSchema.LONG, "3");

        dao.add(new RyaStatement(cpu, loadPerc, longLit));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();

        CloseableIteration<RyaStatement, RyaDAOException> query = RyaDAOHelper.query(queryEngine, new RyaStatement(cpu, null, null), conf);
        assertTrue(query.hasNext());
        RyaStatement next = query.next();
        assertEquals(new Long(longLit.getData()), new Long(next.getObject().getData()));
        query.close();

        RyaType doubleLit = new RyaType(XMLSchema.DOUBLE, "2.0");

        dao.add(new RyaStatement(cpu, loadPerc, doubleLit));

        query = RyaDAOHelper.query(queryEngine, new RyaStatement(cpu, loadPerc, doubleLit), conf);
        assertTrue(query.hasNext());
        next = query.next();
        assertEquals(Double.parseDouble(doubleLit.getData()), Double.parseDouble(next.getObject().getData()), 0.001);
        query.close();
    }

    @Test
    public void testSameLiteralStringTypes() throws Exception {
        RyaIRI cpu = new RyaIRI(litdupsNS + "cpu");
        RyaIRI loadPerc = new RyaIRI(litdupsNS + "loadPerc");
        RyaType longLit = new RyaType(XMLSchema.LONG, "10");
        RyaType strLit = new RyaType(XMLSchema.STRING, new String(RyaContext.getInstance().serializeType(longLit)[0]));

        RyaStatement expected = new RyaStatement(cpu, loadPerc, longLit, TIMESTAMP);
        dao.add(expected);
        dao.add(new RyaStatement(cpu, loadPerc, strLit, TIMESTAMP));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();

        CloseableIteration<RyaStatement, RyaDAOException> query = RyaDAOHelper.query(queryEngine, new RyaStatement(cpu, loadPerc, longLit), conf);
        // Both Long and String representations match the search criteria.

        assertTrue(query.hasNext());
        RyaStatement next = query.next();
        assertEquals(strLit.getData(), next.getObject().getData());
        assertEquals(strLit.getDataType(), next.getObject().getDataType());

        assertTrue(query.hasNext());
        next = query.next();
        assertEquals(new Long(longLit.getData()), new Long(next.getObject().getData()));
        assertEquals(longLit.getDataType(), next.getObject().getDataType());

        assertFalse(query.hasNext());
        query.close();

        RyaType longLitDifferent = new RyaType(XMLSchema.LONG, "11");
        query = RyaDAOHelper.query(queryEngine, new RyaStatement(cpu, loadPerc, longLitDifferent), conf);
        assertFalse(query.hasNext());
        query.close();
    }

    @Test
    public void testPurge() throws RyaDAOException, TableNotFoundException {
        dao.add(newRyaStatement());
        assertFalse("table should not be empty", areTablesEmpty());

        dao.purge(conf);
        assertTrue("table should be empty", areTablesEmpty());
        //assertNotNull(dao.getVersion());
    }

    @Test
    public void testPurgeDoesNotBreakBatchWriters() throws TableNotFoundException, RyaDAOException {
        dao.purge(conf);
        assertTrue("table should be empty", areTablesEmpty());

        dao.add(newRyaStatement());
        assertFalse("table should not be empty", areTablesEmpty());
    }

    @Test
    public void testDropAndDestroy() throws RyaDAOException {
        assertTrue(dao.isInitialized());
        dao.dropAndDestroy();
        for (String tableName : dao.getTables()) {
            assertFalse(tableExists(tableName));
        }
        assertFalse(dao.isInitialized());
    }

    @Test
    public void testQueryWithIterators() throws Exception {
        RyaIRI cpu = new RyaIRI(litdupsNS + "cpu");
        RyaIRI loadPerc = new RyaIRI(litdupsNS + "loadPerc");
        RyaIRI uri1 = new RyaIRI(litdupsNS + "uri1");
        dao.add(new RyaStatement(cpu, loadPerc, uri1, null, "qual1"));
        dao.add(new RyaStatement(cpu, loadPerc, uri1, null, "qual2"));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();

        AccumuloRdfConfiguration queryConf = new AccumuloRdfConfiguration(conf);
        IteratorSetting firstEntryInRow = new IteratorSetting(3 /* correct value?? */, FirstEntryInRowIterator.class);
        queryConf.setAdditionalIterators(firstEntryInRow);

        Collection<RyaStatement> coll = new ArrayList<>();
        coll.add(new RyaStatement(null, loadPerc, uri1));
        CloseableIteration<RyaStatement, RyaDAOException> iter = RyaDAOHelper.query(dao.getQueryEngine(), coll, queryConf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(1, count);

        //Assert that without the iterator we get 2
        coll = new ArrayList<>();
        coll.add(new RyaStatement(null, loadPerc, uri1));
        iter = RyaDAOHelper.query(dao.getQueryEngine(), coll, conf);
        count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(2, count);

    }

    private boolean areTablesEmpty() throws TableNotFoundException {
        for (String table : dao.getTables()) {
            if (tableExists(table)) {
                // TODO: filter out version
                if (createScanner(table).iterator().hasNext()) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean tableExists(String tableName) {
        return dao.getConnector().tableOperations().exists(tableName);
    }

    private Scanner createScanner(String tableName) throws TableNotFoundException {
        return dao.getConnector().createScanner(tableName, conf.getAuthorizations());
    }

    private RyaStatement newRyaStatement() {
        RyaIRI subject = new RyaIRI(litdupsNS + randomString());
        RyaIRI predicate = new RyaIRI(litdupsNS + randomString());
        RyaType object = new RyaType(randomString());

        return new RyaStatement(subject, predicate, object);
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }
}
