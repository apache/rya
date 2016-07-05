package mvm.rya.accumulo;

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



import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import info.aduna.iteration.CloseableIteration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import mvm.rya.accumulo.query.AccumuloRyaQueryEngine;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.query.RyaQuery;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaContext;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.iterators.FirstEntryInRowIterator;
import org.calrissian.mango.collect.FluentCloseableIterable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;

/**
 * Class AccumuloRdfDAOTest
 * Date: Mar 7, 2012
 * Time: 9:42:28 AM
 */
public class AccumuloRyaDAOTest {

    private AccumuloRyaDAO dao;
    private ValueFactory vf = new ValueFactoryImpl();
    static String litdupsNS = "urn:test:litdups#";
    private AccumuloRdfConfiguration conf;
    private Connector connector;

    @Before
    public void setUp() throws Exception {
        dao = new AccumuloRyaDAO();
        connector = new MockInstance().getConnector("", "");
        dao.setConnector(connector);
        conf = new AccumuloRdfConfiguration();
        dao.setConf(conf);
        dao.init();
    }

    @After
    public void tearDown() throws Exception {
        dao.purge(conf);
        dao.destroy();
    }

    @Test
    public void testAdd() throws Exception {
        RyaURI cpu = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "cpu"));
        RyaURI loadPerc = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "loadPerc"));
        RyaURI uri1 = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "uri1"));
        dao.add(new RyaStatement(cpu, loadPerc, uri1));

        CloseableIteration<RyaStatement, RyaDAOException> iter = dao.getQueryEngine().query(new RyaStatement(cpu, loadPerc, null), conf);
        int count = 0;
        while (iter.hasNext()) {
            assertTrue(uri1.equals(iter.next().getObject()));
            count++;
        }
        iter.close();
        assertEquals(1, count);

        dao.delete(new RyaStatement(cpu, loadPerc, null), conf);

        iter = dao.getQueryEngine().query(new RyaStatement(cpu, loadPerc, null), conf);
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
        RyaURI cpu = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "cpu"));
        RyaURI loadPerc = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "loadPerc"));
        RyaURI uri1 = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "uri1"));
        RyaStatement stmt1 = new RyaStatement(cpu, loadPerc, uri1, null, "1", "vis1".getBytes());
        dao.add(stmt1);
        RyaStatement stmt2 = new RyaStatement(cpu, loadPerc, uri1, null, "2", "vis2".getBytes());
        dao.add(stmt2);

        AccumuloRdfConfiguration cloneConf = conf.clone();
        cloneConf.setAuth("vis1,vis2");

        CloseableIteration<RyaStatement, RyaDAOException> iter = dao.getQueryEngine().query(new RyaStatement(cpu, loadPerc, null), cloneConf);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        assertEquals(2, count);

        dao.delete(stmt1, cloneConf);

        iter = dao.getQueryEngine().query(new RyaStatement(cpu, loadPerc, null), cloneConf);
        count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        assertEquals(1, count);
    }

    @Test
    public void testDeleteDiffTimestamp() throws Exception {
        RyaURI cpu = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "cpu"));
        RyaURI loadPerc = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "loadPerc"));
        RyaURI uri1 = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "uri1"));
        RyaStatement stmt1 = new RyaStatement(cpu, loadPerc, uri1, null, "1", null, null, 100l);
        dao.add(stmt1);
        RyaStatement stmt2 = new RyaStatement(cpu, loadPerc, uri1, null, "2", null, null, 100l);
        dao.add(stmt2);

        int resultSize = FluentCloseableIterable.from(dao.getQueryEngine().query(
          RyaQuery.builder(new RyaStatement(cpu, loadPerc, null)).build())).autoClose().size();
        assertEquals(2, resultSize);

        final RyaStatement addStmt = new RyaStatement(cpu, loadPerc, uri1, null, "1",
                                                           null, null, 101l);
        dao.delete(stmt1, conf);
        dao.add(addStmt);

        resultSize = FluentCloseableIterable.from(dao.getQueryEngine().query(
          RyaQuery.builder(new RyaStatement(cpu, loadPerc, null)).build())).autoClose().size();
        assertEquals(2, resultSize); //the delete marker should not delete the new stmt
    }

    @Test
    public void testDelete() throws Exception {
        RyaURI predicate = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "pred"));
        RyaURI subj = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "subj"));

        // create a "bulk load" of 10,000 statements
        int statement_count = 10000;
        for (int i = 0 ; i < statement_count ; i++){
            //make the statement very large so we will get a lot of random flushes
            RyaURI obj = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, String.format("object%050d",i)));
            RyaStatement stmt = new RyaStatement(subj, predicate, obj);
            dao.add(stmt);
        }
        
        CloseableIteration<RyaStatement, RyaDAOException> iter;
        
        //check to see if all of the statements made it to the subj table
        //delete based on the data in the subj table
        RyaStatement subjQuery = new RyaStatement(subj, null, null);
        iter = dao.getQueryEngine().query(subjQuery, conf);
        List<RyaStatement> stmts = new ArrayList<RyaStatement>();
        while (iter.hasNext()) {
            stmts.add(iter.next());
        }
        assertEquals(statement_count, stmts.size());
        dao.delete(stmts.iterator(), conf);

        // check statements in the predicate table
        RyaStatement predQuery = new RyaStatement(null, predicate, null);
        iter = dao.getQueryEngine().query(predQuery, conf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
        }
        iter.close();
        assertEquals(0, count);
    }

    @Test
    public void testAddEmptyString() throws Exception {
        RyaURI cpu = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "cpu"));
        RyaURI loadPerc = RdfToRyaConversions.convertURI(vf.createURI(litdupsNS, "loadPerc"));
        RyaType empty = new RyaType("");
        dao.add(new RyaStatement(cpu, loadPerc, empty));

        CloseableIteration<RyaStatement, RyaDAOException> iter = dao.getQueryEngine().query(new RyaStatement(cpu, loadPerc, null), conf);
        while (iter.hasNext()) {
            assertEquals("", iter.next().getObject().getData());
        }
        iter.close();
    }

    @Test
    public void testMaxResults() throws Exception {
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri1")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri2")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri3")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri4")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri5")));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();
        AccumuloRdfConfiguration queryConf = new AccumuloRdfConfiguration(conf);
        long limit = 3l;
        queryConf.setLimit(limit);

        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.query(new RyaStatement(cpu, loadPerc, null), queryConf);
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
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaURI uri1 = new RyaURI(litdupsNS + "uri1");
        String myval = "myval";
        dao.add(new RyaStatement(cpu, loadPerc, uri1, null, null, null, myval.getBytes()));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();
        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.query(new RyaStatement(cpu, loadPerc, null), conf);
        assertTrue(iter.hasNext());
        assertEquals(myval, new String(iter.next().getValue()));
        iter.close();
    }

    @Test
    public void testAddCv() throws Exception {
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaURI uri1 = new RyaURI(litdupsNS + "uri1");
        RyaURI uri2 = new RyaURI(litdupsNS + "uri2");
        RyaURI uri3 = new RyaURI(litdupsNS + "uri3");
        byte[] colVisABC = "A|B|C".getBytes();
        byte[] colVisAB = "A|B".getBytes();
        byte[] colVisA = "A".getBytes();
        dao.add(new RyaStatement(cpu, loadPerc, uri1, null, null, colVisABC));
        dao.add(new RyaStatement(cpu, loadPerc, uri2, null, null, colVisAB));
        dao.add(new RyaStatement(cpu, loadPerc, uri3, null, null, colVisA));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();

        //query with no auth
        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.query(new RyaStatement(cpu, loadPerc, null), conf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        assertEquals(0, count);
        iter.close();

        AccumuloRdfConfiguration queryConf = new AccumuloRdfConfiguration();
        queryConf.setAuth("B");
        iter = queryEngine.query(new RyaStatement(cpu, loadPerc, null), queryConf);
        count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        assertEquals(2, count);

        queryConf.setAuth("A");
        iter = queryEngine.query(new RyaStatement(cpu, loadPerc, null), queryConf);
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
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        long current = System.currentTimeMillis();
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri1"), null, null, null, null, current));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri2"), null, null, null, null, current - 1010l));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri3"), null, null, null, null, current - 2010l));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri4"), null, null, null, null, current - 3010l));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri5"), null, null, null, null, current - 4010l));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();
        AccumuloRdfConfiguration queryConf = conf.clone();
        queryConf.setTtl(3000l);

        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.query(new RyaStatement(cpu, loadPerc, null), queryConf);
        int count = 0;
        while (iter.hasNext()) {
            iter.next().getObject();
            count++;
        }
        iter.close();
        assertEquals(3, count);

        queryConf.setStartTime(current - 3000l);
        iter = queryEngine.query(new RyaStatement(cpu, loadPerc, null), queryConf);
        count = 0;
        while (iter.hasNext()) {
            iter.next().getObject();
            count++;
        }
        iter.close();
        assertEquals(2, count);
    }

    @Test
    public void testGetNamespace() throws Exception {
        dao.addNamespace("ns", litdupsNS);
        assertEquals(litdupsNS, dao.getNamespace("ns"));
        dao.removeNamespace("ns");
        assertNull(dao.getNamespace("ns"));
    }

    //TOOD: Add test for set of queries
    @Test
    public void testQuery() throws Exception {
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaURI uri1 = new RyaURI(litdupsNS + "uri1");
        RyaURI uri2 = new RyaURI(litdupsNS + "uri2");
        RyaURI uri3 = new RyaURI(litdupsNS + "uri3");
        RyaURI uri4 = new RyaURI(litdupsNS + "uri4");
        RyaURI uri5 = new RyaURI(litdupsNS + "uri5");
        RyaURI uri6 = new RyaURI(litdupsNS + "uri6");
        dao.add(new RyaStatement(cpu, loadPerc, uri1));
        dao.add(new RyaStatement(cpu, loadPerc, uri2));
        dao.add(new RyaStatement(cpu, loadPerc, uri3));
        dao.add(new RyaStatement(cpu, loadPerc, uri4));
        dao.add(new RyaStatement(cpu, loadPerc, uri5));
        dao.add(new RyaStatement(cpu, loadPerc, uri6));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();

        Collection<RyaStatement> coll = new ArrayList();
        coll.add(new RyaStatement(null, loadPerc, uri1));
        coll.add(new RyaStatement(null, loadPerc, uri2));
        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.batchQuery(coll, conf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(2, count);

        //now use batchscanner
        AccumuloRdfConfiguration queryConf = new AccumuloRdfConfiguration(conf);
        queryConf.setMaxRangesForScanner(2);

        coll = new ArrayList();
        coll.add(new RyaStatement(null, loadPerc, uri1));
        coll.add(new RyaStatement(null, loadPerc, uri2));
        coll.add(new RyaStatement(null, loadPerc, uri3));
        coll.add(new RyaStatement(null, loadPerc, uri4));
        iter = queryEngine.batchQuery(coll, queryConf);
        assertTrue(iter.hasNext()); //old code had a weird behaviour that could not perform hasNext consecutively
        assertTrue(iter.hasNext());
        assertTrue(iter.hasNext());
        count = 0;
        while (iter.hasNext()) {
            count++;
            assertTrue(iter.hasNext());
            iter.next();
        }
        iter.close();
        assertEquals(4, count);
    }

	@Test
	public void testQueryDates() throws Exception {
	    RyaURI cpu = new RyaURI(litdupsNS + "cpu");
	    RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
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

	    dao.add(new RyaStatement(cpu, loadPerc, uri0));
	    dao.add(new RyaStatement(cpu, loadPerc, uri1));
	    dao.add(new RyaStatement(cpu, loadPerc, uri2));
	    dao.add(new RyaStatement(cpu, loadPerc, uri3));
	    dao.add(new RyaStatement(cpu, loadPerc, uri4));
	    dao.add(new RyaStatement(cpu, loadPerc, uri5));
	    dao.add(new RyaStatement(cpu, loadPerc, uri6));
	    dao.add(new RyaStatement(cpu, loadPerc, uri7));
	    dao.add(new RyaStatement(cpu, loadPerc, uri8));
	    dao.add(new RyaStatement(cpu, loadPerc, uri9));
	
	    AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();
	
	    Collection<RyaStatement> coll = new ArrayList();
	    coll.add(new RyaStatement(null, loadPerc, uri0));
	    coll.add(new RyaStatement(null, loadPerc, uri1));
	    coll.add(new RyaStatement(null, loadPerc, uri2));
	    CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.batchQuery(coll, conf);
	    int count = 0;
	    while (iter.hasNext()) {
	        count++;
	        iter.next();
	    }
	    iter.close();
	    assertEquals("Three time zones should be normalized when stored, then normalized same when queried.",3, count);
	
	    //now use batchscanner
	    AccumuloRdfConfiguration queryConf = new AccumuloRdfConfiguration(conf);
	    queryConf.setMaxRangesForScanner(2);
	
	    coll = new ArrayList();
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
	    iter = queryEngine.batchQuery(coll, queryConf);
	    count = 0;
	    while (iter.hasNext()) {
	        count++;
	        iter.next();
	    }
	    iter.close();
	    assertEquals("Variety of time specs, including BC, pre-1970, duplicate pair ovewrite,future, 3 digit year.",8, count);
	}

	@Test
    public void testQueryCollectionRegex() throws Exception {
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaURI uri1 = new RyaURI(litdupsNS + "uri1");
        RyaURI uri2 = new RyaURI(litdupsNS + "uri2");
        RyaURI uri3 = new RyaURI(litdupsNS + "uri3");
        RyaURI uri4 = new RyaURI(litdupsNS + "uri4");
        RyaURI uri5 = new RyaURI(litdupsNS + "uri5");
        RyaURI uri6 = new RyaURI(litdupsNS + "uri6");
        dao.add(new RyaStatement(cpu, loadPerc, uri1));
        dao.add(new RyaStatement(cpu, loadPerc, uri2));
        dao.add(new RyaStatement(cpu, loadPerc, uri3));
        dao.add(new RyaStatement(cpu, loadPerc, uri4));
        dao.add(new RyaStatement(cpu, loadPerc, uri5));
        dao.add(new RyaStatement(cpu, loadPerc, uri6));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();

        Collection<RyaStatement> coll = new ArrayList();
        coll.add(new RyaStatement(null, loadPerc, uri1));
        coll.add(new RyaStatement(null, loadPerc, uri2));
        conf.setRegexPredicate(loadPerc.getData());
        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.batchQuery(coll, conf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(2, count);

        conf.setRegexPredicate("notLoadPerc");
        iter = queryEngine.batchQuery(coll, conf);
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
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaURI uri1 = new RyaURI(litdupsNS + "uri1");
        RyaURI uri2 = new RyaURI(litdupsNS + "uri2");
        RyaURI uri3 = new RyaURI(litdupsNS + "uri3");
        RyaURI uri4 = new RyaURI(litdupsNS + "uri4");
        RyaURI uri5 = new RyaURI(litdupsNS + "uri5");
        RyaURI uri6 = new RyaURI(litdupsNS + "uri6");
        dao.add(new RyaStatement(cpu, loadPerc, uri1));
        dao.add(new RyaStatement(cpu, loadPerc, uri2));
        dao.add(new RyaStatement(cpu, loadPerc, uri3));
        dao.add(new RyaStatement(cpu, loadPerc, uri4));
        dao.add(new RyaStatement(cpu, loadPerc, uri5));
        dao.add(new RyaStatement(cpu, loadPerc, uri6));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();
        AccumuloRdfConfiguration queryConf = new AccumuloRdfConfiguration(conf);
        queryConf.setMaxRangesForScanner(1);

        Collection<RyaStatement> coll = new ArrayList();
        coll.add(new RyaStatement(null, loadPerc, uri1));
        coll.add(new RyaStatement(null, loadPerc, uri2));
        conf.setRegexPredicate(loadPerc.getData());
        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.batchQuery(coll, queryConf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(2, count);

        queryConf.setRegexPredicate("notLoadPerc");
        iter = queryEngine.batchQuery(coll, queryConf);
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
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaType longLit = new RyaType(XMLSchema.LONG, "3");

        dao.add(new RyaStatement(cpu, loadPerc, longLit));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();

        CloseableIteration<RyaStatement, RyaDAOException> query = queryEngine.query(new RyaStatement(cpu, null, null), conf);
        assertTrue(query.hasNext());
        RyaStatement next = query.next();
        assertEquals(new Long(longLit.getData()), new Long(next.getObject().getData()));
        query.close();

        RyaType doubleLit = new RyaType(XMLSchema.DOUBLE, "2.0");

        dao.add(new RyaStatement(cpu, loadPerc, doubleLit));

        query = queryEngine.query(new RyaStatement(cpu, loadPerc, doubleLit), conf);
        assertTrue(query.hasNext());
        next = query.next();
        assertEquals(Double.parseDouble(doubleLit.getData()), Double.parseDouble(next.getObject().getData()), 0.001);
        query.close();
    }

    @Test
    public void testSameLiteralStringTypes() throws Exception {
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaType longLit = new RyaType(XMLSchema.LONG, "10");
        RyaType strLit = new RyaType(XMLSchema.STRING, new String(RyaContext.getInstance().serializeType(longLit)[0]));

        RyaStatement expected = new RyaStatement(cpu, loadPerc, longLit);
        dao.add(expected);
        dao.add(new RyaStatement(cpu, loadPerc, strLit));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();

        CloseableIteration<RyaStatement, RyaDAOException> query = queryEngine.query(new RyaStatement(cpu, loadPerc, longLit), conf);
        assertTrue(query.hasNext());
        RyaStatement next = query.next();
        assertEquals(new Long(longLit.getData()), new Long(next.getObject().getData()));
        assertEquals(longLit.getDataType(), next.getObject().getDataType());
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
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaURI uri1 = new RyaURI(litdupsNS + "uri1");
        dao.add(new RyaStatement(cpu, loadPerc, uri1, null, "qual1"));
        dao.add(new RyaStatement(cpu, loadPerc, uri1, null, "qual2"));

        AccumuloRyaQueryEngine queryEngine = dao.getQueryEngine();

        AccumuloRdfConfiguration queryConf = new AccumuloRdfConfiguration(conf);
        IteratorSetting firstEntryInRow = new IteratorSetting(3 /* correct value?? */, FirstEntryInRowIterator.class);
        queryConf.setAdditionalIterators(firstEntryInRow);

        Collection<RyaStatement> coll = new ArrayList<>();
        coll.add(new RyaStatement(null, loadPerc, uri1));
        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.batchQuery(coll, queryConf);
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
        iter = queryEngine.batchQuery(coll, conf);
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
        RyaURI subject = new RyaURI(litdupsNS + randomString());
        RyaURI predicate = new RyaURI(litdupsNS + randomString());
        RyaType object = new RyaType(randomString());

        return new RyaStatement(subject, predicate, object);
    }

    private String randomString() {
        return UUID.randomUUID().toString();
    }
}
