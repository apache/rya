package mvm.rya.cloudbase;

import cloudbase.core.client.Connector;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.client.mock.MockInstance;
import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.cloudbase.query.CloudbaseRyaQueryEngine;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Class CloudbaseRyaDAOTest
 * Date: Mar 7, 2012
 * Time: 9:42:28 AM
 */
public class CloudbaseRyaDAOTest {

    private CloudbaseRyaDAO dao;
    private ValueFactory vf = new ValueFactoryImpl();
    static String litdupsNS = "urn:test:litdups#";
    private Connector connector;
    private CloudbaseRdfConfiguration conf = new CloudbaseRdfConfiguration();

    @Before
    public void setUp() throws Exception {
        dao = new CloudbaseRyaDAO();
        connector = new MockInstance().getConnector("", "");
        dao.setConnector(connector);
        dao.setConf(conf);
        dao.init();
    }

    @After
    public void tearDown() throws Exception {
        //dao.purge(conf);
        dao.destroy();
    }

    @Test
    public void testAdd() throws Exception {
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaURI uri1 = new RyaURI(litdupsNS + "uri1");
        dao.add(new RyaStatement(cpu, loadPerc, uri1));

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();

        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.query(new RyaStatement(cpu, loadPerc, null), conf);
        int count = 0;
        while (iter.hasNext()) {
            assertTrue(uri1.equals(iter.next().getObject()));
            count++;
        }
        iter.close();
        assertEquals(1, count);

        dao.delete(new RyaStatement(cpu, loadPerc, uri1), conf);

        iter = queryEngine.query(new RyaStatement(cpu, loadPerc, null), conf);
        count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(0, count);
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

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();
        CloudbaseRdfConfiguration queryConf = new CloudbaseRdfConfiguration(conf);
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
    public void testTTL() throws Exception {
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        long current = System.currentTimeMillis();
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri1"), null, null, null, null, current));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri2"), null, null, null, null, current - 1000l));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri3"), null, null, null, null, current - 2000l));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri4"), null, null, null, null, current - 3000l));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri5"), null, null, null, null, current - 4000l));

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();
        CloudbaseRdfConfiguration queryConf = new CloudbaseRdfConfiguration(conf);
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
    public void testPredRegex() throws Exception {
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaURI loadPerc2 = new RyaURI(litdupsNS + "loadPerc2");
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri1")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri2")));
        dao.add(new RyaStatement(cpu, loadPerc2, new RyaURI(litdupsNS + "uri3")));
        dao.add(new RyaStatement(cpu, loadPerc2, new RyaURI(litdupsNS + "uri4")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri5")));

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();
        CloudbaseRdfConfiguration queryConf = new CloudbaseRdfConfiguration(conf);
        queryConf.setRegexPredicate(litdupsNS + "loadPerc[2]");

        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.query(new RyaStatement(cpu, null, null), queryConf);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        assertEquals(2, count);
    }

    @Test
    public void testSubjectRegex() throws Exception {
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI cpu2 = new RyaURI(litdupsNS + "cpu2");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaURI loadPerc2 = new RyaURI(litdupsNS + "loadPerc2");
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri1")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri2")));
        dao.add(new RyaStatement(cpu, loadPerc2, new RyaURI(litdupsNS + "uri3")));
        dao.add(new RyaStatement(cpu, loadPerc2, new RyaURI(litdupsNS + "uri4")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri5")));
        dao.add(new RyaStatement(cpu2, loadPerc, new RyaURI(litdupsNS + "uri5")));

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();
        CloudbaseRdfConfiguration queryConf = new CloudbaseRdfConfiguration(conf);
        queryConf.setRegexSubject(cpu.getData());

        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.query(new RyaStatement(null, loadPerc, null), queryConf);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        assertEquals(3, count);
    }

    @Test
    public void testFullTableScan() throws Exception {
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI cpu2 = new RyaURI(litdupsNS + "cpu2");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaURI loadPerc2 = new RyaURI(litdupsNS + "loadPerc2");
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri1")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri2")));
        dao.add(new RyaStatement(cpu, loadPerc2, new RyaURI(litdupsNS + "uri3")));
        dao.add(new RyaStatement(cpu, loadPerc2, new RyaURI(litdupsNS + "uri4")));
        dao.add(new RyaStatement(cpu, loadPerc, new RyaURI(litdupsNS + "uri5")));
        dao.add(new RyaStatement(cpu2, loadPerc, new RyaURI(litdupsNS + "uri5")));

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();
        CloudbaseRdfConfiguration queryConf = new CloudbaseRdfConfiguration(conf);
        queryConf.setRegexSubject(cpu.getData());

        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.query(new RyaStatement(null, null, null), queryConf);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            count++;
        }
        iter.close();
        assertEquals(7, count); //includes the rts:version
    }

    @Test
    public void testAddValue() throws Exception {
        RyaURI cpu = new RyaURI(litdupsNS + "cpu");
        RyaURI loadPerc = new RyaURI(litdupsNS + "loadPerc");
        RyaURI uri1 = new RyaURI(litdupsNS + "uri1");
        String myval = "myval";
        dao.add(new RyaStatement(cpu, loadPerc, uri1, null, null, null, myval.getBytes()));

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();
        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.query(new RyaStatement(cpu, loadPerc, null), conf);
        assertTrue(iter.hasNext());
        assertEquals(myval, new String(iter.next().getValue()));
        iter.close();
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

        CloudbaseRdfConfiguration cloneConf = conf.clone();
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

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();

        //query with no auth
        CloseableIteration<RyaStatement, RyaDAOException> iter = queryEngine.query(new RyaStatement(cpu, loadPerc, null), conf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        assertEquals(0, count);
        iter.close();

        CloudbaseRdfConfiguration queryConf = new CloudbaseRdfConfiguration();
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

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();

        Collection<Map.Entry<RyaStatement, BindingSet>> coll = new ArrayList();
        coll.add(new RdfCloudTripleStoreUtils.CustomEntry(new RyaStatement(null, loadPerc, uri1), null));
        coll.add(new RdfCloudTripleStoreUtils.CustomEntry(new RyaStatement(null, loadPerc, uri2), null));
        CloseableIteration<? extends Map.Entry<RyaStatement, BindingSet>, RyaDAOException> iter = queryEngine.queryWithBindingSet(coll, conf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(2, count);

        //now use batchscanner
        CloudbaseRdfConfiguration queryConf = new CloudbaseRdfConfiguration(conf);
        queryConf.setMaxRangesForScanner(2);

        coll = new ArrayList();
        coll.add(new RdfCloudTripleStoreUtils.CustomEntry(new RyaStatement(null, loadPerc, uri1), null));
        coll.add(new RdfCloudTripleStoreUtils.CustomEntry(new RyaStatement(null, loadPerc, uri2), null));
        coll.add(new RdfCloudTripleStoreUtils.CustomEntry(new RyaStatement(null, loadPerc, uri3), null));
        coll.add(new RdfCloudTripleStoreUtils.CustomEntry(new RyaStatement(null, loadPerc, uri4), null));
        iter = queryEngine.queryWithBindingSet(coll, queryConf);
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

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();

        Collection<Map.Entry<RyaStatement, BindingSet>> coll = new ArrayList();
        coll.add(new RdfCloudTripleStoreUtils.CustomEntry(new RyaStatement(null, loadPerc, uri1), null));
        coll.add(new RdfCloudTripleStoreUtils.CustomEntry(new RyaStatement(null, loadPerc, uri2), null));
        conf.setRegexPredicate(loadPerc.getData());
        CloseableIteration<? extends Map.Entry<RyaStatement, BindingSet>, RyaDAOException> iter = queryEngine.queryWithBindingSet(coll, conf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(2, count);

        conf.setRegexPredicate("notLoadPerc");
        iter = queryEngine.queryWithBindingSet(coll, conf);
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

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();
        CloudbaseRdfConfiguration queryConf = new CloudbaseRdfConfiguration(conf);
        queryConf.setMaxRangesForScanner(1);

        Collection<Map.Entry<RyaStatement, BindingSet>> coll = new ArrayList();
        coll.add(new RdfCloudTripleStoreUtils.CustomEntry(new RyaStatement(null, loadPerc, uri1), null));
        coll.add(new RdfCloudTripleStoreUtils.CustomEntry(new RyaStatement(null, loadPerc, uri2), null));
        conf.setRegexPredicate(loadPerc.getData());
        CloseableIteration<? extends Map.Entry<RyaStatement, BindingSet>, RyaDAOException> iter = queryEngine.queryWithBindingSet(coll, queryConf);
        int count = 0;
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        iter.close();
        assertEquals(2, count);

        queryConf.setRegexPredicate("notLoadPerc");
        iter = queryEngine.queryWithBindingSet(coll, queryConf);
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

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();

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

        CloudbaseRyaQueryEngine queryEngine = dao.getQueryEngine();

        CloseableIteration<RyaStatement, RyaDAOException> query = queryEngine.query(new RyaStatement(cpu, loadPerc, longLit), conf);
        assertTrue(query.hasNext());
        RyaStatement next = query.next();
        assertEquals(new Long(longLit.getData()), new Long(next.getObject().getData()));
        assertEquals(longLit.getDataType(), next.getObject().getDataType());
        assertFalse(query.hasNext());
        query.close();
    }

    @Test
    @Ignore("Purge does not work with the batch deleter in mock cloudbase being null")
    public void testPurge() throws RyaDAOException, TableNotFoundException {
        dao.add(newRyaStatement());
        assertFalse("table should not be empty", areTablesEmpty());

        dao.purge(conf);
        assertTrue("table should be empty", areTablesEmpty());
        //assertNotNull(dao.getVersion());
    }

    @Test
    @Ignore("Purge does not work with the batch deleter in mock cloudbase being null")
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
        return connector.tableOperations().exists(tableName);
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
