package org.apache.rya.indexing.accumulo.temporal;

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


import static org.apache.rya.api.resolver.RdfToRyaConversions.convertStatement;
import static org.junit.Assert.*; 
import org.junit.Assert; 
import java.io.IOException;
import java.io.PrintStream;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryEvaluationException;

import com.beust.jcommander.internal.Lists;

import info.aduna.iteration.CloseableIteration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.StatementSerializer;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.TemporalInterval;
import org.apache.rya.indexing.accumulo.ConfigUtils;

/**
 * JUnit tests for TemporalIndexer and it's implementation AccumuloTemporalIndexer
 *
 * If you enjoy this test, please read RyaTemporalIndexerTest and YagoKBTest, which contain
 * many example SPARQL queries and updates and attempts to test independently of Accumulo:
 *
 *     extras/indexingSail/src/test/java/org.apache/rya/indexing/accumulo/RyaTemporalIndexerTest.java
 *     {@link org.apache.rya.indexing.accumulo.RyaTemporalIndexerTest}
 *     {@link org.apache.rya.indexing.accumulo.YagoKBTest.java}
 *
 * Remember, this class in instantiated fresh for each @test method.
 * so fields are reset, unless they are static.
 *
 * These are covered:
 *   Instance {before, equals, after} given Instance
 *   Instance {before, after, inside} given Interval
 *   Instance {hasBeginning, hasEnd} given Interval
 * And a few more.
 *
 *  The temporal predicates are from these ontologies: 
 *      http://linkedevents.org/ontology
 *      http://motools.sourceforge.net/event/event.html
 */
public final class AccumuloTemporalIndexerTest {
    // Configuration properties, this is reset per test in setup.
    Configuration conf;
    // temporal indexer to test, this is created for each test method by setup.
    AccumuloTemporalIndexer tIndexer;

    private static final String URI_PROPERTY_EVENT_TIME = "Property:event:time";
    private static final String URI_PROPERTY_CIRCA = "Property:circa";
    private static final String URI_PROPERTY_AT_TIME = "Property:atTime";
    private static final String STAT_COUNT = "count";
    private static final String STAT_KEYHASH = "keyhash";
    private static final String STAT_VALUEHASH = "valuehash";
    private static final StatementConstraints EMPTY_CONSTRAINTS = new StatementConstraints();

    // Assign this in setUpBeforeClass, store them in each test.
    // setup() deletes table before each test.
    static final Statement spo_B00_E01;
    static final Statement spo_B03_E20;
    static final Statement spo_B02_E29;
    static final Statement spo_B02_E30;
    static final Statement spo_B02_E40;
    static final Statement spo_B02_E31;
    static final Statement spo_B29_E30;
    static final Statement spo_B30_E32;

    // Instants:
    static final Statement spo_B02;
    static final int SERIES_OF_SECONDS = 41;
    static final Statement seriesSpo[] = new Statement[SERIES_OF_SECONDS];

    // These are shared for several tests. Only the seconds are different.
    // tvB03_E20 read as: interval Begins 3 seconds, ends at 20 seconds
    static final TemporalInterval tvB00_E01 = new TemporalInterval(//
            makeInstant(00), //
            makeInstant(01));
    static final TemporalInterval tvB29_E30= new TemporalInterval(//
            makeInstant(29), //
            makeInstant(30));
    static final TemporalInterval tvB30_E32= new TemporalInterval(//
            makeInstant(30), //
            makeInstant(32));
    static final TemporalInterval tvB03_E20 = new TemporalInterval(//
            makeInstant(03), //
            makeInstant(20));
    // 30 seconds, Begins earlier, ends later
    static final TemporalInterval tvB02_E30= new TemporalInterval(//
            makeInstant(02), //
            makeInstant(30));
    // use for interval after
    static final TemporalInterval tvB02_E29= new TemporalInterval(//
            makeInstant(02), //
            makeInstant(29));
    // same as above, but ends in the middle
    static final TemporalInterval tvB02_E31 = new TemporalInterval(//
            makeInstant(02), //
            makeInstant(31));
    // same as above, but ends even later
    static final TemporalInterval tvB02_E40 = new TemporalInterval(//
            makeInstant(02), //
            makeInstant(40));
    // instant, match beginnings of several above, before tiB03_E20
    static final TemporalInstant tsB02 = makeInstant(02);
    // instant, after all above
    static final TemporalInstant tsB04 = makeInstant(04);

    // Create a series of instants about times 0 - 40 seconds
    static final TemporalInstant seriesTs[];
    static {
        seriesTs = new TemporalInstant[SERIES_OF_SECONDS];
        for (int i = 0; i <= 40; i++)
            seriesTs[i] = makeInstant(i);
    };

    /**
     * Make an uniform instant with given seconds.
     */
    static TemporalInstant makeInstant(int secondsMakeMeUnique) {
        return new TemporalInstantRfc3339(2015, 12, 30, 12, 00, secondsMakeMeUnique);
    }

    static {
        // Setup the statements only once. Each test will store some of these in there own index table.
        ValueFactory vf = new ValueFactoryImpl();
        URI pred1_atTime = vf.createURI(URI_PROPERTY_AT_TIME);
        // tiB03_E20 read as: time interval that Begins 3 seconds, ends at 20 seconds,
        // Each time element the same, except seconds. year, month, .... minute are the same for each statement below.
        spo_B00_E01 = new StatementImpl(vf.createURI("foo:event0"), pred1_atTime, vf.createLiteral(tvB00_E01.toString()));
        spo_B02_E29 = new StatementImpl(vf.createURI("foo:event2"), pred1_atTime, vf.createLiteral(tvB02_E29.toString()));
        spo_B02_E30 = new StatementImpl(vf.createURI("foo:event2"), pred1_atTime, vf.createLiteral(tvB02_E30.toString()));
        spo_B02_E31 = new StatementImpl(vf.createURI("foo:event3"), pred1_atTime, vf.createLiteral(tvB02_E31.toString()));
        spo_B02_E40 = new StatementImpl(vf.createURI("foo:event4"), pred1_atTime, vf.createLiteral(tvB02_E40.toString()));
        spo_B03_E20 = new StatementImpl(vf.createURI("foo:event5"), pred1_atTime, vf.createLiteral(tvB03_E20.toString()));
        spo_B29_E30 = new StatementImpl(vf.createURI("foo:event1"), pred1_atTime, vf.createLiteral(tvB29_E30.toString()));
        spo_B30_E32 = new StatementImpl(vf.createURI("foo:event1"), pred1_atTime, vf.createLiteral(tvB30_E32.toString()));
        spo_B02 = new StatementImpl(vf.createURI("foo:event6"), pred1_atTime, vf.createLiteral(tsB02.getAsReadable()));

        // Create statements about time instants 0 - 40 seconds
        for (int i = 0; i < seriesTs.length; i++) {
            seriesSpo[i] = new StatementImpl(vf.createURI("foo:event0" + i), pred1_atTime, vf.createLiteral(seriesTs[i].getAsReadable()));
        }

    }

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        conf = new Configuration();
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "triplestore_");
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        // The temporal predicates are from http://linkedevents.org/ontology
        // and http://motools.sourceforge.net/event/event.html
        conf.setStrings(ConfigUtils.TEMPORAL_PREDICATES_LIST, ""
                + URI_PROPERTY_AT_TIME + ","
                + URI_PROPERTY_CIRCA + ","
                + URI_PROPERTY_EVENT_TIME);

        tIndexer = new AccumuloTemporalIndexer();
        tIndexer.setConf(conf);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    	String indexTableName = tIndexer.getTableName();
        tIndexer.close();
        TableOperations tableOps = ConfigUtils.getConnector(conf).tableOperations();

        if (tableOps.exists(indexTableName))
            tableOps.delete(indexTableName);
    }

    /**
     * Test method for {@link AccumuloTemporalIndexer#TemporalIndexerImpl(org.apache.hadoop.conf.Configuration)} .
     *
     * @throws TableExistsException
     * @throws TableNotFoundException
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws IOException
     */
    @Test
    public void testTemporalIndexerImpl()
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException, IOException {
        assertNotNull("Constructed.", tIndexer.toString());
    }

    /**
     * Test method for {@link AccumuloTemporalIndexer#storeStatement(convertStatement(org.openrdf.model.Statement)}
     *
     * @throws NoSuchAlgorithmException
     */
    @Test
    public void testStoreStatement() throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException, NoSuchAlgorithmException {
        // count rows expected to store:
        int rowsStoredExpected = 0;

        ValueFactory vf = new ValueFactoryImpl();

        URI pred1_atTime = vf.createURI(URI_PROPERTY_AT_TIME);
        URI pred2_circa = vf.createURI(URI_PROPERTY_CIRCA);

        // Should not be stored because they are not in the predicate list
        String validDateStringWithThirteens = "1313-12-13T13:13:13Z";
        tIndexer.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj1"), RDFS.LABEL, vf.createLiteral(validDateStringWithThirteens))));

        // Test: Should not store an improper date, and log a warning (log warning not tested).
        final String invalidDateString = "ThisIsAnInvalidDate";
//        // Silently logs a warning for bad dates.  Old: Set true when we catch the error:
//        boolean catchErrorThrownCorrectly = false;
//        try {
            tIndexer.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj2"), pred1_atTime, vf.createLiteral(invalidDateString))));
//        } catch (IllegalArgumentException e) {
//            catchErrorThrownCorrectly = true;
//            Assert.assertTrue(
//                    "Invalid date parse error should include the invalid string. message=" + e.getMessage(),
//                    e.getMessage().contains(invalidDateString));
//        }
//        Assert.assertTrue("Invalid date parse error should be thrown for this bad date=" + invalidDateString, catchErrorThrownCorrectly);

        // These are different datetimes instant but from different time zones.
        // This is an arbitrary zone, BRST=Brazil, better if not local.
        // same as "2015-01-01T01:59:59Z"
        final String testDate2014InBRST = "2014-12-31T23:59:59-02:00";
        // next year, same as "2017-01-01T01:59:59Z"
        final String testDate2016InET = "2016-12-31T20:59:59-05:00";

        // These should be stored because they are in the predicate list.
        // BUT they will get converted to the same exact datetime in UTC.
        Statement s3 = new StatementImpl(vf.createURI("foo:subj3"), pred1_atTime, vf.createLiteral(testDate2014InBRST));
        Statement s4 = new StatementImpl(vf.createURI("foo:subj4"), pred2_circa, vf.createLiteral(testDate2016InET));
        tIndexer.storeStatement(convertStatement(s3));
        rowsStoredExpected++;
        tIndexer.storeStatement(convertStatement(s4));
        rowsStoredExpected++;

        // This should not be stored because the object is not a literal
        tIndexer.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj5"), pred1_atTime, vf.createURI("in:valid"))));

        tIndexer.flush();

        int rowsStoredActual = printTables("junit testing: Temporal entities stored in testStoreStatement", null, null);
        assertEquals("Number of rows stored.", rowsStoredExpected*4, rowsStoredActual); // 4 index entries per statement

    }

    @Test
    public void testDelete() throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException, NoSuchAlgorithmException {
        // count rows expected to store:
        int rowsStoredExpected = 0;

        ValueFactory vf = new ValueFactoryImpl();

        URI pred1_atTime = vf.createURI(URI_PROPERTY_AT_TIME);
        URI pred2_circa = vf.createURI(URI_PROPERTY_CIRCA);

        final String testDate2014InBRST = "2014-12-31T23:59:59-02:00";
        final String testDate2016InET = "2016-12-31T20:59:59-05:00";

        // These should be stored because they are in the predicate list.
        // BUT they will get converted to the same exact datetime in UTC.
        Statement s1 = new StatementImpl(vf.createURI("foo:subj3"), pred1_atTime, vf.createLiteral(testDate2014InBRST));
        Statement s2 = new StatementImpl(vf.createURI("foo:subj4"), pred2_circa, vf.createLiteral(testDate2016InET));
        tIndexer.storeStatement(convertStatement(s1));
        rowsStoredExpected++;
        tIndexer.storeStatement(convertStatement(s2));
        rowsStoredExpected++;

        tIndexer.flush();

        int rowsStoredActual = printTables("junit testing: Temporal entities stored in testDelete before delete", System.out, null);
        Assert.assertEquals("Number of rows stored.", rowsStoredExpected*4, rowsStoredActual); // 4 index entries per statement

        tIndexer.deleteStatement(convertStatement(s1));
        tIndexer.deleteStatement(convertStatement(s2));

        int afterDeleteRowsStoredActual = printTables("junit testing: Temporal entities stored in testDelete after delete", System.out, null);
        Assert.assertEquals("Number of rows stored after delete.", 0, afterDeleteRowsStoredActual);
    }

    @Test
    public void testStoreStatementWithInterestingLiterals() throws Exception {
        ValueFactory vf = new ValueFactoryImpl();

        URI pred1_atTime = vf.createURI(URI_PROPERTY_AT_TIME);

        tIndexer.storeStatement(convertStatement(new StatementImpl(
                vf.createURI("foo:subj2"),
                pred1_atTime,
                vf.createLiteral("A number of organizations located, gathered, or classed together. [Derived from Concise Oxford English Dictionary, 11th Edition, 2008]"))));

        int rowsStoredActual = printTables("junit testing: Temporal entities stored in testStoreStatement", null, null);
        Assert.assertEquals("Number of rows stored.", 0, rowsStoredActual); // 4 index entries per statement
    }

    /**
     * Test method for {@link AccumuloTemporalIndexer#storeStatement(convertStatement(org.openrdf.model.Statement)}
     *
     * @throws NoSuchAlgorithmException
     */
    @Test
    public void testStoreStatementBadInterval() throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException, NoSuchAlgorithmException {
        // count rows expected to store:
        int rowsStoredExpected = 0;

        ValueFactory vf = new ValueFactoryImpl();
        URI pred1_atTime = vf.createURI(URI_PROPERTY_AT_TIME);

        // Test: Should not store an improper date interval, and log a warning (log warning not tested).
        final String invalidDateIntervalString="[bad,interval]";
        // Silently logs a warning for bad dates.
        tIndexer.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj1"), pred1_atTime, vf.createLiteral(invalidDateIntervalString))));

        final String validDateIntervalString="[2016-12-31T20:59:59-05:00,2016-12-31T21:00:00-05:00]";
        tIndexer.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj2"), pred1_atTime, vf.createLiteral(validDateIntervalString))));
        rowsStoredExpected++;

        tIndexer.flush();

        int rowsStoredActual = printTables("junit testing: Temporal intervals stored in testStoreStatement", null, null);
        Assert.assertEquals("Only good intervals should be stored.", rowsStoredExpected*2, rowsStoredActual); // 2 index entries per interval statement
    }

    @Test
    public void testStoreStatementsSameTime() throws IOException, NoSuchAlgorithmException, AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        ValueFactory vf = new ValueFactoryImpl();
        URI pred1_atTime = vf.createURI(URI_PROPERTY_AT_TIME);
        URI pred2_circa = vf.createURI(URI_PROPERTY_CIRCA);

        // These are the same datetime instant but from different time
        // zones.
        // This is an arbitrary zone, BRST=Brazil, better if not local.
        final String ZONETestDateInBRST = "2014-12-31T23:59:59-02:00";
        final String ZONETestDateInZulu = "2015-01-01T01:59:59Z";
        final String ZONETestDateInET = "2014-12-31T20:59:59-05:00";

        // These all should be stored because they are in the predicate list.
        // BUT they will get converted to the same exact datetime in UTC.
        // So we have to make the key distinct! Good luck indexer!
        Statement s1 = new StatementImpl(vf.createURI("foo:subj1"), pred2_circa, vf.createLiteral(ZONETestDateInET));
        Statement s2 = new StatementImpl(vf.createURI("foo:subj2"), pred1_atTime, vf.createLiteral(ZONETestDateInZulu));
        Statement s3 = new StatementImpl(vf.createURI("foo:subj3"), pred1_atTime, vf.createLiteral(ZONETestDateInBRST));
        int rowsStoredExpected = 0;
        tIndexer.storeStatement(convertStatement(s1));
        rowsStoredExpected++;
        tIndexer.storeStatement(convertStatement(s2));
        rowsStoredExpected++;
        tIndexer.storeStatement(convertStatement(s3));
        rowsStoredExpected++;
        int rowsStoredActual = printTables("junit testing: Duplicate times stored", null /*System.out*/, null);
        Assert.assertEquals("Number of Duplicate times stored, 1 means duplicates not handled correctly.", rowsStoredExpected*4, rowsStoredActual);
    }

    /**
     * Test method for {@link AccumuloTemporalIndexer#storeStatements(java.util.Collection)} .
     *
     * @throws TableExistsException
     * @throws TableNotFoundException
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws NoSuchAlgorithmException
     */
    @Test
    public void testStoreStatements()
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException, IllegalArgumentException, IOException,
            NoSuchAlgorithmException {
        long valueHash = 0;
        Collection<Statement> statements = new ArrayList<Statement>(70);
        statements.addAll(Arrays.asList(seriesSpo));
        int rowsStoredExpected = statements.size()*4;  // instants store 4 each
        // hash the expected output:
        for (Statement statement : statements) {
            valueHash = hasher(valueHash, StringUtils.getBytesUtf8(StatementSerializer.writeStatement(statement)));
            valueHash = hasher(valueHash, StringUtils.getBytesUtf8(StatementSerializer.writeStatement(statement)));
            valueHash = hasher(valueHash, StringUtils.getBytesUtf8(StatementSerializer.writeStatement(statement)));
            valueHash = hasher(valueHash, StringUtils.getBytesUtf8(StatementSerializer.writeStatement(statement)));
        }
        statements.add(spo_B02_E30);
        rowsStoredExpected += 2; // intervals store two dates
        statements.add(spo_B30_E32);
        rowsStoredExpected += 2; // intervals store two dates
        valueHash = hasher(valueHash, StringUtils.getBytesUtf8(StatementSerializer.writeStatement(spo_B02_E30)));
        valueHash = hasher(valueHash, StringUtils.getBytesUtf8(StatementSerializer.writeStatement(spo_B02_E30)));
        valueHash = hasher(valueHash, StringUtils.getBytesUtf8(StatementSerializer.writeStatement(spo_B30_E32)));
        valueHash = hasher(valueHash, StringUtils.getBytesUtf8(StatementSerializer.writeStatement(spo_B30_E32)));
        // duplicates will overwrite old ones, no change in the output except timestamps
        statements.add(spo_B30_E32);
        statements.add(spo_B30_E32);

        List<RyaStatement> ryaStatements = Lists.newArrayList();
        for (Statement s : statements){ ryaStatements.add(convertStatement(s));}
        tIndexer.storeStatements(ryaStatements);

        Map<String, Long> statistics = new HashMap<String, Long>();
        int rowsStoredActual = printTables("junit testing: StoreStatements multiple statements", null, statistics);
        Assert.assertEquals("Number of rows stored.", rowsStoredExpected, rowsStoredActual); // 4 index entries per statement
        Assert.assertEquals("value hash.", valueHash, statistics.get(STAT_VALUEHASH).longValue());
    }

    /**
     * test this classe's hash method to check un-ordered results.
     */
    @Test
    public void testSelfTestHashMethod() {
        // self test on the hash method:
        long hash01dup1 = hasher(0, new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        long hash01dup2 = hasher(0, new byte[] { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 });
        Assert.assertEquals("same numbers, different sequence, hash should be the same.", hash01dup1, hash01dup2);

        // this one fails for sum hash, passes for XOR
        long hash02dup1 = hasher(0, new byte[] { 123, 2, 1, 1 });
        hash02dup1 = hasher(hash02dup1, new byte[] { 123, 1, 1, 2 });
        long hash02dup2 = hasher(0, new byte[] { 123, 1, 1, 2 });
        hash02dup2 = hasher(hash02dup2, new byte[] { 123, 1, 3, 0, 0 });
        Assert.assertTrue("Different numbers, should be different hashes: " + hash02dup1 + " != " + hash02dup2, hash02dup1 != hash02dup2);
    }
    /**
     * Test instant equal to a given instant.
     * From the series: instant {equal, before, after} instant
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    @Test
    public void testQueryInstantEqualsInstant() throws IOException, QueryEvaluationException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        // these should not match as they are not instances.
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B30_E32));
        int expectedStoreCount = 5 * 2; // two entries for intervals

        // seriesSpo[s] and seriesTs[s] are statements and instant for s seconds after the uniform time.
        int searchForSeconds = 5;
        int expectedResultCount = 1;
        for (int s = 0; s <= searchForSeconds + 3; s++) { // <== logic here
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
            expectedStoreCount+=4; //4 entries per statement.
        }
        tIndexer.flush();

        int rowsStoredActual = printTables("junit testing: testQueryInstantEqualsInstant 0 to 8 seconds and 5 intervals stored. expectedStoreCount="+expectedStoreCount, null /*System.out*/, null);
        Assert.assertEquals("Should find count of rows.", expectedStoreCount, rowsStoredActual);

        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantEqualsInstant(seriesTs[searchForSeconds], EMPTY_CONSTRAINTS); // <== logic here
        int count = 0;
        while (iter.hasNext()) {
            Statement s = iter.next();
            Statement nextExpectedStatement = seriesSpo[searchForSeconds]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        Assert.assertEquals("Should find count of rows.", expectedResultCount, count);

    }

    /**
     * Test instant after a given instant.
     * From the series: instant {equal, before, after} instant
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    @Test
    public void testQueryInstantAfterInstant() throws IOException, QueryEvaluationException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        // these should not match as they are not instances.
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B30_E32));

        // seriesSpo[s] and seriesTs[s] are statements and instant for s seconds after the uniform time.
        int searchForSeconds = 4;
        int expectedResultCount = 9;
        for (int s = 0; s <= searchForSeconds + expectedResultCount; s++) { // <== logic here
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }
        tIndexer.flush();
        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantAfterInstant(seriesTs[searchForSeconds], EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            Statement s = iter.next();
            Statement nextExpectedStatement = seriesSpo[searchForSeconds + count + 1]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        Assert.assertEquals("Should find count of rows.", expectedResultCount, count);
    }
    /**
     * Test instant before a given instant.
     * From the series: instant {equal, before, after} instant
     */
    @Test
    public void testQueryInstantBeforeInstant() throws IOException, QueryEvaluationException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        // these should not match as they are not instances.
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B30_E32));

        // seriesSpo[s] and seriesTs[s] are statements and instant for s seconds after the uniform time.
        int searchForSeconds = 4;
        int expectedResultCount = 4;
        for (int s = 0; s <= searchForSeconds + 15; s++) { // <== logic here
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }
        tIndexer.flush();
        CloseableIteration<Statement, QueryEvaluationException> iter;

        iter = tIndexer.queryInstantBeforeInstant(seriesTs[searchForSeconds], EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            Statement s = iter.next();
            Statement nextExpectedStatement = seriesSpo[count]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        Assert.assertEquals("Should find count of rows.", expectedResultCount, count);
    }

    /**
     * Test instant before given interval.
     * From the series:  Instance {before, after, inside} given Interval
     */
    @Test
    public void testQueryInstantBeforeInterval() throws IOException, QueryEvaluationException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        // these should not match as they are not instances.
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B30_E32));

        // seriesSpo[s] and seriesTs[s] are statements and instants for s seconds after the uniform time.
        TemporalInterval searchForSeconds = tvB02_E31;
        int expectedResultCount = 2; // 00 and 01 seconds.
        for (int s = 0; s <= 40; s++) { // <== logic here
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }
        tIndexer.flush();
        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantBeforeInterval(searchForSeconds, EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            Statement s = iter.next();
            Statement nextExpectedStatement = seriesSpo[count]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        Assert.assertEquals("Should find count of rows.", expectedResultCount, count);
    }

    /**
     * Test instant after given interval.
     * Instance {before, after, inside} given Interval
     */
    @Test
    public void testQueryInstantAfterInterval() throws IOException, QueryEvaluationException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        // these should not match as they are not instances.
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B30_E32));

        // seriesSpo[s] and seriesTs[s] are statements and instants for s seconds after the uniform time.
        TemporalInterval searchAfterInterval = tvB02_E31; // from 2 to 31 seconds
        int endingSeconds = 31;
        int expectedResultCount = 9; // 32,33,...,40 seconds.
        for (int s = 0; s <= endingSeconds + expectedResultCount; s++) { // <== logic here
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }
        tIndexer.flush();
        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantAfterInterval(searchAfterInterval, EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            Statement s = iter.next();
            Statement nextExpectedStatement = seriesSpo[count + endingSeconds + 1]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        Assert.assertEquals("Should find count of rows.", expectedResultCount, count);
    }

    /**
     * Test instant inside given interval.
     * Instance {before, after, inside} given Interval
     */
    @Test
    public void testQueryInstantInsideInterval() throws IOException, QueryEvaluationException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        // these should not match as they are not instances.
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B30_E32));

        // seriesSpo[s] and seriesTs[s] are statements and instants for s seconds after the uniform time.
        TemporalInterval searchInsideInterval = tvB02_E31; // from 2 to 31 seconds
        int beginningSeconds = 2; // <== logic here, and next few lines.
        int endingSeconds = 31;
        int expectedResultCount = endingSeconds - beginningSeconds - 1; // 3,4,...,30 seconds.
        for (int s = 0; s <= 40; s++) {
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }
        tIndexer.flush();
        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantInsideInterval(searchInsideInterval, EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            Statement s = iter.next();
            Statement nextExpectedStatement = seriesSpo[count + beginningSeconds + 1]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        Assert.assertEquals("Should find count of rows.", expectedResultCount, count);
    }
    /**
     * Test instant is the Beginning of the given interval.
     * from the series: Instance {hasBeginning, hasEnd} Interval
     */
    @Test
    public void testQueryInstantHasBeginningInterval() throws IOException, QueryEvaluationException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        // these should not match as they are not instances.
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B30_E32));

        // seriesSpo[s] and seriesTs[s] are statements and instants for s seconds after the uniform time.
        TemporalInterval searchInsideInterval = tvB02_E31; // from 2 to 31 seconds
        int searchSeconds = 2; // <== logic here, and next few lines.
        int expectedResultCount = 1; // 2 seconds.
        for (int s = 0; s <= 10; s++) {
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }
        tIndexer.flush();
        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantHasBeginningInterval(searchInsideInterval, EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            Statement s = iter.next();
            Statement nextExpectedStatement = seriesSpo[searchSeconds]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        Assert.assertEquals("Should find count of rows.", expectedResultCount, count);
    }
    /**
     * Test instant is the end of the given interval.
     * from the series: Instance {hasBeginning, hasEnd} Interval
     */
    @Test
    public void testQueryInstantHasEndInterval()  throws IOException, QueryEvaluationException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        // these should not match as they are not instances.
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B30_E32));

        // seriesSpo[s] and seriesTs[s] are statements and instants for s seconds after the uniform time.
        TemporalInterval searchInsideInterval = tvB02_E31; // from 2 to 31 seconds
        int searchSeconds = 31; // <== logic here, and next few lines.
        int expectedResultCount = 1; // 31 seconds.
        for (int s = 0; s <= 40; s++) {
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }
        tIndexer.flush();
        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantHasEndInterval(searchInsideInterval, EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            Statement s = iter.next();
            Statement nextExpectedStatement = seriesSpo[searchSeconds]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        Assert.assertEquals("Should find count of rows.", expectedResultCount, count);
    }

    /**
     * Test method for
     * {@link org.apache.rya.indexing.accumulo.temporal.AccumuloTemporalIndexer#queryIntervalEquals(TemporalInterval, StatementConstraints)}
     * .
     * @throws IOException
     * @throws QueryEvaluationException
     *
     */
    @Test
    public void testQueryIntervalEquals() throws IOException, QueryEvaluationException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B30_E32));
        tIndexer.storeStatement(convertStatement(seriesSpo[4])); // instance at 4 seconds
        tIndexer.flush();

        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryIntervalEquals(tvB02_E40, EMPTY_CONSTRAINTS);
        // Should be found twice:
        Assert.assertTrue("queryIntervalEquals: spo_B02_E40 should be found, but actually returned empty results. spo_B02_E40=" + spo_B02_E40, iter.hasNext());
        Assert.assertTrue("queryIntervalEquals: spo_B02_E40 should be found, but does not match.", spo_B02_E40.equals(iter.next()));
        Assert.assertFalse("queryIntervalEquals: Find no more than one, but actually has more.", iter.hasNext());
    }

    /**
     * Test interval before a given interval, for method:
     * {@link AccumuloTemporalIndexer#queryIntervalBefore(TemporalInterval, StatementConstraints)}.
     *
     * @throws IOException
     * @throws QueryEvaluationException
     */
    @Test
    public void testQueryIntervalBefore() throws IOException, QueryEvaluationException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        tIndexer.storeStatement(convertStatement(spo_B00_E01));
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        // instants should be ignored.
        tIndexer.storeStatement(convertStatement(spo_B30_E32));
        tIndexer.storeStatement(convertStatement(seriesSpo[1])); // instance at 1 seconds
        tIndexer.storeStatement(convertStatement(seriesSpo[2]));
        tIndexer.storeStatement(convertStatement(seriesSpo[31]));
        tIndexer.flush();

        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryIntervalBefore(tvB02_E31, EMPTY_CONSTRAINTS);
        // Should be found twice:
        Assert.assertTrue("spo_B00_E01 should be found, but actually returned empty results. spo_B00_E01=" + spo_B00_E01, iter.hasNext());
        Assert.assertTrue("spo_B00_E01 should be found, but found another.", spo_B00_E01.equals(iter.next()));
        Assert.assertFalse("Find no more than one, but actually has more.", iter.hasNext());
    }

    /**
     * interval is after the given interval.  Find interval beginnings after the endings of the given interval.
     * {@link AccumuloTemporalIndexer#queryIntervalAfter(TemporalInterval, StatementContraints).
     *
     * @throws IOException
     * @throws QueryEvaluationException
     */
    @Test
    public void testQueryIntervalAfter() throws IOException, QueryEvaluationException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        tIndexer.storeStatement(convertStatement(spo_B00_E01));
        tIndexer.storeStatement(convertStatement(spo_B02_E29)); //<- after this one.
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        tIndexer.storeStatement(convertStatement(spo_B29_E30));
        tIndexer.storeStatement(convertStatement(spo_B30_E32));
        // instants should be ignored.
        tIndexer.storeStatement(convertStatement(spo_B02));
        tIndexer.storeStatement(convertStatement(seriesSpo[1])); // instance at 1 seconds
        tIndexer.storeStatement(convertStatement(seriesSpo[2]));
        tIndexer.storeStatement(convertStatement(seriesSpo[31]));
        tIndexer.flush();

        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryIntervalAfter(tvB02_E29, EMPTY_CONSTRAINTS);
        // Should be found twice:
        Assert.assertTrue("spo_B30_E32 should be found, but actually returned empty results. spo_B30_E32=" + spo_B30_E32, iter.hasNext());
        Statement s = iter.next();
        Assert.assertTrue("spo_B30_E32 should be found, but found another. spo_B30_E32="+spo_B30_E32+", but found="+s, spo_B30_E32.equals(s));
        Assert.assertFalse("Find no more than one, but actually has more.", iter.hasNext());

    }

    /**
     * Test instant after a given instant WITH two different predicates as constraints.
     */
    @Test
    public void testQueryWithMultiplePredicates() throws IOException, QueryEvaluationException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        // these should not match as they are not instances.
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B30_E32));

        // seriesSpo[s] and seriesTs[s] are statements and instant for s seconds after the uniform time.
        int searchForSeconds = 4;
        int expectedResultCount = 9;
        for (int s = 0; s <= searchForSeconds + expectedResultCount; s++) { // <== logic here
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }
        ValueFactory vf = new ValueFactoryImpl();
        URI pred3_CIRCA_ = vf.createURI(URI_PROPERTY_CIRCA);  // this one to ignore.
        URI pred2_eventTime = vf.createURI(URI_PROPERTY_EVENT_TIME);
        URI pred1_atTime = vf.createURI(URI_PROPERTY_AT_TIME);

        // add the predicate = EventTime ; Store in an array for verification.
        Statement[] SeriesTs_EventTime = new Statement[expectedResultCount+1];
        for (int s = 0; s <= searchForSeconds + expectedResultCount; s++) { // <== logic here
            Statement statement = new StatementImpl(vf.createURI("foo:EventTimeSubj0" + s), pred2_eventTime, vf.createLiteral(seriesTs[s].getAsReadable()));
            tIndexer.storeStatement(convertStatement(statement));
            if (s>searchForSeconds)
                SeriesTs_EventTime[s - searchForSeconds -1 ] = statement;
        }
        // add the predicate = CIRCA ; to be ignored because it is not in the constraints.
        for (int s = 0; s <= searchForSeconds + expectedResultCount; s++) { // <== logic here
            Statement statement = new StatementImpl(vf.createURI("foo:CircaEventSubj0" + s), pred3_CIRCA_, vf.createLiteral(seriesTs[s].getAsReadable()));
            tIndexer.storeStatement(convertStatement(statement));
        }
        tIndexer.flush();
        CloseableIteration<Statement, QueryEvaluationException> iter;
        StatementConstraints constraints = new StatementConstraints();
        constraints.setPredicates(new HashSet<URI>(Arrays.asList( pred2_eventTime,  pred1_atTime )));

        iter = tIndexer.queryInstantAfterInstant(seriesTs[searchForSeconds], constraints); // EMPTY_CONSTRAINTS);//
        int count_AtTime = 0;
        int count_EventTime = 0;
        while (iter.hasNext()) {
            Statement s = iter.next();
            //System.out.println("testQueryWithMultiplePredicates result="+s);
            Statement nextExpectedStatement = seriesSpo[searchForSeconds + count_AtTime + 1]; // <== logic here
            if (s.getPredicate().equals(pred1_atTime)) {
                assertTrue("Should match atTime: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
                count_AtTime++;
            }
            else if (s.getPredicate().equals(pred2_eventTime)) {
                assertTrue("Should match eventTime: " + SeriesTs_EventTime[count_EventTime] + " == " + s, SeriesTs_EventTime[count_EventTime].equals(s));
                count_EventTime++;
            } else {
                assertTrue("This predicate should not be returned: "+s, false);
            }

        }

        Assert.assertEquals("Should find count of atTime    rows.", expectedResultCount, count_AtTime);
        Assert.assertEquals("Should find count of eventTime rows.", expectedResultCount, count_EventTime);
    }


    /**
     * Test method for {@link AccumuloTemporalIndexer#getIndexablePredicates()} .
     *
     * @throws TableExistsException
     * @throws TableNotFoundException
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws IOException
     */
    @Test
    public void testGetIndexablePredicates() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException, IOException {
        Set<URI> p = tIndexer.getIndexablePredicates();
        Assert.assertEquals("number of predicates returned:", 3, p.size());
    }

    /**
     * Count all the entries in the temporal index table, return the count.
     * Uses printTables for reliability.
     *
     */
    public int countAllRowsInTable() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, NoSuchAlgorithmException {
        return printTables("Counting rows.", null, null);
    }

    /**
     * Print and gather statistics on the entire index table.
     *
     * @param description
     *            Printed to the console to find the test case.
     * @param out
     *            null or System.out or other output to send a listing.
     * @param statistics
     *            Hashes, sums, and counts for assertions.
     * @return Count of entries in the index table.
     */
    public int printTables(String description, PrintStream out, Map<String, Long> statistics)
            throws TableNotFoundException, AccumuloException, AccumuloSecurityException
    {
        if (out == null) {
            out = new PrintStream(new NullOutputStream());
        }
        out.println("-- start printTables() -- " + description);
        String FORMAT = "%-20s  %-20s  %-40s  %-40s\n";
        int rowsPrinted = 0;
        long keyHasher = 0;
        long valueHasher = 0;
        final String indexTableName = tIndexer.getTableName();
        out.println("Reading : " + indexTableName);
        out.format(FORMAT, "--Row--", "--ColumnFamily--", "--ColumnQualifier--", "--Value--");

        Scanner s = ConfigUtils.getConnector(conf).createScanner(indexTableName, Authorizations.EMPTY);
        for (Entry<Key, org.apache.accumulo.core.data.Value> entry : s) {
            rowsPrinted++;
            Key k = entry.getKey();
            out.format(FORMAT, toHumanString(k.getRow()),
                    toHumanString(k.getColumnFamily()),
                    toHumanString(k.getColumnQualifier()),
                    toHumanString(entry.getValue()));
            keyHasher = hasher(keyHasher, (StringUtils.getBytesUtf8(entry.getKey().toStringNoTime())));
            valueHasher = hasher(valueHasher, (entry.getValue().get()));
        }
        out.println();

        if (statistics != null) {
            statistics.put(STAT_COUNT, (long) rowsPrinted);
            statistics.put(STAT_KEYHASH, keyHasher);
            statistics.put(STAT_VALUEHASH, valueHasher);
        }

        return rowsPrinted;

    }

    /**
     * Order independent hashcode.
     * Read more: http://stackoverflow.com/questions/18021643/hashing-a-set-of-integers-in-an-order-independent-way
     *
     * @param hashcode
     * @param list
     * @return
     */
    private static long hasher(long hashcode, byte[] list) {
        long sum = 0;
        for (byte val : list) {
            sum += 1L + val;
        }
        hashcode ^= sum;
        return hashcode;
    }

    /**
     * convert a non-utf8 byte[] and text and value to string and show unprintable bytes as {xx} where x is hex.
     * @param value
     * @return Human readable representation.
     */
    static String toHumanString(Value value) {
        return toHumanString(value==null?null:value.get());
    }
    static String toHumanString(Text text) {
        return toHumanString(text==null?null:text.copyBytes());
    }
    static String toHumanString(byte[] bytes) {
        if (bytes==null)
            return "{null}";
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            if ((b > 0x7e) || (b < 32)) {
                sb.append("{");
                sb.append(Integer.toHexString( b & 0xff )); // Lop off the sign extended ones.
                sb.append("}");
            } else if (b == '{'||b == '}') { // Escape the literal braces.
                sb.append("{");
                sb.append((char)b);
                sb.append("}");
            } else
                sb.append((char)b);
        }
        return sb.toString();
    }
}
