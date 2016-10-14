package org.apache.rya.indexing.mongo;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryEvaluationException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;
import info.aduna.iteration.CloseableIteration;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.TemporalInterval;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.mongodb.temporal.MongoTemporalIndexer;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;

/**
 * JUnit tests for TemporalIndexer and it's implementation MongoTemporalIndexer
 *
 * If you enjoy this test, please read RyaTemporalIndexerTest and YagoKBTest, which contain
 * many example SPARQL queries and updates and attempts to test independently of Mongo:
 *
 *     extras/indexingSail/src/test/java/org.apache/rya/indexing/Mongo/RyaTemporalIndexerTest.java
 *     {@link org.apache.rya.indexing.Mongo.RyaTemporalIndexerTest}
 *     {@link org.apache.rya.indexing.Mongo.YagoKBTest.java}
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
 */
public final class MongoTemporalIndexerTest {
    MongoDBRdfConfiguration conf;
    MongoTemporalIndexer tIndexer;
    DBCollection collection;

    private static final String URI_PROPERTY_EVENT_TIME = "Property:event:time";
    private static final String URI_PROPERTY_CIRCA = "Property:circa";
    private static final String URI_PROPERTY_AT_TIME = "Property:atTime";
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
    static final TemporalInterval tvB00_E01 = new TemporalInterval(makeInstant(00), makeInstant(01));
    static final TemporalInterval tvB29_E30= new TemporalInterval(makeInstant(29), makeInstant(30));
    static final TemporalInterval tvB30_E32= new TemporalInterval(makeInstant(30), makeInstant(32));
    static final TemporalInterval tvB03_E20 = new TemporalInterval(makeInstant(03), makeInstant(20));
    // 30 seconds, Begins earlier, ends later
    static final TemporalInterval tvB02_E30= new TemporalInterval(makeInstant(02), makeInstant(30));
    // use for interval after
    static final TemporalInterval tvB02_E29= new TemporalInterval(makeInstant(02), makeInstant(29));
    // same as above, but ends in the middle
    static final TemporalInterval tvB02_E31 = new TemporalInterval(makeInstant(02), makeInstant(31));
    // same as above, but ends even later
    static final TemporalInterval tvB02_E40 = new TemporalInterval(makeInstant(02), makeInstant(40));
    // instant, match beginnings of several above, before tiB03_E20
    static final TemporalInstant tsB02 = makeInstant(02);
    // instant, after all above
    static final TemporalInstant tsB04 = makeInstant(04);

    // Create a series of instants about times 0 - 40 seconds
    static final TemporalInstant seriesTs[];
    static {
        seriesTs = new TemporalInstant[SERIES_OF_SECONDS];
        for (int i = 0; i <= 40; i++) {
            seriesTs[i] = makeInstant(i);
        }
    };

    /**
     * Make an uniform instant with given seconds.
     */
    static TemporalInstant makeInstant(final int secondsMakeMeUnique) {
        return new TemporalInstantRfc3339(2015, 12, 30, 12, 00, secondsMakeMeUnique);
    }

    static {
        // Setup the statements only once. Each test will store some of these in there own index table.
        final ValueFactory vf = new ValueFactoryImpl();
        final URI pred1_atTime = vf.createURI(URI_PROPERTY_AT_TIME);
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
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void before() throws Exception {
        conf = new MongoDBRdfConfiguration();
        conf.set(ConfigUtils.USE_MONGO, "true");
        conf.set(MongoDBRdfConfiguration.USE_TEST_MONGO, "true");
        conf.set(MongoDBRdfConfiguration.MONGO_DB_NAME, "test");
        conf.set(MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX, "rya_");
        conf.setTablePrefix("isthisused_");
        
        // This is from http://linkedevents.org/ontology
        // and http://motools.sourceforge.net/event/event.html
        conf.setStrings(ConfigUtils.TEMPORAL_PREDICATES_LIST, ""
                + URI_PROPERTY_AT_TIME + ","
                + URI_PROPERTY_CIRCA + ","
                + URI_PROPERTY_EVENT_TIME);

        final MongodForTestsFactory testsFactory = MongodForTestsFactory.with(Version.Main.PRODUCTION);
        final MongoClient mongoClient = testsFactory.newMongo();
        tIndexer = new MongoTemporalIndexer();
        tIndexer.initIndexer(conf, mongoClient);


        final String dbName = conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME);
        final DB db = mongoClient.getDB(dbName);
        collection = db.getCollection(conf.get(MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX, "rya") + tIndexer.getCollectionName());
   }
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        tIndexer.close();
    }

    /**
     * Test method for {@link MongoTemporalIndexer#storeStatement(convertStatement(org.openrdf.model.Statement)}
     */
    @Test
    public void testStoreStatement() throws IOException {
        final ValueFactory vf = new ValueFactoryImpl();

        final URI pred1_atTime = vf.createURI(URI_PROPERTY_AT_TIME);
        final URI pred2_circa = vf.createURI(URI_PROPERTY_CIRCA);

        // Should not be stored because they are not in the predicate list
        final String validDateStringWithThirteens = "1313-12-13T13:13:13Z";
        tIndexer.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj1"), RDFS.LABEL, vf.createLiteral(validDateStringWithThirteens))));

        final String invalidDateString = "ThisIsAnInvalidDate";
        tIndexer.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj2"), pred1_atTime, vf.createLiteral(invalidDateString))));

        // These are different datetimes instant but from different time zones.
        // This is an arbitrary zone, BRST=Brazil, better if not local.
        // same as "2015-01-01T01:59:59Z"
        final String testDate2014InBRST = "2014-12-31T23:59:59-02:00";
        // next year, same as "2017-01-01T01:59:59Z"
        final String testDate2016InET = "2016-12-31T20:59:59-05:00";

        // These should be stored because they are in the predicate list.
        // BUT they will get converted to the same exact datetime in UTC.
        final Statement s3 = new StatementImpl(vf.createURI("foo:subj3"), pred1_atTime, vf.createLiteral(testDate2014InBRST));
        final Statement s4 = new StatementImpl(vf.createURI("foo:subj4"), pred2_circa, vf.createLiteral(testDate2016InET));
        tIndexer.storeStatement(convertStatement(s3));
        tIndexer.storeStatement(convertStatement(s4));

        // This should not be stored because the object is not a literal
        tIndexer.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj5"), pred1_atTime, vf.createURI("in:valid"))));

        printTables("junit testing: Temporal entities stored in testStoreStatement");
        assertEquals(2, tIndexer.getCollection().find().count());
    }

    @Test
    public void testDelete() throws IOException, MongoException, TableNotFoundException, TableExistsException, NoSuchAlgorithmException {
        final ValueFactory vf = new ValueFactoryImpl();

        final URI pred1_atTime = vf.createURI(URI_PROPERTY_AT_TIME);
        final URI pred2_circa = vf.createURI(URI_PROPERTY_CIRCA);

        final String testDate2014InBRST = "2014-12-31T23:59:59-02:00";
        final String testDate2016InET = "2016-12-31T20:59:59-05:00";

        // These should be stored because they are in the predicate list.
        // BUT they will get converted to the same exact datetime in UTC.
        final Statement s1 = new StatementImpl(vf.createURI("foo:subj3"), pred1_atTime, vf.createLiteral(testDate2014InBRST));
        final Statement s2 = new StatementImpl(vf.createURI("foo:subj4"), pred2_circa, vf.createLiteral(testDate2016InET));
        tIndexer.storeStatement(convertStatement(s1));
        tIndexer.storeStatement(convertStatement(s2));


        printTables("junit testing: Temporal entities stored in testDelete before delete");
        assertEquals("Number of rows stored.", 2, collection.count()); // 4 index entries per statement

        tIndexer.deleteStatement(convertStatement(s1));
        tIndexer.deleteStatement(convertStatement(s2));

        printTables("junit testing: Temporal entities stored in testDelete after delete");
        assertEquals("Number of rows stored after delete.", 0, collection.count());
    }

    /**
     * Test instant after a given instant.
     * From the series: instant {equal, before, after} instant
     * @throws MongoSecurityException
     * @throws MongoException
     * @throws TableNotFoundException
     */
    @Test
    public void testQueryInstantAfterInstant() throws IOException, QueryEvaluationException, TableNotFoundException, MongoException {
        // tiB02_E30 read as: Begins 2 seconds, ends at 30 seconds
        // these should not match as they are not instances.
        tIndexer.storeStatement(convertStatement(spo_B03_E20));
        tIndexer.storeStatement(convertStatement(spo_B02_E30));
        tIndexer.storeStatement(convertStatement(spo_B02_E40));
        tIndexer.storeStatement(convertStatement(spo_B02_E31));
        tIndexer.storeStatement(convertStatement(spo_B30_E32));

        // seriesSpo[s] and seriesTs[s] are statements and instant for s seconds after the uniform time.
        final int searchForSeconds = 4;
        final int expectedResultCount = 9;
        for (int s = 0; s <= searchForSeconds + expectedResultCount; s++) { // <== logic here
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }

        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantAfterInstant(seriesTs[searchForSeconds], EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            final Statement s = iter.next();
            final Statement nextExpectedStatement = seriesSpo[searchForSeconds + count + 1]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        assertEquals("Should find count of rows.", expectedResultCount, count);
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
        final int searchForSeconds = 4;
        final int expectedResultCount = 4;
        for (int s = 0; s <= searchForSeconds + 15; s++) { // <== logic here
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }

        CloseableIteration<Statement, QueryEvaluationException> iter;

        iter = tIndexer.queryInstantBeforeInstant(seriesTs[searchForSeconds], EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            final Statement s = iter.next();
            final Statement nextExpectedStatement = seriesSpo[count]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        assertEquals("Should find count of rows.", expectedResultCount, count);
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
        final TemporalInterval searchForSeconds = tvB02_E31;
        final int expectedResultCount = 2; // 00 and 01 seconds.
        for (int s = 0; s <= 40; s++) { // <== logic here
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }

        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantBeforeInterval(searchForSeconds, EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            final Statement s = iter.next();
            final Statement nextExpectedStatement = seriesSpo[count]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        assertEquals("Should find count of rows.", expectedResultCount, count);
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
        final TemporalInterval searchAfterInterval = tvB02_E31; // from 2 to 31 seconds
        final int endingSeconds = 31;
        final int expectedResultCount = 9; // 32,33,...,40 seconds.
        for (int s = 0; s <= endingSeconds + expectedResultCount; s++) { // <== logic here
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }

        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantAfterInterval(searchAfterInterval, EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            final Statement s = iter.next();
            final Statement nextExpectedStatement = seriesSpo[count + endingSeconds + 1]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        assertEquals("Should find count of rows.", expectedResultCount, count);
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
        final TemporalInterval searchInsideInterval = tvB02_E31; // from 2 to 31 seconds
        final int beginningSeconds = 2; // <== logic here, and next few lines.
        final int endingSeconds = 31;
        final int expectedResultCount = endingSeconds - beginningSeconds - 1; // 3,4,...,30 seconds.
        for (int s = 0; s <= 40; s++) {
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }

        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantInsideInterval(searchInsideInterval, EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            final Statement s = iter.next();
            final Statement nextExpectedStatement = seriesSpo[count + beginningSeconds + 1]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        assertEquals("Should find count of rows.", expectedResultCount, count);
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
        final TemporalInterval searchInsideInterval = tvB02_E31; // from 2 to 31 seconds
        final int searchSeconds = 2; // <== logic here, and next few lines.
        final int expectedResultCount = 1; // 2 seconds.
        for (int s = 0; s <= 10; s++) {
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }

        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantHasBeginningInterval(searchInsideInterval, EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            final Statement s = iter.next();
            final Statement nextExpectedStatement = seriesSpo[searchSeconds]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        assertEquals("Should find count of rows.", expectedResultCount, count);
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
        final TemporalInterval searchInsideInterval = tvB02_E31; // from 2 to 31 seconds
        final int searchSeconds = 31; // <== logic here, and next few lines.
        final int expectedResultCount = 1; // 31 seconds.
        for (int s = 0; s <= 40; s++) {
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }

        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryInstantHasEndInterval(searchInsideInterval, EMPTY_CONSTRAINTS);
        int count = 0;
        while (iter.hasNext()) {
            final Statement s = iter.next();
            final Statement nextExpectedStatement = seriesSpo[searchSeconds]; // <== logic here
            assertTrue("Should match: " + nextExpectedStatement + " == " + s, nextExpectedStatement.equals(s));
            count++;
        }
        assertEquals("Should find count of rows.", expectedResultCount, count);
    }

    /**
     * Test method for
     * {@link org.apache.rya.indexing.Mongo.temporal.MongoTemporalIndexer#queryIntervalEquals(TemporalInterval, StatementConstraints)}
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


        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryIntervalEquals(tvB02_E40, EMPTY_CONSTRAINTS);
        // Should be found twice:
        assertTrue("queryIntervalEquals: spo_B02_E40 should be found, but actually returned empty results. spo_B02_E40=" + spo_B02_E40, iter.hasNext());
        assertTrue("queryIntervalEquals: spo_B02_E40 should be found, but does not match.", spo_B02_E40.equals(iter.next()));
        assertFalse("queryIntervalEquals: Find no more than one, but actually has more.", iter.hasNext());
    }

    /**
     * Test interval before a given interval, for method:
     * {@link MongoTemporalIndexer#queryIntervalBefore(TemporalInterval, StatementConstraints)}.
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


        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryIntervalBefore(tvB02_E31, EMPTY_CONSTRAINTS);
        // Should be found twice:
        assertTrue("spo_B00_E01 should be found, but actually returned empty results. spo_B00_E01=" + spo_B00_E01, iter.hasNext());
        assertTrue("spo_B00_E01 should be found, but found another.", spo_B00_E01.equals(iter.next()));
        assertFalse("Find no more than one, but actually has more.", iter.hasNext());
    }

    /**
     * interval is after the given interval.  Find interval beginnings after the endings of the given interval.
     * {@link MongoTemporalIndexer#queryIntervalAfter(TemporalInterval, StatementContraints).
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

        CloseableIteration<Statement, QueryEvaluationException> iter;
        iter = tIndexer.queryIntervalAfter(tvB02_E29, EMPTY_CONSTRAINTS);
        // Should be found twice:
        assertTrue("spo_B30_E32 should be found, but actually returned empty results. spo_B30_E32=" + spo_B30_E32, iter.hasNext());
        final Statement s = iter.next();
        assertTrue("spo_B30_E32 should be found, but found another. spo_B30_E32="+spo_B30_E32+", but found="+s, spo_B30_E32.equals(s));
        assertFalse("Find no more than one, but actually has more.", iter.hasNext());

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
        final int searchForSeconds = 4;
        final int expectedResultCount = 9;
        for (int s = 0; s <= searchForSeconds + expectedResultCount; s++) { // <== logic here
            tIndexer.storeStatement(convertStatement(seriesSpo[s]));
        }
        final ValueFactory vf = new ValueFactoryImpl();
        final URI pred3_CIRCA_ = vf.createURI(URI_PROPERTY_CIRCA);  // this one to ignore.
        final URI pred2_eventTime = vf.createURI(URI_PROPERTY_EVENT_TIME);
        final URI pred1_atTime = vf.createURI(URI_PROPERTY_AT_TIME);

        // add the predicate = EventTime ; Store in an array for verification.
        final Statement[] SeriesTs_EventTime = new Statement[expectedResultCount+1];
        for (int s = 0; s <= searchForSeconds + expectedResultCount; s++) { // <== logic here
            final Statement statement = new StatementImpl(vf.createURI("foo:EventTimeSubj0" + s), pred2_eventTime, vf.createLiteral(seriesTs[s].getAsReadable()));
            tIndexer.storeStatement(convertStatement(statement));
            if (s>searchForSeconds) {
                SeriesTs_EventTime[s - searchForSeconds -1 ] = statement;
            }
        }
        // add the predicate = CIRCA ; to be ignored because it is not in the constraints.
        for (int s = 0; s <= searchForSeconds + expectedResultCount; s++) { // <== logic here
            final Statement statement = new StatementImpl(vf.createURI("foo:CircaEventSubj0" + s), pred3_CIRCA_, vf.createLiteral(seriesTs[s].getAsReadable()));
            tIndexer.storeStatement(convertStatement(statement));
        }

        CloseableIteration<Statement, QueryEvaluationException> iter;
        final StatementConstraints constraints = new StatementConstraints();
        constraints.setPredicates(new HashSet<URI>(Arrays.asList( pred2_eventTime,  pred1_atTime )));

        iter = tIndexer.queryInstantAfterInstant(seriesTs[searchForSeconds], constraints); // EMPTY_CONSTRAINTS);//
        int count_AtTime = 0;
        int count_EventTime = 0;
        while (iter.hasNext()) {
            final Statement s = iter.next();
            final Statement nextExpectedStatement = seriesSpo[searchForSeconds + count_AtTime + 1]; // <== logic here
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

        assertEquals("Should find count of atTime    rows.", expectedResultCount, count_AtTime);
        assertEquals("Should find count of eventTime rows.", expectedResultCount, count_EventTime);
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
     * @throws IOException
     */
    public void printTables(final String description) throws IOException {
        System.out.println("-- start printTables() -- " + description);
        System.out.println("Reading : " + tIndexer.getCollection().getFullName());
        final DBCursor cursor = tIndexer.getCollection().find();
        while(cursor.hasNext()) {
            final DBObject dbo = cursor.next();
            System.out.println(dbo.toString());
        }
        System.out.println();
    }
}
