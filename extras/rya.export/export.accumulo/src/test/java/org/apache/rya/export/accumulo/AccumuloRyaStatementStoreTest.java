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
package org.apache.rya.export.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rya.export.accumulo.common.InstanceType;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;
import org.apache.rya.export.api.MergerException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.indexing.accumulo.ConfigUtils;

/**
 * Tests the methods of {@link AccumuloRyaStatementStore}.
 */
public class AccumuloRyaStatementStoreTest {
    private static final Logger log = LogManager.getLogger(AccumuloRyaStatementStoreTest.class);
    private static final InstanceType INSTANCE_TYPE = InstanceType.MINI;

    private static final boolean IS_MOCK = INSTANCE_TYPE.isMock();
    private static final String USER_NAME = IS_MOCK ? "test_user" : AccumuloInstanceDriver.ROOT_USER_NAME;
    private static final String PASSWORD = "password";
    private static final String INSTANCE_NAME = "test_instance";
    private static final String AUTHS = "test_auth";
    private static final String RYA_TABLE_PREFIX = "test_";

    // Rya data store and connections.
    private static AccumuloInstanceDriver accumuloInstanceDriver = null;
//    protected Connector accumuloConn = null;
//    protected RyaSailRepository ryaRepo = null;
//    protected RepositoryConnection ryaConn = null;
//    protected AccumuloRyaDAO accumuloRyaDao = null;
    private static AccumuloRdfConfiguration accumuloRdfConfiguration = null;

    private static final Date DATE = new Date();

    private static final ImmutableList<RyaStatement> RYA_STATEMENTS = ImmutableList.of(
        TestUtils.createRyaStatement("Adam", "analyzes", "apple", DATE),
        TestUtils.createRyaStatement("Bob", "bites", "burger", DATE),
        TestUtils.createRyaStatement("Charlie", "checks", "chores", DATE),
        TestUtils.createRyaStatement("Debbie", "drives", "deal", DATE),
        TestUtils.createRyaStatement("Emma", "eats", "everything", DATE)
    );

    @BeforeClass
    public static void setupResources() throws Exception {
    	//TestUtils.setupLogging();
    	//log.info("STARTING");
        // Initialize the Accumulo instance that will be used to store Triples and get a connection to it.
        accumuloInstanceDriver = startAccumuloInstanceDriver();

        // Setup the Rya library to use the Accumulo instance.
//        ryaRepo = setupRya();
//        ryaConn = ryaRepo.getConnection();
//        RdfCloudTripleStore rdfCloudTripleStore = (RdfCloudTripleStore) ryaRepo.getSail();
//        accumuloRyaDao = (AccumuloRyaDAO) rdfCloudTripleStore.getRyaDAO();
//        accumuloRdfConfiguration = accumuloRyaDao.getConf();

        accumuloRdfConfiguration = accumuloInstanceDriver.getDao().getConf();
    }

    @Before
    public void setUpPerTest() throws Exception {
        accumuloInstanceDriver.setUpTables();
        accumuloInstanceDriver.setUpDao();
        accumuloInstanceDriver.setUpConfig();
    }

    @After
    public void tearDownPerTest() throws Exception {
        log.info("tearDownPerTest(): tearing down now.");
        accumuloInstanceDriver.tearDownTables();
        accumuloInstanceDriver.tearDownDao();
    }

    @AfterClass
    public static void tearDownPerClass() throws Exception {
        log.info("tearDownPerClass(): tearing down now.");
        accumuloInstanceDriver.tearDown();
    }

    @Test
    public void testInit() throws MergerException {
        AccumuloRyaStatementStore accumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloRdfConfiguration);
        accumuloRyaStatementStore.init();
        assertTrue(accumuloRyaStatementStore.isInitialized());
    }

    @Test(expected = MergerException.class)
    public void testInitAlreadyInitialized() throws MergerException {
        AccumuloRyaStatementStore accumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloRdfConfiguration);
        accumuloRyaStatementStore.init();
        assertTrue(accumuloRyaStatementStore.isInitialized());
        accumuloRyaStatementStore.init();
    }

    @Test
    public void testFetchStatements() throws MergerException {
        AccumuloRyaStatementStore accumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloRdfConfiguration);
        accumuloRyaStatementStore.init();

        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        accumuloRyaStatementStore.fetchStatements();
    }

    @Test (expected = MergerException.class)
    public void testFetchStatements_FetchWrongInstance() throws MergerException {
        AccumuloRyaStatementStore accumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloRdfConfiguration);
        accumuloRyaStatementStore.init();

        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        Configuration config = accumuloRyaStatementStore.getConfiguration();

        config.set(ConfigUtils.CLOUDBASE_INSTANCE, "wrong instance");

        accumuloRyaStatementStore.fetchStatements();
    }

    @Test
    public void testAddStatement() throws MergerException {
        AccumuloRyaStatementStore accumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloRdfConfiguration);
        accumuloRyaStatementStore.init();

        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }
    }

    @Test (expected = MergerException.class)
    public void testAddStatement_AddNull() throws Exception {
        AccumuloRyaStatementStore accumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloRdfConfiguration);
        accumuloRyaStatementStore.init();

        accumuloRyaStatementStore.addStatement(null);
    }

    @Test
    public void testRemoveStatement() throws MergerException {
        AccumuloRyaStatementStore accumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloRdfConfiguration);
        accumuloRyaStatementStore.init();

        // Add one then remove it right away
        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
            accumuloRyaStatementStore.removeStatement(ryaStatement);
            assertTrue(isStatementStoreEmpty(accumuloRyaStatementStore));
        }

        // Add all then remove all
        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }
        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.removeStatement(ryaStatement);
        }
        assertTrue(isStatementStoreEmpty(accumuloRyaStatementStore));

        // Add all then remove all in reverse order
        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }
        ImmutableList<RyaStatement> reverseList = RYA_STATEMENTS.reverse();
        for (RyaStatement ryaStatement : reverseList) {
            accumuloRyaStatementStore.removeStatement(ryaStatement);
        }
        assertTrue(isStatementStoreEmpty(accumuloRyaStatementStore));

        // Add all then remove one from middle follow by another before and
        // after the first removed one
        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        RyaStatement firstToRemove = RYA_STATEMENTS.get(2);
        RyaStatement before = RYA_STATEMENTS.get(1);
        RyaStatement after = RYA_STATEMENTS.get(3);

        accumuloRyaStatementStore.removeStatement(firstToRemove);
        accumuloRyaStatementStore.removeStatement(before);
        accumuloRyaStatementStore.removeStatement(after);
    }

    @Test (expected = MergerException.class)
    public void testRemoveStatement_RemoveNull() throws MergerException {
        AccumuloRyaStatementStore accumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloRdfConfiguration);
        accumuloRyaStatementStore.init();

        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        accumuloRyaStatementStore.removeStatement(null);
    }

    @Test
    public void testRemoveStatement_RemoveStatementNotFound() throws MergerException {
        AccumuloRyaStatementStore accumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloRdfConfiguration);
        accumuloRyaStatementStore.init();

        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        RyaStatement notFoundStatement = TestUtils.createRyaStatement("Statement", "not found", "here", DATE);
        accumuloRyaStatementStore.removeStatement(notFoundStatement);
    }

    @Test
    public void testUpdateStatement() throws MergerException {
        AccumuloRyaStatementStore accumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloRdfConfiguration);
        accumuloRyaStatementStore.init();

        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        RyaStatement firstRyaStatement = RYA_STATEMENTS.get(0);
        RyaStatement updatedRyaStatement = TestUtils.copyRyaStatement(firstRyaStatement);

        assertEquals(firstRyaStatement, updatedRyaStatement);

        String subject = TestUtils.convertRyaUriToString(updatedRyaStatement.getSubject());
        String predicate = TestUtils.convertRyaUriToString(updatedRyaStatement.getPredicate());
        updatedRyaStatement.setSubject(TestUtils.createRyaUri(subject + "_UPDATED"));
        updatedRyaStatement.setPredicate(TestUtils.createRyaUri(predicate + "_UPDATED"));

        accumuloRyaStatementStore.updateStatement(firstRyaStatement, updatedRyaStatement);

        Iterator<RyaStatement> ryaStatementsIterator = accumuloRyaStatementStore.fetchStatements();
        int originalCount = 0;
        int updatedCount = 0;
        int totalCount = 0;
        while (ryaStatementsIterator.hasNext()) {
            RyaStatement ryaStatement = ryaStatementsIterator.next();
            if (ryaStatement.equals(firstRyaStatement)) {
                originalCount++;
            }
            if (ryaStatement.equals(updatedRyaStatement)) {
                updatedCount++;
            }
            totalCount++;
        }

        assertEquals(0, originalCount);
        assertEquals(1, updatedCount);
        assertEquals(RYA_STATEMENTS.size(), totalCount);
    }

    @Test (expected = MergerException.class)
    public void testUpdateStatement_UpdateNull() throws MergerException {
        AccumuloRyaStatementStore accumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloRdfConfiguration);
        accumuloRyaStatementStore.init();

        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        RyaStatement firstRyaStatement = RYA_STATEMENTS.get(0);
        RyaStatement updatedRyaStatement = TestUtils.copyRyaStatement(firstRyaStatement);

        assertEquals(firstRyaStatement, updatedRyaStatement);

        String subject = TestUtils.convertRyaUriToString(updatedRyaStatement.getSubject());
        String predicate = TestUtils.convertRyaUriToString(updatedRyaStatement.getPredicate());
        updatedRyaStatement.setSubject(TestUtils.createRyaUri(subject + "_UPDATED"));
        updatedRyaStatement.setPredicate(TestUtils.createRyaUri(predicate + "_UPDATED"));

        accumuloRyaStatementStore.updateStatement(firstRyaStatement, null);
    }

    @Test
    public void testUpdateStatement_OriginalNotFound() throws MergerException {
        AccumuloRyaStatementStore accumuloRyaStatementStore = new AccumuloRyaStatementStore(accumuloRdfConfiguration);
        accumuloRyaStatementStore.init();

        for (RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        RyaStatement notFoundStatement = TestUtils.createRyaStatement("Statement", "not found", "here", DATE);
        RyaStatement updatedRyaStatement = TestUtils.copyRyaStatement(notFoundStatement);

        assertEquals(notFoundStatement, updatedRyaStatement);

        String subject = TestUtils.convertRyaUriToString(updatedRyaStatement.getSubject());
        String predicate = TestUtils.convertRyaUriToString(updatedRyaStatement.getPredicate());
        updatedRyaStatement.setSubject(TestUtils.createRyaUri(subject + "_UPDATED"));
        updatedRyaStatement.setPredicate(TestUtils.createRyaUri(predicate + "_UPDATED"));

        accumuloRyaStatementStore.updateStatement(notFoundStatement, updatedRyaStatement);

        Iterator<RyaStatement> ryaStatementsIterator = accumuloRyaStatementStore.fetchStatements();
        int originalCount = 0;
        int updatedCount = 0;
        int totalCount = 0;
        while (ryaStatementsIterator.hasNext()) {
            RyaStatement ryaStatement = ryaStatementsIterator.next();
            if (ryaStatement.equals(notFoundStatement)) {
                originalCount++;
            }
            if (ryaStatement.equals(updatedRyaStatement)) {
                updatedCount++;
            }
            totalCount++;
        }

        assertEquals(0, originalCount);
        assertEquals(1, updatedCount);
        assertEquals(RYA_STATEMENTS.size() + 1, totalCount);
    }



    @After
    public void shutdownMiniResources() {
//        if(ryaConn != null) {
//            try {
//                log.info("Shutting down Rya Connection.");
//                ryaConn.close();
//                log.info("Rya Connection shut down.");
//            } catch(final Exception e) {
//                log.error("Could not shut down the Rya Connection.", e);
//            }
//        }
//
//        if(ryaRepo != null) {
//            try {
//                log.info("Shutting down Rya Repo.");
//                ryaRepo.shutDown();
//                log.info("Rya Repo shut down.");
//            } catch(final Exception e) {
//                log.error("Could not shut down the Rya Repo.", e);
//            }
//        }

        if(accumuloInstanceDriver != null) {
            try {
                log.info("Shutting down the Mini Accumulo being used as a Rya store.");
                accumuloInstanceDriver.tearDown();
                log.info("Mini Accumulo being used as a Rya store shut down.");
            } catch(final Exception e) {
                log.error("Could not shut down the Mini Accumulo.", e);
            }
        }
    }

    private static boolean isStatementStoreEmpty(AccumuloRyaStatementStore accumuloRyaStatementStore) throws MergerException {
        Iterator<RyaStatement> iterator = accumuloRyaStatementStore.fetchStatements();
        return !iterator.hasNext();
    }

    /**
     * Setup a Accumulo instance driver to run the test. Establishes the
     * connector to the Accumulo instance.
     * @return an {@link AccumuloInstanceDriver}.
     * @throws Exception
     */
    private static AccumuloInstanceDriver startAccumuloInstanceDriver() throws Exception {
        AccumuloInstanceDriver accumuloInstanceDriver = new AccumuloInstanceDriver("Test Driver", INSTANCE_TYPE, true, false, true, USER_NAME, PASSWORD, INSTANCE_NAME, RYA_TABLE_PREFIX, AUTHS);
        accumuloInstanceDriver.setUp();

        //accumuloConn = accumuloInstanceDriver.getConnector();

        return accumuloInstanceDriver;
    }

    /**
     * Format an Accumulo instance to be a Rya repository.
     *
     * @return The Rya repository sitting on top of the Accumulo instance.
     */
//    private RyaSailRepository setupRya() throws AccumuloException, AccumuloSecurityException, RepositoryException {
//        // Setup the Rya Repository that will be used to create Repository Connections.
//        final RdfCloudTripleStore ryaStore = new RdfCloudTripleStore();
//        final AccumuloRyaDAO accumuloRyaDao = new AccumuloRyaDAO();
//        accumuloRyaDao.setConnector(accumuloConn);
//
//        // Setup Rya configuration values.
//        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
//        conf.setTablePrefix(RYA_TABLE_PREFIX);
//        conf.setDisplayQueryPlan(true);
//
//        conf.setBoolean(USE_MOCK_INSTANCE, IS_MOCK);
//        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, RYA_TABLE_PREFIX);
//        conf.set(CLOUDBASE_USER, USER_NAME);
//        conf.set(CLOUDBASE_PASSWORD, PASSWORD);
//        conf.set(CLOUDBASE_INSTANCE, INSTANCE_NAME);
//
//        accumuloRyaDao.setConf(conf);
//        ryaStore.setRyaDAO(accumuloRyaDao);
//
//        final RyaSailRepository ryaRepo = new RyaSailRepository(ryaStore);
//        ryaRepo.initialize();
//
//        return ryaRepo;
//    }
}
