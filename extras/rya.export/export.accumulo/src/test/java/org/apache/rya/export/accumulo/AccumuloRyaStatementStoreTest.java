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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rya.export.MergePolicy;
import org.apache.rya.export.accumulo.common.InstanceType;
import org.apache.rya.export.accumulo.conf.AccumuloExportConstants;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;
import org.apache.rya.export.api.MergerException;
import org.apache.rya.export.api.conf.AccumuloMergeConfiguration;
import org.apache.rya.export.api.store.AddStatementException;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RemoveStatementException;
import org.apache.rya.export.api.store.UpdateStatementException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.indexing.accumulo.ConfigUtils;

/**
 * Tests the methods of {@link AccumuloRyaStatementStore}.
 */
public class AccumuloRyaStatementStoreTest {
    private static final Logger log = LogManager.getLogger(AccumuloRyaStatementStoreTest.class);
    private static final InstanceType INSTANCE_TYPE = InstanceType.MOCK;

    private static final boolean IS_MOCK = INSTANCE_TYPE.isMock();
    private static final String USER_NAME = IS_MOCK ? "test_user" : AccumuloInstanceDriver.ROOT_USER_NAME;
    private static final String PASSWORD = "password";
    private static final String INSTANCE_NAME = "test_instance";
    private static final String AUTHS = "test_auth";
    private static final String RYA_TABLE_PREFIX = "test_";
    private static final String ZOOKEEPERS = "localhost";

    // Rya data store and connections.
    private static AccumuloInstanceDriver accumuloInstanceDriver = null;

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
        // Initialize the Accumulo instance that will be used to store Triples and get a connection to it.
        accumuloInstanceDriver = startAccumuloInstanceDriver();
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
    public void testFetchStatements() throws MergerException {
        final AccumuloRyaStatementStore accumuloRyaStatementStore = createAccumuloRyaStatementStore();

        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        accumuloRyaStatementStore.fetchStatements();
    }

    @Test (expected = FetchStatementException.class)
    public void testFetchStatements_FetchWrongInstance() throws MergerException {
        final AccumuloRyaStatementStore accumuloRyaStatementStore = createAccumuloRyaStatementStore();

        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        final Configuration config = accumuloRyaStatementStore.getRyaDAO().getConf();

        config.set(ConfigUtils.CLOUDBASE_INSTANCE, "wrong instance");

        accumuloRyaStatementStore.fetchStatements();
    }

    @Test
    public void testAddStatement() throws MergerException {
        final AccumuloRyaStatementStore accumuloRyaStatementStore = createAccumuloRyaStatementStore();

        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }
    }

    @Test (expected = AddStatementException.class)
    public void testAddStatement_AddNull() throws Exception {
        final AccumuloRyaStatementStore accumuloRyaStatementStore = createAccumuloRyaStatementStore();

        accumuloRyaStatementStore.addStatement(null);
    }

    @Test
    public void testRemoveStatement() throws MergerException {
        final AccumuloRyaStatementStore accumuloRyaStatementStore = createAccumuloRyaStatementStore();

        // Add one then remove it right away
        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
            accumuloRyaStatementStore.removeStatement(ryaStatement);
            assertTrue(isStatementStoreEmpty(accumuloRyaStatementStore));
        }

        // Add all then remove all
        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }
        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.removeStatement(ryaStatement);
        }
        assertTrue(isStatementStoreEmpty(accumuloRyaStatementStore));

        // Add all then remove all in reverse order
        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }
        final ImmutableList<RyaStatement> reverseList = RYA_STATEMENTS.reverse();
        for (final RyaStatement ryaStatement : reverseList) {
            accumuloRyaStatementStore.removeStatement(ryaStatement);
        }
        assertTrue(isStatementStoreEmpty(accumuloRyaStatementStore));

        // Add all then remove one from middle follow by another before and
        // after the first removed one
        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        final RyaStatement firstToRemove = RYA_STATEMENTS.get(2);
        final RyaStatement before = RYA_STATEMENTS.get(1);
        final RyaStatement after = RYA_STATEMENTS.get(3);

        accumuloRyaStatementStore.removeStatement(firstToRemove);
        accumuloRyaStatementStore.removeStatement(before);
        accumuloRyaStatementStore.removeStatement(after);
    }

    @Test (expected = RemoveStatementException.class)
    public void testRemoveStatement_RemoveNull() throws MergerException {
        final AccumuloRyaStatementStore accumuloRyaStatementStore = createAccumuloRyaStatementStore();

        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        accumuloRyaStatementStore.removeStatement(null);
    }

    @Test
    public void testRemoveStatement_RemoveStatementNotFound() throws MergerException {
        final AccumuloRyaStatementStore accumuloRyaStatementStore = createAccumuloRyaStatementStore();

        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        final RyaStatement notFoundStatement = TestUtils.createRyaStatement("Statement", "not found", "here", DATE);
        accumuloRyaStatementStore.removeStatement(notFoundStatement);
    }

    @Test
    public void testUpdateStatement() throws MergerException {
        final AccumuloRyaStatementStore accumuloRyaStatementStore = createAccumuloRyaStatementStore();

        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        final RyaStatement firstRyaStatement = RYA_STATEMENTS.get(0);
        final RyaStatement updatedRyaStatement = TestUtils.copyRyaStatement(firstRyaStatement);

        assertEquals(firstRyaStatement, updatedRyaStatement);

        final String subject = TestUtils.convertRyaUriToString(updatedRyaStatement.getSubject());
        final String predicate = TestUtils.convertRyaUriToString(updatedRyaStatement.getPredicate());
        updatedRyaStatement.setSubject(TestUtils.createRyaUri(subject + "_UPDATED"));
        updatedRyaStatement.setPredicate(TestUtils.createRyaUri(predicate + "_UPDATED"));

        accumuloRyaStatementStore.updateStatement(firstRyaStatement, updatedRyaStatement);

        final Iterator<RyaStatement> ryaStatementsIterator = accumuloRyaStatementStore.fetchStatements();
        int originalCount = 0;
        int updatedCount = 0;
        int totalCount = 0;
        while (ryaStatementsIterator.hasNext()) {
            final RyaStatement ryaStatement = ryaStatementsIterator.next();
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

    @Test (expected = UpdateStatementException.class)
    public void testUpdateStatement_UpdateNull() throws MergerException {
        final AccumuloRyaStatementStore accumuloRyaStatementStore = createAccumuloRyaStatementStore();

        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        final RyaStatement firstRyaStatement = RYA_STATEMENTS.get(0);
        final RyaStatement updatedRyaStatement = TestUtils.copyRyaStatement(firstRyaStatement);

        assertEquals(firstRyaStatement, updatedRyaStatement);

        final String subject = TestUtils.convertRyaUriToString(updatedRyaStatement.getSubject());
        final String predicate = TestUtils.convertRyaUriToString(updatedRyaStatement.getPredicate());
        updatedRyaStatement.setSubject(TestUtils.createRyaUri(subject + "_UPDATED"));
        updatedRyaStatement.setPredicate(TestUtils.createRyaUri(predicate + "_UPDATED"));

        accumuloRyaStatementStore.updateStatement(firstRyaStatement, null);
    }

    @Test
    public void testUpdateStatement_OriginalNotFound() throws MergerException {
        final AccumuloRyaStatementStore accumuloRyaStatementStore = createAccumuloRyaStatementStore();

        for (final RyaStatement ryaStatement : RYA_STATEMENTS) {
            accumuloRyaStatementStore.addStatement(ryaStatement);
        }

        final RyaStatement notFoundStatement = TestUtils.createRyaStatement("Statement", "not found", "here", DATE);
        final RyaStatement updatedRyaStatement = TestUtils.copyRyaStatement(notFoundStatement);

        assertEquals(notFoundStatement, updatedRyaStatement);

        final String subject = TestUtils.convertRyaUriToString(updatedRyaStatement.getSubject());
        final String predicate = TestUtils.convertRyaUriToString(updatedRyaStatement.getPredicate());
        updatedRyaStatement.setSubject(TestUtils.createRyaUri(subject + "_UPDATED"));
        updatedRyaStatement.setPredicate(TestUtils.createRyaUri(predicate + "_UPDATED"));

        accumuloRyaStatementStore.updateStatement(notFoundStatement, updatedRyaStatement);

        final Iterator<RyaStatement> ryaStatementsIterator = accumuloRyaStatementStore.fetchStatements();
        int originalCount = 0;
        int updatedCount = 0;
        int totalCount = 0;
        while (ryaStatementsIterator.hasNext()) {
            final RyaStatement ryaStatement = ryaStatementsIterator.next();
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

    private static boolean isStatementStoreEmpty(final AccumuloRyaStatementStore accumuloRyaStatementStore) throws MergerException {
        final Iterator<RyaStatement> iterator = accumuloRyaStatementStore.fetchStatements();
        return !iterator.hasNext();
    }

    /**
     * Setup a Accumulo instance driver to run the test. Establishes the
     * connector to the Accumulo instance.
     * @return an {@link AccumuloInstanceDriver}.
     * @throws Exception
     */
    private static AccumuloInstanceDriver startAccumuloInstanceDriver() throws Exception {
        final AccumuloInstanceDriver accumuloInstanceDriver = new AccumuloInstanceDriver("Test Driver", INSTANCE_TYPE, true, false, true, USER_NAME, PASSWORD, INSTANCE_NAME, RYA_TABLE_PREFIX, AUTHS, ZOOKEEPERS);
        accumuloInstanceDriver.setUp();

        return accumuloInstanceDriver;
    }

    private static AccumuloMergeConfiguration createAccumuloMergeConfiguration() {
        final AccumuloMergeConfiguration accumuloMergeConfiguration = mock(AccumuloMergeConfiguration.class);

        when(accumuloMergeConfiguration.getParentRyaInstanceName()).thenReturn(INSTANCE_NAME);
        when(accumuloMergeConfiguration.getParentUsername()).thenReturn(USER_NAME);
        when(accumuloMergeConfiguration.getParentPassword()).thenReturn(PASSWORD);
        when(accumuloMergeConfiguration.getParentInstanceType()).thenReturn(INSTANCE_TYPE);
        when(accumuloMergeConfiguration.getParentTablePrefix()).thenReturn(RYA_TABLE_PREFIX);
        when(accumuloMergeConfiguration.getParentAuths()).thenReturn(AUTHS);

        // Other
        when(accumuloMergeConfiguration.getMergePolicy()).thenReturn(MergePolicy.TIMESTAMP);
        when(accumuloMergeConfiguration.getToolStartTime()).thenReturn(AccumuloExportConstants.convertDateToStartTimeString(new Date()));

        return accumuloMergeConfiguration;
    }

    private static AccumuloRyaStatementStore createAccumuloRyaStatementStore() throws MergerException {
        final AccumuloMergeConfiguration accumuloMergeConfiguration = createAccumuloMergeConfiguration();
        return createAccumuloRyaStatementStore(accumuloMergeConfiguration);
    }

    private static AccumuloRyaStatementStore createAccumuloRyaStatementStore(final AccumuloMergeConfiguration accumuloMergeConfiguration) throws MergerException {
        final String instance = accumuloMergeConfiguration.getParentRyaInstanceName();
        final String username = accumuloMergeConfiguration.getParentUsername();
        final String password = accumuloMergeConfiguration.getParentPassword();
        final InstanceType instanceType = accumuloMergeConfiguration.getParentInstanceType();
        final String tablePrefix = accumuloMergeConfiguration.getParentTablePrefix();
        final String auths = accumuloMergeConfiguration.getParentAuths();
        final String zooKeepers = accumuloMergeConfiguration.getParentZookeepers();

        return new AccumuloRyaStatementStore(instance, username, password, instanceType, tablePrefix, auths, zooKeepers);
    }
}
