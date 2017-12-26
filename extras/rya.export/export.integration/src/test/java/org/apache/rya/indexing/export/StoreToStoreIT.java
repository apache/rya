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
package org.apache.rya.indexing.export;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.export.InstanceType;
import org.apache.rya.export.accumulo.AccumuloRyaStatementStore;
import org.apache.rya.export.accumulo.policy.TimestampPolicyAccumuloRyaStatementStore;
import org.apache.rya.export.accumulo.util.AccumuloInstanceDriver;
import org.apache.rya.export.api.metadata.ParentMetadataDoesNotExistException;
import org.apache.rya.export.api.store.AddStatementException;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.merge.MemoryTimeMerger;
import org.apache.rya.export.client.merge.VisibilityStatementMerger;
import org.apache.rya.export.mongo.MongoRyaStatementStore;
import org.apache.rya.export.mongo.policy.TimestampPolicyMongoRyaStatementStore;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.mongodb.MongoClient;

@RunWith(Parameterized.class)
public class StoreToStoreIT extends ITBase {
    private static final String RYA_INSTANCE = "ryaInstance";
    private static final InstanceType type = InstanceType.MOCK;
    private static final String tablePrefix = "accumuloTest";
    private static final String auths = "U";

    private final RyaStatementStore parentStore;
    private final RyaStatementStore childStore;
    private final static List<MongoClient> clients = new ArrayList<>();
    private final static List<AccumuloInstanceDriver> drivers = new ArrayList<>();
    private static Date currentDate;

    private static TimestampPolicyMongoRyaStatementStore getParentMongo() throws Exception {
        final MongoClient mongo = getNewMongoResources(RYA_INSTANCE);
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        dao.setConf(new StatefulMongoDBRdfConfiguration(ITBase.getConf(mongo), mongo, new ArrayList<>()));
        dao.init();
        final MongoRyaStatementStore store = new MongoRyaStatementStore(mongo, RYA_INSTANCE, dao);
        final TimestampPolicyMongoRyaStatementStore timeStore = new TimestampPolicyMongoRyaStatementStore(store, currentDate, RYA_INSTANCE);
        clients.add(mongo);
        return timeStore;
    }

    private static MongoRyaStatementStore getChildMongo() throws Exception {
        final MongoClient mongo = getNewMongoResources(RYA_INSTANCE);
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        dao.setConf(new StatefulMongoDBRdfConfiguration(ITBase.getConf(mongo), mongo, new ArrayList<>()));
        dao.init();
        final MongoRyaStatementStore store = new MongoRyaStatementStore(mongo, RYA_INSTANCE, dao);
        clients.add(mongo);
        return store;
    }

    private static TimestampPolicyAccumuloRyaStatementStore getParentAccumulo() throws Exception {
        final AccumuloInstanceDriver driver = new AccumuloInstanceDriver(RYA_INSTANCE, type, true, false, true, "TEST1", PASSWORD, RYA_INSTANCE, tablePrefix, auths, "");
        driver.setUp();
        final AccumuloRyaStatementStore store = new AccumuloRyaStatementStore(driver.getDao(), tablePrefix, RYA_INSTANCE);
        drivers.add(driver);
        return new TimestampPolicyAccumuloRyaStatementStore(store, currentDate);
    }

    private static AccumuloRyaStatementStore getChildAccumulo() throws Exception {
        final AccumuloInstanceDriver driver = new AccumuloInstanceDriver(RYA_INSTANCE, type, true, false, false, "TEST2", PASSWORD, RYA_INSTANCE+"_child", tablePrefix, auths, "");
        driver.setUp();
        drivers.add(driver);
        return new AccumuloRyaStatementStore(driver.getDao(), tablePrefix, RYA_INSTANCE);
    }

    @Before
    public void clearDBS() throws Exception {
        for(final AccumuloInstanceDriver driver : drivers) {
            driver.setUpInstance();
            driver.setUpTables();
            driver.setUpDao();
            driver.setUpConfig();
        }
    }

    @After
    public void cleanupTables() throws Exception {
        for(final AccumuloInstanceDriver driver : drivers) {
            driver.tearDown();
        }
        for(final MongoClient client : clients) {
            client.dropDatabase(RYA_INSTANCE);
        }
    }

    @AfterClass
    public static void shutdown() throws Exception {
        for(final AccumuloInstanceDriver driver : drivers) {
            driver.tearDown();
        }
        for(final MongoClient client : clients) {
            client.close();
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> instancesToTest() throws Exception {
        currentDate = new Date();
        final Collection<Object[]> stores = new ArrayList<>();
        stores.add(new Object[]{getParentMongo(), getChildMongo()});
        stores.add(new Object[]{getParentMongo(), getChildAccumulo()});
        stores.add(new Object[]{getParentAccumulo(), getChildMongo()});
        stores.add(new Object[]{getParentAccumulo(), getChildAccumulo()});
        return stores;
    }

    public StoreToStoreIT(final RyaStatementStore parentStore,
            final RyaStatementStore childStore) {
        this.parentStore = parentStore;
        this.childStore = childStore;
    }

    @Test
    public void cloneTest() throws AddStatementException, FetchStatementException, ParentMetadataDoesNotExistException {
        loadMockStatements(parentStore, 50, new Date(currentDate.getTime() + 10000L));

        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
            new VisibilityStatementMerger(), currentDate, RYA_INSTANCE, 0L);
        merger.runJob();
        assertEquals(50, count(childStore));
    }

    @Test
    public void no_statementsTest() throws AddStatementException, FetchStatementException {
        loadMockStatements(parentStore, 50, new Date(0L));

        assertEquals(0, count(childStore));
        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
                new VisibilityStatementMerger(), currentDate, RYA_INSTANCE, 0L);
        merger.runJob();
        assertEquals(0, count(childStore));
    }

    @Test
    public void childToParent_ChildAddTest() throws AddStatementException, FetchStatementException {
        loadMockStatements(parentStore, 50, new Date(currentDate.getTime() + 100L));

        //setup child
        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
            new VisibilityStatementMerger(), currentDate, RYA_INSTANCE, 0L);
        merger.runJob();

        //add a few statements to child
        final RyaStatement stmnt1 = makeRyaStatement("http://subject", "http://predicate", "http://51");
        final RyaStatement stmnt2 = makeRyaStatement("http://subject", "http://predicate", "http://52");
        childStore.addStatement(stmnt1);
        childStore.addStatement(stmnt2);

        final MemoryTimeMerger otherMerger = new MemoryTimeMerger(childStore, parentStore,
             new VisibilityStatementMerger(), currentDate, RYA_INSTANCE, 0L);
        otherMerger.runJob();
        assertEquals(52, count(parentStore));
    }

    @Test
    public void childToParent_ChildReAddsDeletedStatementTest() throws Exception {
        loadMockStatements(parentStore, 50, new Date(currentDate.getTime() + 10000L));

        //setup child
        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
            new VisibilityStatementMerger(), currentDate, RYA_INSTANCE, 0L);
        merger.runJob();

        //remove a statement from the parent
        final RyaStatement stmnt1 = makeRyaStatement("http://subject", "http://predicate", "http://1");
        parentStore.removeStatement(stmnt1);
        assertEquals(49, count(parentStore));

        assertFalse(parentStore.containsStatement(stmnt1));

        final MemoryTimeMerger otherMerger = new MemoryTimeMerger(childStore, parentStore,
            new VisibilityStatementMerger(), currentDate, RYA_INSTANCE, 0L);
        otherMerger.runJob();

        //merging will have added the statement back
        assertEquals(50, count(parentStore));
    }

    @Test
    public void childToParent_BothAddTest() throws Exception {
        loadMockStatements(parentStore, 50, new Date(currentDate.getTime() + 10000L));

        assertEquals(0, count(childStore));
        final MemoryTimeMerger merger = new MemoryTimeMerger(parentStore, childStore,
            new VisibilityStatementMerger(), currentDate, RYA_INSTANCE, 0L);
        merger.runJob();


        assertEquals(50, count(parentStore));
        assertEquals(50, count(childStore));

        //add a statement to each store
        final RyaStatement stmnt1 = makeRyaStatement("http://subject", "http://predicate", "http://add");
        final RyaStatement stmnt2 = makeRyaStatement("http://subject", "http://predicate", "http://add2");
        stmnt1.setTimestamp(new Date().getTime() + 10L);
        stmnt2.setTimestamp(currentDate.getTime() + 1000L);
        parentStore.addStatement(stmnt1);
        childStore.addStatement(stmnt2);

        final MemoryTimeMerger otherMerger = new MemoryTimeMerger(childStore, parentStore,
                new VisibilityStatementMerger(), currentDate, RYA_INSTANCE, 0L);
        otherMerger.runJob();
        //both should still be there
        assertEquals(52, count(parentStore));
    }

    private void loadMockStatements(final RyaStatementStore store, final int count, final Date timestamp) throws AddStatementException {
        for(int ii = 0; ii < count; ii++) {
            final RyaStatement statement = makeRyaStatement("http://subject", "http://predicate", "http://"+ii);
            statement.setTimestamp(timestamp.getTime());
            parentStore.addStatement(statement);
        }
    }

    private int count(final RyaStatementStore store) throws FetchStatementException {
        final Iterator<RyaStatement> statements = store.fetchStatements();
        int count = 0;
        while(statements.hasNext()) {
            final RyaStatement statement = statements.next();
            System.out.println(statement.getObject().getData() + "     " + statement.getTimestamp());
            count++;
        }
        return count;
    }
}
