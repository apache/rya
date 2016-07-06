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
package org.apache.rya.indexing.pcj.fluo.visibility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.fluo.api.client.Snapshot;
import io.fluo.api.data.Bytes;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RyaTypeResolverException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;
import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaExportParameters;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.ShiftVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.junit.Test;
import org.openrdf.model.impl.NumericLiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryException;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

public class PcjVisibilityIT extends ITBase {

    private static final AccumuloPcjSerializer converter = new AccumuloPcjSerializer();
    /**
     * Configure the export observer to use the Mini Accumulo instance as the
     * export destination for new PCJ results.
     */
    @Override
    protected Map<String, String> makeExportParams() {
        final HashMap<String, String> params = new HashMap<>();

        final RyaExportParameters ryaParams = new RyaExportParameters(params);
        ryaParams.setExportToRya(true);
        ryaParams.setAccumuloInstanceName(instanceName);
        ryaParams.setZookeeperServers(zookeepers);
        ryaParams.setExporterUsername(ITBase.ACCUMULO_USER);
        ryaParams.setExporterPassword(ITBase.ACCUMULO_PASSWORD);

        return params;
    }

    @Test
    public void createWithVisibilityDirect() throws RepositoryException, PcjException, TableNotFoundException, RyaTypeResolverException, AccumuloException, AccumuloSecurityException, BindingSetConversionException {
        // Create a PCJ table that will include those triples in its results.
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        final String pcjTableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "testPcj");

        // Create and populate the PCJ table.
        final PcjTables pcjs = new PcjTables();
        final PcjVarOrderFactory varOrderFactory = Optional.<PcjVarOrderFactory>absent().or(new ShiftVarOrderFactory());
        final Set<VariableOrder> varOrders = varOrderFactory.makeVarOrders( new VariableOrder(new String[]{"name", "age"}) );

        // Create the PCJ table in Accumulo.
        pcjs.createPcjTable(accumuloConn, pcjTableName, varOrders, sparql);
        setupTestUsers(pcjTableName);

        // Add a few results to the PCJ table.
        final MapBindingSet alice = new MapBindingSet();
        alice.addBinding("name", new URIImpl("http://Alice"));
        alice.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));

        final MapBindingSet bob = new MapBindingSet();
        bob.addBinding("name", new URIImpl("http://Bob"));
        bob.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));

        final MapBindingSet charlie = new MapBindingSet();
        charlie.addBinding("name", new URIImpl("http://Charlie"));
        charlie.addBinding("age", new NumericLiteralImpl(12, XMLSchema.INTEGER));

        final VisibilityBindingSet aliceVisibility = new VisibilityBindingSet(alice, "A&B&C");
        final VisibilityBindingSet bobVisibility = new VisibilityBindingSet(bob, "B&C");
        final VisibilityBindingSet charlieVisibility = new VisibilityBindingSet(charlie, "C");

        final Set<BindingSet> results = Sets.<BindingSet>newHashSet(alice, bob, charlie);
        final Set<VisibilityBindingSet> visibilityResults = Sets.<VisibilityBindingSet>newHashSet(aliceVisibility, bobVisibility, charlieVisibility);
        // Load historic matches from Rya into the PCJ table.
        pcjs.addResults(accumuloConn, pcjTableName, visibilityResults);

        // Make sure the cardinality was updated.
        final PcjMetadata metadata = pcjs.getPcjMetadata(accumuloConn, pcjTableName);
        assertEquals(3, metadata.getCardinality());

        // Scan Accumulo for the stored results.
        Multimap<String, BindingSet> fetchedResults = loadPcjResults(accumuloConn, pcjTableName, "A", "B", "C");
        assertEquals(getExpectedResults(results), fetchedResults);
        fetchedResults = loadPcjResults(accumuloConn, pcjTableName, "C", "B");
        assertEquals(getExpectedResults(Sets.<BindingSet>newHashSet(bob, charlie)), fetchedResults);
        fetchedResults = loadPcjResults(accumuloConn, pcjTableName, "C");
        assertEquals(getExpectedResults(Sets.<BindingSet>newHashSet(charlie)), fetchedResults);

        final Connector cConn = accumuloConn.getInstance().getConnector("cUser", new PasswordToken("password"));
        // Scan Accumulo for the stored results.
        fetchedResults = loadPcjResults(cConn, pcjTableName, "C");
        assertEquals(getExpectedResults(Sets.<BindingSet>newHashSet(charlie)), fetchedResults);
        fetchedResults = loadPcjResults(cConn, pcjTableName);
        assertEquals(getExpectedResults(Sets.<BindingSet>newHashSet(charlie)), fetchedResults);

        final Connector noAuthConn = accumuloConn.getInstance().getConnector("noAuth", new PasswordToken("password"));
        // Scan Accumulo for the stored results.
        fetchedResults = loadPcjResults(noAuthConn, pcjTableName);
        assertTrue(fetchedResults.isEmpty());
    }

    @Test
    public void createWithVisibilityFluo() throws Exception {
        final String sparql =
                "SELECT ?customer ?worker ?city " +
                "{ " +
                  "?customer <http://talksTo> ?worker. " +
                  "?worker <http://livesIn> ?city. " +
                  "?worker <http://worksAt> <http://Chipotle>. " +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Map<RyaStatement, String> streamedTriples = new HashMap<>();
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Alice", "http://talksTo", "http://Bob"), "A&B");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Bob", "http://livesIn", "http://London"), "A");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Bob", "http://worksAt", "http://Chipotle"), "B");

        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Alice", "http://talksTo", "http://Charlie"), "B&C");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Charlie", "http://livesIn", "http://London"), "B");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Charlie", "http://worksAt", "http://Chipotle"), "C");

        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Alice", "http://talksTo", "http://David"), "C&D");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://David", "http://livesIn", "http://London"), "C");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://David", "http://worksAt", "http://Chipotle"), "D");

        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Alice", "http://talksTo", "http://Eve"), "D&E");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Eve", "http://livesIn", "http://Leeds"), "D");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Eve", "http://worksAt", "http://Chipotle"), "E");

        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Frank", "http://talksTo", "http://Alice"), "");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Frank", "http://livesIn", "http://London"), "");
        addStatementVisibilityEntry(streamedTriples, makeRyaStatement("http://Frank", "http://worksAt", "http://Chipotle"), "");

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, new HashSet<VariableOrder>(), sparql);

        // Stream the data into Fluo.
        for(final RyaStatement statement : streamedTriples.keySet()) {
            new InsertTriples().insert(fluoClient, statement, Optional.of(streamedTriples.get(statement)));
        }

        // Fetch the exported results from Accumulo once the observers finish working.
        fluo.waitForObservers();

        // Fetch expected results from the PCJ table that is in Accumulo.
        final String exportTableName;
        try(Snapshot snapshot = fluoClient.newSnapshot()) {
            final Bytes queryId = snapshot.get(Bytes.of(sparql), FluoQueryColumns.QUERY_ID);
            exportTableName = snapshot.get(queryId, FluoQueryColumns.QUERY_RYA_EXPORT_TABLE_NAME).toString();
        }
        setupTestUsers(exportTableName);
        Multimap<String, BindingSet> results = loadPcjResults(accumuloConn, exportTableName);

        // Verify the end results of the query match the expected results.
        Multimap<String, BindingSet> expected = makeExpected(
            makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Bob")),
                new BindingImpl("city", new URIImpl("http://London"))),
            makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Charlie")),
                new BindingImpl("city", new URIImpl("http://London"))),
            makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Eve")),
                new BindingImpl("city", new URIImpl("http://Leeds"))),
            makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://David")),
                new BindingImpl("city", new URIImpl("http://London"))));

        assertEquals(expected,  results);

        final PasswordToken pass = new PasswordToken("password");
        Connector userConn = accumuloConn.getInstance().getConnector("abUser", pass);
        results = loadPcjResults(userConn, exportTableName);
        // Verify the end results of the query match the expected results.
        expected = makeExpected(
            makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Bob")),
                new BindingImpl("city", new URIImpl("http://London"))));
        assertEquals(expected,  results);

        userConn = accumuloConn.getInstance().getConnector("abcUser", pass);
        results = loadPcjResults(userConn, exportTableName);
        expected = makeExpected(
                makeBindingSet(
                        new BindingImpl("customer", new URIImpl("http://Alice")),
                        new BindingImpl("worker", new URIImpl("http://Bob")),
                        new BindingImpl("city", new URIImpl("http://London"))),
                makeBindingSet(
                        new BindingImpl("customer", new URIImpl("http://Alice")),
                        new BindingImpl("worker", new URIImpl("http://Charlie")),
                        new BindingImpl("city", new URIImpl("http://London"))));
        assertEquals(expected,  results);

        userConn = accumuloConn.getInstance().getConnector("adeUser", pass);
        results = loadPcjResults(userConn, exportTableName);
        expected = makeExpected(
                makeBindingSet(
                    new BindingImpl("customer", new URIImpl("http://Alice")),
                    new BindingImpl("worker", new URIImpl("http://Eve")),
                    new BindingImpl("city", new URIImpl("http://Leeds"))));
        assertEquals(expected,  results);

        userConn = accumuloConn.getInstance().getConnector("noAuth", pass);
        results = loadPcjResults(userConn, exportTableName);
        assertTrue(results.isEmpty());
    }

    private Multimap<String, BindingSet> makeExpected(final BindingSet... bindingSets) {
        final Set<BindingSet> expectedResults = new HashSet<>();
        for(final BindingSet bs : bindingSets) {
            expectedResults.add(bs);
        }

        final Multimap<String, BindingSet> expected = HashMultimap.create();
        expected.putAll("customer;worker;city", expectedResults);
        expected.putAll("worker;city;customer", expectedResults);
        expected.putAll("city;customer;worker", expectedResults);
        return expected;
    }

    private void setupTestUsers(final String exportTableName) throws AccumuloException, AccumuloSecurityException {
        final PasswordToken pass = new PasswordToken("password");
        final SecurityOperations secOps = accumuloConn.securityOperations();
        secOps.changeUserAuthorizations("root", new Authorizations("A", "B", "C", "D", "E"));
        secOps.createLocalUser("abUser", pass);
        secOps.changeUserAuthorizations("abUser", new Authorizations("A", "B"));
        secOps.grantTablePermission("abUser", exportTableName, TablePermission.READ);

        secOps.createLocalUser("abcUser", pass);
        secOps.changeUserAuthorizations("abcUser", new Authorizations("A", "B", "C"));
        secOps.grantTablePermission("abcUser", exportTableName, TablePermission.READ);

        secOps.createLocalUser("adeUser", pass);
        secOps.changeUserAuthorizations("adeUser", new Authorizations("A", "D", "E"));
        secOps.grantTablePermission("adeUser", exportTableName, TablePermission.READ);

        secOps.createLocalUser("cUser", pass);
        secOps.changeUserAuthorizations("cUser", new Authorizations("C"));
        secOps.grantTablePermission("cUser", exportTableName, TablePermission.READ);

        secOps.createLocalUser("noAuth", pass);
        secOps.changeUserAuthorizations("noAuth", new Authorizations());
        secOps.grantTablePermission("noAuth", exportTableName, TablePermission.READ);
    }


    private Multimap<String, BindingSet> getExpectedResults(final Set<BindingSet> results) {
        final Multimap<String, BindingSet> expectedResults = HashMultimap.create();
        expectedResults.putAll("name;age", results);
        expectedResults.putAll("age;name", results);
        return expectedResults;
    }

    protected static void addStatementVisibilityEntry(final Map<RyaStatement, String> triplesMap, final RyaStatement statement, final String visibility) {
        triplesMap.put(statement, visibility);
    }

    /**
     * Scan accumulo for the results that are stored in a PCJ table. The
     * multimap stores a set of deserialized binding sets that were in the PCJ
     * table for every variable order that is found in the PCJ metadata.
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     */
    private static Multimap<String, BindingSet> loadPcjResults(final Connector accumuloConn, final String pcjTableName, final String... visibility) throws PcjException, TableNotFoundException, BindingSetConversionException, AccumuloException, AccumuloSecurityException {
        final Multimap<String, BindingSet> fetchedResults = HashMultimap.create();

        final Authorizations userAuths;
        if(visibility.length == 0) {
            userAuths = accumuloConn.securityOperations().getUserAuthorizations(accumuloConn.whoami());
        } else {
            userAuths = new Authorizations(visibility);
        }

        // Get the variable orders the data was written to.
        final PcjTables pcjs = new PcjTables();
        final PcjMetadata pcjMetadata = pcjs.getPcjMetadata(accumuloConn, pcjTableName);

        // Scan Accumulo for the stored results.
        for(final VariableOrder varOrder : pcjMetadata.getVarOrders()) {
            final Scanner scanner = accumuloConn.createScanner(pcjTableName, userAuths);
            scanner.fetchColumnFamily( new Text(varOrder.toString()) );

            for(final Entry<Key, Value> entry : scanner) {
                final byte[] serializedResult = entry.getKey().getRow().getBytes();
                final BindingSet result = converter.convert(serializedResult, varOrder);
                fetchedResults.put(varOrder.toString(), result);
            }
        }

        return fetchedResults;
    }
}
