/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.pcj.storage.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.zookeeper.ClientCnxn;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.NumericLiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.MiniAccumuloClusterInstance;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;

/**
 * Performs integration test using {@link MiniAccumuloCluster} to ensure the
 * functions of {@link PcjTables} work within a cluster setting.
 */
public class PcjTablesIntegrationTest {

    private static final String USE_MOCK_INSTANCE = ".useMockInstance";
    private static final String CLOUDBASE_INSTANCE = "sc.cloudbase.instancename";
    private static final String CLOUDBASE_USER = "sc.cloudbase.username";
    private static final String CLOUDBASE_PASSWORD = "sc.cloudbase.password";

    private static final AccumuloPcjSerializer converter = new AccumuloPcjSerializer();

    protected static final String RYA_TABLE_PREFIX = "demo_";

    // The MiniAccumuloCluster is re-used between tests.
    private MiniAccumuloClusterInstance cluster;

    // Rya data store and connections.
    protected RyaSailRepository ryaRepo = null;
    protected RepositoryConnection ryaConn = null;

    @BeforeClass
    public static void killLoudLogs() {
        Logger.getLogger(ClientCnxn.class).setLevel(Level.ERROR);
    }

    @Before
    public void resetTestEnvironmanet() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, RepositoryException, IOException, InterruptedException {
        // Start the cluster.
        cluster = new MiniAccumuloClusterInstance();
        cluster.startMiniAccumulo();

        // Setup the Rya library to use the Mini Accumulo.
        ryaRepo = setupRya();
        ryaConn = ryaRepo.getConnection();
    }

    @After
    public void shutdownMiniCluster() throws IOException, InterruptedException, RepositoryException {
        // Stop Rya.
        ryaRepo.shutDown();

        // Stop the cluster.
        cluster.stopMiniAccumulo();
    }

    /**
     * Format a Mini Accumulo to be a Rya repository.
     *
     * @return The Rya repository sitting on top of the Mini Accumulo.
     */
    private RyaSailRepository setupRya() throws AccumuloException, AccumuloSecurityException, RepositoryException {
        // Setup the Rya Repository that will be used to create Repository Connections.
        final RdfCloudTripleStore ryaStore = new RdfCloudTripleStore();
        final AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
        crdfdao.setConnector( cluster.getConnector() );

        // Setup Rya configuration values.
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(RYA_TABLE_PREFIX);
        conf.setDisplayQueryPlan(true);

        conf.setBoolean(USE_MOCK_INSTANCE, false);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, RYA_TABLE_PREFIX);
        conf.set(CLOUDBASE_USER, cluster.getUsername());
        conf.set(CLOUDBASE_PASSWORD, cluster.getPassword());
        conf.set(CLOUDBASE_INSTANCE, cluster.getInstanceName());

        crdfdao.setConf(conf);
        ryaStore.setRyaDAO(crdfdao);

        final RyaSailRepository ryaRepo = new RyaSailRepository(ryaStore);
        ryaRepo.initialize();

        return ryaRepo;
    }

    /**
     * Ensure that when a new PCJ table is created, it is initialized with the
     * correct metadata values.
     * <p>
     * The method being tested is {@link PcjTables#createPcjTable(Connector, String, Set, String)}
     */
    @Test
    public void createPcjTable() throws PcjException, AccumuloException, AccumuloSecurityException {
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        final Connector accumuloConn = cluster.getConnector();

        // Create a PCJ table in the Mini Accumulo.
        final String pcjTableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "testPcj");
        final Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(new VariableOrder("name;age"));
        final PcjTables pcjs = new PcjTables();
        pcjs.createPcjTable(accumuloConn, pcjTableName, varOrders, sparql);

        // Fetch the PcjMetadata and ensure it has the correct values.
        final PcjMetadata pcjMetadata = pcjs.getPcjMetadata(accumuloConn, pcjTableName);

        // Ensure the metadata matches the expected value.
        final PcjMetadata expected = new PcjMetadata(sparql, 0L, varOrders);
        assertEquals(expected, pcjMetadata);
    }

    /**
     * Ensure when results have been written to the PCJ table that they are in Accumulo.
     * <p>
     * The method being tested is {@link PcjTables#addResults(Connector, String, java.util.Collection)}
     */
    @Test
    public void addResults() throws PcjException, TableNotFoundException, BindingSetConversionException, AccumuloException, AccumuloSecurityException {
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        final Connector accumuloConn = cluster.getConnector();

        // Create a PCJ table in the Mini Accumulo.
        final String pcjTableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "testPcj");
        final Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(new VariableOrder("name;age"));
        final PcjTables pcjs = new PcjTables();
        pcjs.createPcjTable(accumuloConn, pcjTableName, varOrders, sparql);

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

        final Set<BindingSet> results = Sets.<BindingSet>newHashSet(alice, bob, charlie);
        pcjs.addResults(accumuloConn, pcjTableName, Sets.<VisibilityBindingSet>newHashSet(
                new VisibilityBindingSet(alice),
                new VisibilityBindingSet(bob),
                new VisibilityBindingSet(charlie)));

        // Make sure the cardinality was updated.
        final PcjMetadata metadata = pcjs.getPcjMetadata(accumuloConn, pcjTableName);
        assertEquals(3, metadata.getCardinality());

        // Scan Accumulo for the stored results.
        final Multimap<String, BindingSet> fetchedResults = loadPcjResults(accumuloConn, pcjTableName);

        // Ensure the expected results match those that were stored.
        final Multimap<String, BindingSet> expectedResults = HashMultimap.create();
        expectedResults.putAll("name;age", results);
        expectedResults.putAll("age;name", results);
        assertEquals(expectedResults, fetchedResults);
    }

    @Test
    public void listResults() throws PCJStorageException, AccumuloException, AccumuloSecurityException {
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        final Connector accumuloConn = cluster.getConnector();

        // Create a PCJ table in the Mini Accumulo.
        final String pcjTableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "testPcj");
        final Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(new VariableOrder("name;age"));
        final PcjTables pcjs = new PcjTables();
        pcjs.createPcjTable(accumuloConn, pcjTableName, varOrders, sparql);

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

        pcjs.addResults(accumuloConn, pcjTableName, Sets.<VisibilityBindingSet>newHashSet(
                new VisibilityBindingSet(alice),
                new VisibilityBindingSet(bob),
                new VisibilityBindingSet(charlie)));

        // Fetch the Binding Sets that have been stored in the PCJ table.
        final Set<BindingSet> results = new HashSet<>();
        for(final BindingSet result : pcjs.listResults(accumuloConn, pcjTableName, new Authorizations())) {
            results.add( result );
        }

        // Verify the fetched results match the expected ones.
        final Set<BindingSet> expected = Sets.<BindingSet>newHashSet(alice, bob, charlie);
        assertEquals(expected, results);
    }

    /**
     * Ensure when results are already stored in Rya, that we are able to populate
     * the PCJ table for a new SPARQL query using those results.
     * <p>
     * The method being tested is: {@link PcjTables#populatePcj(Connector, String, RepositoryConnection, String)}
     */
    @Test
    public void populatePcj() throws RepositoryException, PcjException, TableNotFoundException, BindingSetConversionException, AccumuloException, AccumuloSecurityException {
        // Load some Triples into Rya.
        final Set<Statement> triples = new HashSet<>();
        triples.add( new StatementImpl(new URIImpl("http://Alice"), new URIImpl("http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Alice"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
        triples.add( new StatementImpl(new URIImpl("http://Bob"), new URIImpl("http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Bob"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
        triples.add( new StatementImpl(new URIImpl("http://Charlie"), new URIImpl("http://hasAge"), new NumericLiteralImpl(12, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Charlie"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
        triples.add( new StatementImpl(new URIImpl("http://Eve"), new URIImpl("http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Eve"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );

        for(final Statement triple : triples) {
            ryaConn.add(triple);
        }

        // Create a PCJ table that will include those triples in its results.
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        final Connector accumuloConn = cluster.getConnector();

        final String pcjTableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "testPcj");
        final Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(new VariableOrder("name;age"));
        final PcjTables pcjs = new PcjTables();
        pcjs.createPcjTable(accumuloConn, pcjTableName, varOrders, sparql);

        // Populate the PCJ table using a Rya connection.
        pcjs.populatePcj(accumuloConn, pcjTableName, ryaConn);

        // Scan Accumulo for the stored results.
        final Multimap<String, BindingSet> fetchedResults = loadPcjResults(accumuloConn, pcjTableName);

        // Make sure the cardinality was updated.
        final PcjMetadata metadata = pcjs.getPcjMetadata(accumuloConn, pcjTableName);
        assertEquals(3, metadata.getCardinality());

        // Ensure the expected results match those that were stored.
        final MapBindingSet alice = new MapBindingSet();
        alice.addBinding("name", new URIImpl("http://Alice"));
        alice.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));

        final MapBindingSet bob = new MapBindingSet();
        bob.addBinding("name", new URIImpl("http://Bob"));
        bob.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));

        final MapBindingSet charlie = new MapBindingSet();
        charlie.addBinding("name", new URIImpl("http://Charlie"));
        charlie.addBinding("age", new NumericLiteralImpl(12, XMLSchema.INTEGER));

        final Set<BindingSet> results = Sets.<BindingSet>newHashSet(alice, bob, charlie);

        final Multimap<String, BindingSet> expectedResults = HashMultimap.create();
        expectedResults.putAll("name;age", results);
        expectedResults.putAll("age;name", results);
        assertEquals(expectedResults, fetchedResults);
    }

    /**
     * Ensure the method that creates a new PCJ table, scans Rya for matches, and
     * stores them in the PCJ table works.
     * <p>
     * The method being tested is: {@link PcjTables#createAndPopulatePcj(RepositoryConnection, Connector, String, String, String[], Optional)}
     */
    @Test
    public void createAndPopulatePcj() throws RepositoryException, PcjException, TableNotFoundException, BindingSetConversionException, AccumuloException, AccumuloSecurityException {
        // Load some Triples into Rya.
        final Set<Statement> triples = new HashSet<>();
        triples.add( new StatementImpl(new URIImpl("http://Alice"), new URIImpl("http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Alice"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
        triples.add( new StatementImpl(new URIImpl("http://Bob"), new URIImpl("http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Bob"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
        triples.add( new StatementImpl(new URIImpl("http://Charlie"), new URIImpl("http://hasAge"), new NumericLiteralImpl(12, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Charlie"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
        triples.add( new StatementImpl(new URIImpl("http://Eve"), new URIImpl("http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Eve"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );

        for(final Statement triple : triples) {
            ryaConn.add(triple);
        }

        // Create a PCJ table that will include those triples in its results.
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        final Connector accumuloConn = cluster.getConnector();

        final String pcjTableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "testPcj");

        // Create and populate the PCJ table.
        final PcjTables pcjs = new PcjTables();
        pcjs.createAndPopulatePcj(ryaConn, accumuloConn, pcjTableName, sparql, new String[]{"name", "age"}, Optional.<PcjVarOrderFactory>absent());

        // Make sure the cardinality was updated.
        final PcjMetadata metadata = pcjs.getPcjMetadata(accumuloConn, pcjTableName);
        assertEquals(3, metadata.getCardinality());

        // Scan Accumulo for the stored results.
        final Multimap<String, BindingSet> fetchedResults = loadPcjResults(accumuloConn, pcjTableName);

        // Ensure the expected results match those that were stored.
        final MapBindingSet alice = new MapBindingSet();
        alice.addBinding("name", new URIImpl("http://Alice"));
        alice.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));

        final MapBindingSet bob = new MapBindingSet();
        bob.addBinding("name", new URIImpl("http://Bob"));
        bob.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));

        final MapBindingSet charlie = new MapBindingSet();
        charlie.addBinding("name", new URIImpl("http://Charlie"));
        charlie.addBinding("age", new NumericLiteralImpl(12, XMLSchema.INTEGER));

        final Set<BindingSet> results = Sets.<BindingSet>newHashSet(alice, bob, charlie);

        final Multimap<String, BindingSet> expectedResults = HashMultimap.create();
        expectedResults.putAll("name;age", results);
        expectedResults.putAll("age;name", results);

        assertEquals(expectedResults, fetchedResults);
    }

    @Test
    public void listPcjs() throws PCJStorageException, AccumuloException, AccumuloSecurityException {
        final Connector accumuloConn = cluster.getConnector();

        // Set up the table names that will be used.
        final String instance1 = "instance1_";
        final String instance2 = "instance2_";

        final String instance1_table1 = new PcjTableNameFactory().makeTableName(instance1, "table1");
        final String instance1_table2 = new PcjTableNameFactory().makeTableName(instance1, "table2");
        final String instance1_table3 = new PcjTableNameFactory().makeTableName(instance1, "table3");

        final String instance2_table1 = new PcjTableNameFactory().makeTableName(instance2, "table1");

        // Create the PCJ Tables that are in instance 1 and instance 2.
        final Set<VariableOrder> varOrders = Sets.<VariableOrder>newHashSet( new VariableOrder("x") );
        final String sparql = "SELECT x WHERE ?x <http://isA> <http://Food>";

        final PcjTables pcjs = new PcjTables();
        pcjs.createPcjTable(accumuloConn, instance1_table1, varOrders, sparql);
        pcjs.createPcjTable(accumuloConn, instance1_table2, varOrders, sparql);
        pcjs.createPcjTable(accumuloConn, instance1_table3, varOrders, sparql);

        pcjs.createPcjTable(accumuloConn, instance2_table1, varOrders, sparql);

        // Ensure all of the names have been stored for instance 1 and 2.
        final Set<String> expected1 = Sets.newHashSet(instance1_table1, instance1_table2, instance1_table3);
        final Set<String> instance1Tables = Sets.newHashSet( pcjs.listPcjTables(accumuloConn, instance1) );
        assertEquals(expected1, instance1Tables);

        final Set<String> expected2 = Sets.newHashSet(instance2_table1);
        final Set<String> instance2Tables = Sets.newHashSet( pcjs.listPcjTables(accumuloConn, instance2) );
        assertEquals(expected2, instance2Tables);
    }

    @Test
    public void purge() throws PCJStorageException, AccumuloException, AccumuloSecurityException {
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        final Connector accumuloConn = cluster.getConnector();

        // Create a PCJ table in the Mini Accumulo.
        final String pcjTableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "testPcj");
        final Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(new VariableOrder("name;age"));
        final PcjTables pcjs = new PcjTables();
        pcjs.createPcjTable(accumuloConn, pcjTableName, varOrders, sparql);

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

        pcjs.addResults(accumuloConn, pcjTableName, Sets.<VisibilityBindingSet>newHashSet(
                new VisibilityBindingSet(alice),
                new VisibilityBindingSet(bob),
                new VisibilityBindingSet(charlie)));

        // Make sure the cardinality was updated.
        PcjMetadata metadata = pcjs.getPcjMetadata(accumuloConn, pcjTableName);
        assertEquals(3, metadata.getCardinality());

        // Purge the data.
        pcjs.purgePcjTable(accumuloConn, pcjTableName);

        // Make sure the cardinality was updated to 0.
        metadata = pcjs.getPcjMetadata(accumuloConn, pcjTableName);
        assertEquals(0, metadata.getCardinality());
    }

    @Test
    public void dropPcj() throws PCJStorageException, AccumuloException, AccumuloSecurityException {
        final Connector accumuloConn = cluster.getConnector();

        // Create a PCJ index.
        final String tableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "thePcj");
        final Set<VariableOrder> varOrders = Sets.<VariableOrder>newHashSet( new VariableOrder("x") );
        final String sparql = "SELECT x WHERE ?x <http://isA> <http://Food>";

        final PcjTables pcjs = new PcjTables();
        pcjs.createPcjTable(accumuloConn, tableName, varOrders, sparql);

        // Fetch its metadata to show that it has actually been created.
        final PcjMetadata expectedMetadata = new PcjMetadata(sparql, 0L, varOrders);
        PcjMetadata metadata = pcjs.getPcjMetadata(accumuloConn, tableName);
        assertEquals(expectedMetadata, metadata);

        // Drop it.
        pcjs.dropPcjTable(accumuloConn, tableName);

        // Show the metadata is no longer present.
        PCJStorageException tableDoesNotExistException = null;
        try {
            metadata = pcjs.getPcjMetadata(accumuloConn, tableName);
        } catch(final PCJStorageException e) {
            tableDoesNotExistException = e;
        }
        assertNotNull(tableDoesNotExistException);
    }

    /**
     * Scan accumulo for the results that are stored in a PCJ table. The
     * multimap stores a set of deserialized binding sets that were in the PCJ
     * table for every variable order that is found in the PCJ metadata.
     */
    private static Multimap<String, BindingSet> loadPcjResults(final Connector accumuloConn, final String pcjTableName) throws PcjException, TableNotFoundException, BindingSetConversionException {
        final Multimap<String, BindingSet> fetchedResults = HashMultimap.create();

        // Get the variable orders the data was written to.
        final PcjTables pcjs = new PcjTables();
        final PcjMetadata pcjMetadata = pcjs.getPcjMetadata(accumuloConn, pcjTableName);

        // Scan Accumulo for the stored results.
        for(final VariableOrder varOrder : pcjMetadata.getVarOrders()) {
            final Scanner scanner = accumuloConn.createScanner(pcjTableName, new Authorizations());
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