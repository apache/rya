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
package org.apache.rya.indexing.pcj.storage.mongo;

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.utils.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.MongoITBase;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

/**
 * Performs integration test using {@link MiniAccumuloCluster} to ensure the
 * functions of {@link PcjTables} work within a cluster setting.
 */
public class PcjDocumentsIntegrationTest extends MongoITBase {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Override
    protected void updateConfiguration(final MongoDBRdfConfiguration conf) {
        conf.setDisplayQueryPlan(true);
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

        final String pcjTableName = "testPcj";
        final MongoPcjDocuments pcjs = new MongoPcjDocuments(getMongoClient(), conf.getRyaInstanceName());
        pcjs.createPcj(pcjTableName, sparql);

        // Fetch the PcjMetadata and ensure it has the correct values.
        final PcjMetadata pcjMetadata = pcjs.getPcjMetadata(pcjTableName);

        // Ensure the metadata matches the expected value.
        final PcjMetadata expected = new PcjMetadata(sparql, 0L, Sets.newHashSet(new VariableOrder("name", "age"), new VariableOrder("age", "name")));
        assertEquals(expected, pcjMetadata);
    }

    /**
     * Ensure when results have been written to the PCJ table that they are in Accumulo.
     * <p>
     * The method being tested is {@link PcjTables#addResults(Connector, String, java.util.Collection)}
     */
    @Test
    public void addResults() throws Exception {
        final String sparql =
                "SELECT ?name ?age " +
                        "{" +
                        "FILTER(?age < 30) ." +
                        "?name <http://hasAge> ?age." +
                        "?name <http://playsSport> \"Soccer\" " +
                        "}";

        final String pcjTableName = "testPcj";
        final MongoPcjDocuments pcjs = new MongoPcjDocuments(getMongoClient(), conf.getRyaInstanceName());
        pcjs.createPcj(pcjTableName, sparql);

        // Add a few results to the PCJ table.
        final MapBindingSet alice = new MapBindingSet();
        alice.addBinding("name", VF.createIRI("http://Alice"));
        alice.addBinding("age", VF.createLiteral(BigInteger.valueOf(14)));

        final MapBindingSet bob = new MapBindingSet();
        bob.addBinding("name", VF.createIRI("http://Bob"));
        bob.addBinding("age", VF.createLiteral(BigInteger.valueOf(16)));

        final MapBindingSet charlie = new MapBindingSet();
        charlie.addBinding("name", VF.createIRI("http://Charlie"));
        charlie.addBinding("age", VF.createLiteral(BigInteger.valueOf(12)));

        final Set<BindingSet> expected = Sets.<BindingSet>newHashSet(alice, bob, charlie);
        pcjs.addResults(pcjTableName, Sets.<VisibilityBindingSet>newHashSet(
                new VisibilityBindingSet(alice),
                new VisibilityBindingSet(bob),
                new VisibilityBindingSet(charlie)));

        // Make sure the cardinality was updated.
        final PcjMetadata metadata = pcjs.getPcjMetadata(pcjTableName);
        assertEquals(3, metadata.getCardinality());

        // Scan Accumulo for the stored results.
        final Collection<BindingSet> fetchedResults = loadPcjResults(pcjTableName);
        assertEquals(expected, fetchedResults);
    }

    @Test
    public void listResults() throws Exception {
        final String sparql =
                "SELECT ?name ?age " +
                        "{" +
                        "FILTER(?age < 30) ." +
                        "?name <http://hasAge> ?age." +
                        "?name <http://playsSport> \"Soccer\" " +
                        "}";

        final String pcjTableName = "testPcj";
        final MongoPcjDocuments pcjs = new MongoPcjDocuments(getMongoClient(), conf.getRyaInstanceName());
        pcjs.createPcj(pcjTableName, sparql);

        // Add a few results to the PCJ table.
        final MapBindingSet alice = new MapBindingSet();
        alice.addBinding("name", VF.createIRI("http://Alice"));
        alice.addBinding("age", VF.createLiteral(BigInteger.valueOf(14)));

        final MapBindingSet bob = new MapBindingSet();
        bob.addBinding("name", VF.createIRI("http://Bob"));
        bob.addBinding("age", VF.createLiteral(BigInteger.valueOf(16)));

        final MapBindingSet charlie = new MapBindingSet();
        charlie.addBinding("name", VF.createIRI("http://Charlie"));
        charlie.addBinding("age", VF.createLiteral(BigInteger.valueOf(12)));

        pcjs.addResults(pcjTableName, Sets.<VisibilityBindingSet>newHashSet(
                new VisibilityBindingSet(alice),
                new VisibilityBindingSet(bob),
                new VisibilityBindingSet(charlie)));

        // Fetch the Binding Sets that have been stored in the PCJ table.
        final Set<BindingSet> results = new HashSet<>();

        final CloseableIterator<BindingSet> resultsIt = pcjs.listResults(pcjTableName);
        try {
            while(resultsIt.hasNext()) {
                results.add( resultsIt.next() );
            }
        } finally {
            resultsIt.close();
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
    public void populatePcj() throws Exception {
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        dao.setConf(new StatefulMongoDBRdfConfiguration(conf, getMongoClient()));
        dao.init();
        final RdfCloudTripleStore ryaStore = new RdfCloudTripleStore();
        ryaStore.setRyaDAO(dao);
        ryaStore.initialize();
        final SailRepositoryConnection ryaConn = new RyaSailRepository(ryaStore).getConnection();
        ryaConn.begin();

        try {
            // Load some Triples into Rya.
            final Set<Statement> triples = new HashSet<>();
            triples.add( VF.createStatement(VF.createIRI("http://Alice"), VF.createIRI("http://hasAge"), VF.createLiteral(BigInteger.valueOf(14))) );
            triples.add( VF.createStatement(VF.createIRI("http://Alice"), VF.createIRI("http://playsSport"), VF.createLiteral("Soccer")) );
            triples.add( VF.createStatement(VF.createIRI("http://Bob"), VF.createIRI("http://hasAge"), VF.createLiteral(BigInteger.valueOf(16))) );
            triples.add( VF.createStatement(VF.createIRI("http://Bob"), VF.createIRI("http://playsSport"), VF.createLiteral("Soccer")) );
            triples.add( VF.createStatement(VF.createIRI("http://Charlie"), VF.createIRI("http://hasAge"), VF.createLiteral(BigInteger.valueOf(12))) );
            triples.add( VF.createStatement(VF.createIRI("http://Charlie"), VF.createIRI("http://playsSport"), VF.createLiteral("Soccer")) );
            triples.add( VF.createStatement(VF.createIRI("http://Eve"), VF.createIRI("http://hasAge"), VF.createLiteral(BigInteger.valueOf(43))) );
            triples.add( VF.createStatement(VF.createIRI("http://Eve"), VF.createIRI("http://playsSport"), VF.createLiteral("Soccer")) );

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

            final String pcjTableName = "testPcj";
            final MongoPcjDocuments pcjs = new MongoPcjDocuments(getMongoClient(), conf.getRyaInstanceName());
            pcjs.createPcj(pcjTableName, sparql);

            // Populate the PCJ table using a Rya connection.
            pcjs.populatePcj(pcjTableName, ryaConn);

            final Collection<BindingSet> fetchedResults = loadPcjResults(pcjTableName);

            // Make sure the cardinality was updated.
            final PcjMetadata metadata = pcjs.getPcjMetadata(pcjTableName);
            assertEquals(3, metadata.getCardinality());

            // Ensure the expected results match those that were stored.
            final MapBindingSet alice = new MapBindingSet();
            alice.addBinding("name", VF.createIRI("http://Alice"));
            alice.addBinding("age", VF.createLiteral(BigInteger.valueOf(14)));

            final MapBindingSet bob = new MapBindingSet();
            bob.addBinding("name", VF.createIRI("http://Bob"));
            bob.addBinding("age", VF.createLiteral(BigInteger.valueOf(16)));

            final MapBindingSet charlie = new MapBindingSet();
            charlie.addBinding("name", VF.createIRI("http://Charlie"));
            charlie.addBinding("age", VF.createLiteral(BigInteger.valueOf(12)));

            final Set<BindingSet> expected = Sets.<BindingSet>newHashSet(alice, bob, charlie);

            assertEquals(expected, fetchedResults);
        } finally {
            ryaConn.close();
            ryaStore.shutDown();
        }
    }

    /**
     * Ensure the method that creates a new PCJ table, scans Rya for matches, and
     * stores them in the PCJ table works.
     * <p>
     * The method being tested is: {@link PcjTables#createAndPopulatePcj(RepositoryConnection, Connector, String, String, String[], Optional)}
     */
    @Test
    public void createAndPopulatePcj() throws Exception {
        final MongoDBRyaDAO dao = new MongoDBRyaDAO();
        dao.setConf(new StatefulMongoDBRdfConfiguration(conf, getMongoClient()));
        dao.init();
        final RdfCloudTripleStore ryaStore = new RdfCloudTripleStore();
        ryaStore.setRyaDAO(dao);
        ryaStore.initialize();
        final SailRepositoryConnection ryaConn = new RyaSailRepository(ryaStore).getConnection();
        ryaConn.begin();

        try {
            // Load some Triples into Rya.
            final Set<Statement> triples = new HashSet<>();
            triples.add( VF.createStatement(VF.createIRI("http://Alice"), VF.createIRI("http://hasAge"), VF.createLiteral(BigInteger.valueOf(14))) );
            triples.add( VF.createStatement(VF.createIRI("http://Alice"), VF.createIRI("http://playsSport"), VF.createLiteral("Soccer")) );
            triples.add( VF.createStatement(VF.createIRI("http://Bob"), VF.createIRI("http://hasAge"), VF.createLiteral(BigInteger.valueOf(16))) );
            triples.add( VF.createStatement(VF.createIRI("http://Bob"), VF.createIRI("http://playsSport"), VF.createLiteral("Soccer")) );
            triples.add( VF.createStatement(VF.createIRI("http://Charlie"), VF.createIRI("http://hasAge"), VF.createLiteral(BigInteger.valueOf(12))) );
            triples.add( VF.createStatement(VF.createIRI("http://Charlie"), VF.createIRI("http://playsSport"), VF.createLiteral("Soccer")) );
            triples.add( VF.createStatement(VF.createIRI("http://Eve"), VF.createIRI("http://hasAge"), VF.createLiteral(BigInteger.valueOf(43))) );
            triples.add( VF.createStatement(VF.createIRI("http://Eve"), VF.createIRI("http://playsSport"), VF.createLiteral("Soccer")) );

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

            final String pcjTableName = "testPcj";

            // Create and populate the PCJ table.
            final MongoPcjDocuments pcjs = new MongoPcjDocuments(getMongoClient(), conf.getRyaInstanceName());
            pcjs.createAndPopulatePcj(ryaConn, pcjTableName, sparql);

            // Make sure the cardinality was updated.
            final PcjMetadata metadata = pcjs.getPcjMetadata(pcjTableName);
            assertEquals(3, metadata.getCardinality());

            // Scan Accumulo for the stored results.
            final Collection<BindingSet> fetchedResults = loadPcjResults(pcjTableName);

            // Ensure the expected results match those that were stored.
            final MapBindingSet alice = new MapBindingSet();
            alice.addBinding("name", VF.createIRI("http://Alice"));
            alice.addBinding("age", VF.createLiteral(BigInteger.valueOf(14)));

            final MapBindingSet bob = new MapBindingSet();
            bob.addBinding("name", VF.createIRI("http://Bob"));
            bob.addBinding("age", VF.createLiteral(BigInteger.valueOf(16)));

            final MapBindingSet charlie = new MapBindingSet();
            charlie.addBinding("name", VF.createIRI("http://Charlie"));
            charlie.addBinding("age", VF.createLiteral(BigInteger.valueOf(12)));

            final Set<BindingSet> expected = Sets.<BindingSet>newHashSet(alice, bob, charlie);

            assertEquals(expected, fetchedResults);
        } finally {
            ryaConn.close();
            ryaStore.shutDown();
        }
    }

    @Test
    public void listPcjs() throws Exception {
        // Set up the table names that will be used.
        final String instance1 = "instance1_";
        final String instance2 = "instance2_";

        final String instance1_table1 = new PcjTableNameFactory().makeTableName(instance1, "table1");
        final String instance1_table2 = new PcjTableNameFactory().makeTableName(instance1, "table2");
        final String instance1_table3 = new PcjTableNameFactory().makeTableName(instance1, "table3");

        final String instance2_table1 = new PcjTableNameFactory().makeTableName(instance2, "table1");

        // Create the PCJ Tables that are in instance 1 and instance 2.
        final String sparql = "SELECT ?x WHERE { ?x <http://isA> <http://Food> }";

        final MongoPcjDocuments pcjs1 = new MongoPcjDocuments(getMongoClient(), instance1);
        final MongoPcjDocuments pcjs2 = new MongoPcjDocuments(getMongoClient(), instance2);
        pcjs1.createPcj(instance1_table1, sparql);
        pcjs1.createPcj(instance1_table2, sparql);
        pcjs1.createPcj(instance1_table3, sparql);

        pcjs2.createPcj(instance2_table1, sparql);

        // Ensure all of the names have been stored for instance 1 and 2.
        final Set<String> expected1 = Sets.newHashSet(instance1_table1, instance1_table2, instance1_table3);
        final Set<String> instance1Tables = Sets.newHashSet( pcjs1.listPcjDocuments() );
        assertEquals(expected1, instance1Tables);

        final Set<String> expected2 = Sets.newHashSet(instance2_table1);
        final Set<String> instance2Tables = Sets.newHashSet( pcjs2.listPcjDocuments() );
        assertEquals(expected2, instance2Tables);
    }

    @Test
    public void purge() throws Exception {
        final String sparql =
                "SELECT ?name ?age " +
                        "{" +
                        "FILTER(?age < 30) ." +
                        "?name <http://hasAge> ?age." +
                        "?name <http://playsSport> \"Soccer\" " +
                        "}";

        final String pcjTableName = "testPcj";
        final MongoPcjDocuments pcjs = new MongoPcjDocuments(getMongoClient(), conf.getRyaInstanceName());
        pcjs.createPcj(pcjTableName, sparql);

        // Add a few results to the PCJ table.
        final MapBindingSet alice = new MapBindingSet();
        alice.addBinding("name", VF.createIRI("http://Alice"));
        alice.addBinding("age", VF.createLiteral(BigInteger.valueOf(14)));

        final MapBindingSet bob = new MapBindingSet();
        bob.addBinding("name", VF.createIRI("http://Bob"));
        bob.addBinding("age", VF.createLiteral(BigInteger.valueOf(16)));

        final MapBindingSet charlie = new MapBindingSet();
        charlie.addBinding("name", VF.createIRI("http://Charlie"));
        charlie.addBinding("age", VF.createLiteral(BigInteger.valueOf(12)));

        pcjs.addResults(pcjTableName, Sets.<VisibilityBindingSet>newHashSet(
                new VisibilityBindingSet(alice),
                new VisibilityBindingSet(bob),
                new VisibilityBindingSet(charlie)));

        // Make sure the cardinality was updated.
        PcjMetadata metadata = pcjs.getPcjMetadata(pcjTableName);
        assertEquals(3, metadata.getCardinality());

        // Purge the data.
        pcjs.purgePcjs(pcjTableName);

        // Make sure the cardinality was updated to 0.
        metadata = pcjs.getPcjMetadata(pcjTableName);
        assertEquals(0, metadata.getCardinality());
    }

    @Test(expected=PCJStorageException.class)
    public void dropPcj() throws Exception {
        // Create a PCJ index.
        final String pcjTableName = "testPcj";
        final String sparql = "SELECT x WHERE ?x <http://isA> <http://Food>";

        final MongoPcjDocuments pcjs = new MongoPcjDocuments(getMongoClient(), conf.getRyaInstanceName());
        pcjs.createPcj(pcjTableName, sparql);

        // Fetch its metadata to show that it has actually been created.
        final PcjMetadata expectedMetadata = new PcjMetadata(sparql, 0L, new ArrayList<VariableOrder>());
        PcjMetadata metadata = pcjs.getPcjMetadata(pcjTableName);
        assertEquals(expectedMetadata, metadata);

        // Drop it.
        pcjs.dropPcj(pcjTableName);

        // Show the metadata is no longer present.
        metadata = pcjs.getPcjMetadata(pcjTableName);
    }

    private Collection<BindingSet> loadPcjResults(final String pcjTableName) throws PcjException, TableNotFoundException, BindingSetConversionException {

        // Get the variable orders the data was written to.
        final MongoPcjDocuments pcjs = new MongoPcjDocuments(getMongoClient(), conf.getRyaInstanceName());
        final CloseableIterator<BindingSet> bindings = pcjs.listResults(pcjTableName);
        final Set<BindingSet> bindingSets = new HashSet<>();
        while(bindings.hasNext()) {
            bindingSets.add(bindings.next());
        }
        return bindingSets;
    }
}