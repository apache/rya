package mvm.rya.indexing.external.tupleSet;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
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
import com.google.common.io.Files;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.resolver.RyaTypeResolverException;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.external.tupleSet.PcjTables.PcjException;
import mvm.rya.indexing.external.tupleSet.PcjTables.PcjMetadata;
import mvm.rya.indexing.external.tupleSet.PcjTables.PcjTableNameFactory;
import mvm.rya.indexing.external.tupleSet.PcjTables.PcjVarOrderFactory;
import mvm.rya.indexing.external.tupleSet.PcjTables.ShiftVarOrderFactory;
import mvm.rya.indexing.external.tupleSet.PcjTables.VariableOrder;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;
import mvm.rya.rdftriplestore.RyaSailRepository;

public class PcjTablesIntegrationTests {
    private static final Logger log = Logger.getLogger(PcjTablesIntegrationTests.class);

    protected static final String RYA_TABLE_PREFIX = "demo_";

    // Rya data store and connections.
    protected MiniAccumuloCluster accumulo = null;
    protected static Connector accumuloConn = null;
    protected RyaSailRepository ryaRepo = null;
    protected RepositoryConnection ryaConn = null;

    @Before
    public void setupMiniResources() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, RepositoryException {
        // Initialize the Mini Accumulo that will be used to store Triples and get a connection to it.
        accumulo = startMiniAccumulo();

        // Setup the Rya library to use the Mini Accumulo.
        ryaRepo = setupRya(accumulo);
        ryaConn = ryaRepo.getConnection();
    }

    /**
     * Ensure that when a new PCJ table is created, it is initialized with the
     * correct metadata values.
     */
    @Test
    public void createPcjTable() throws PcjException {
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        // Create a PCJ table in the Mini Accumulo.
        final String pcjTableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "testPcj");
        Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(new VariableOrder("name;age"));
        PcjTables.createPcjTable(accumuloConn, pcjTableName, varOrders, sparql);

        // Fetch the PCJMetadata and ensure it has the correct values.
        final Optional<PcjMetadata> pcjMetadata = PcjTables.getPcjMetadata(accumuloConn, pcjTableName);

        // Ensure the metadata matches the expected value.
        final PcjMetadata expected = new PcjMetadata(sparql, 10L, varOrders);
        assertEquals(expected, pcjMetadata.get());
    }

    /**
     * Ensure when results have been written to the PCJ table that they are in Accumulo.
     */
    @Test
    public void addResults() throws PcjException, TableNotFoundException, RyaTypeResolverException {
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        // Create a PCJ table in the Mini Accumulo.
        final String pcjTableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "testPcj");
        Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(new VariableOrder("name;age"));
        PcjTables.createPcjTable(accumuloConn, pcjTableName, varOrders, sparql);

        // Add a few results to the PCJ table.
        MapBindingSet alice = new MapBindingSet();
        alice.addBinding("name", new URIImpl("http://Alice"));
        alice.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));

        MapBindingSet bob = new MapBindingSet();
        bob.addBinding("name", new URIImpl("http://Bob"));
        bob.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));

        MapBindingSet charlie = new MapBindingSet();
        charlie.addBinding("name", new URIImpl("http://Charlie"));
        charlie.addBinding("age", new NumericLiteralImpl(12, XMLSchema.INTEGER));

        Set<BindingSet> results = Sets.<BindingSet>newHashSet(alice, bob, charlie);
        PcjTables.addResults(accumuloConn, pcjTableName, results);

        // Get the variable orders the data was written to.
        PcjMetadata pcjMetadata = PcjTables.getPcjMetadata(accumuloConn, pcjTableName).get();

        // Scan Accumulo for the stored results.
        Multimap<String, BindingSet> fetchedResults = HashMultimap.create();

        for(VariableOrder varOrder : pcjMetadata.getVarOrders()) {
            Scanner scanner = accumuloConn.createScanner(pcjTableName, new Authorizations());
            scanner.fetchColumnFamily( new Text(varOrder.toString()) );

            for(Entry<Key, Value> entry : scanner) {
                byte[] serializedResult = entry.getKey().getRow().getBytes();
                BindingSet result = AccumuloPcjSerializer.deSerialize(serializedResult, varOrder.toArray());
                fetchedResults.put(varOrder.toString(), result);
            }
        }

        // Ensure the expected results match those that were stored.
        Multimap<String, BindingSet> expectedResults = HashMultimap.create();
        expectedResults.putAll("name;age", results);
        expectedResults.putAll("age;name", results);

        assertEquals(expectedResults, fetchedResults);
    }

    /**
     * TODO doc
     */
    @Test
    public void populatePcj() throws RepositoryException, PcjException, TableNotFoundException, RyaTypeResolverException {
        // Load some Triples into Rya.
        Set<Statement> triples = new HashSet<>();
        triples.add( new StatementImpl(new URIImpl("http://Alice"), new URIImpl("http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Alice"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
        triples.add( new StatementImpl(new URIImpl("http://Bob"), new URIImpl("http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Bob"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
        triples.add( new StatementImpl(new URIImpl("http://Charlie"), new URIImpl("http://hasAge"), new NumericLiteralImpl(12, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Charlie"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
        triples.add( new StatementImpl(new URIImpl("http://Eve"), new URIImpl("http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Eve"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );

        for(Statement triple : triples) {
            ryaConn.add(triple);
        }

        // Create a PCJ table will include those triples in its results.
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        final String pcjTableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "testPcj");
        Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(new VariableOrder("name;age"));
        PcjTables.createPcjTable(accumuloConn, pcjTableName, varOrders, sparql);

        // Populate the PCJ table using a Rya connection.
        PcjTables.populatePcj(accumuloConn, pcjTableName, ryaConn, sparql);

        // Get the variable orders the data was written to.
        PcjMetadata pcjMetadata = PcjTables.getPcjMetadata(accumuloConn, pcjTableName).get();

        // Scan Accumulo for the stored results.
        Multimap<String, BindingSet> fetchedResults = HashMultimap.create();

        for(VariableOrder varOrder : pcjMetadata.getVarOrders()) {
            Scanner scanner = accumuloConn.createScanner(pcjTableName, new Authorizations());
            scanner.fetchColumnFamily( new Text(varOrder.toString()) );

            for(Entry<Key, Value> entry : scanner) {
                byte[] serializedResult = entry.getKey().getRow().getBytes();
                BindingSet result = AccumuloPcjSerializer.deSerialize(serializedResult, varOrder.toArray());
                fetchedResults.put(varOrder.toString(), result);
            }
        }

        // Ensure the expected results match those that were stored.
        MapBindingSet alice = new MapBindingSet();
        alice.addBinding("name", new URIImpl("http://Alice"));
        alice.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));

        MapBindingSet bob = new MapBindingSet();
        bob.addBinding("name", new URIImpl("http://Bob"));
        bob.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));

        MapBindingSet charlie = new MapBindingSet();
        charlie.addBinding("name", new URIImpl("http://Charlie"));
        charlie.addBinding("age", new NumericLiteralImpl(12, XMLSchema.INTEGER));

        Set<BindingSet> results = Sets.<BindingSet>newHashSet(alice, bob, charlie);

        Multimap<String, BindingSet> expectedResults = HashMultimap.create();
        expectedResults.putAll("name;age", results);
        expectedResults.putAll("age;name", results);

        assertEquals(expectedResults, fetchedResults);
    }

    /**
     * TODO doc
     */
    @Test
    public void createAndPopulatePcj() throws RepositoryException, PcjException, TableNotFoundException, RyaTypeResolverException {
        // Load some Triples into Rya.
        Set<Statement> triples = new HashSet<>();
        triples.add( new StatementImpl(new URIImpl("http://Alice"), new URIImpl("http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Alice"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
        triples.add( new StatementImpl(new URIImpl("http://Bob"), new URIImpl("http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Bob"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
        triples.add( new StatementImpl(new URIImpl("http://Charlie"), new URIImpl("http://hasAge"), new NumericLiteralImpl(12, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Charlie"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
        triples.add( new StatementImpl(new URIImpl("http://Eve"), new URIImpl("http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)) );
        triples.add( new StatementImpl(new URIImpl("http://Eve"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );

        for(Statement triple : triples) {
            ryaConn.add(triple);
        }

        // Create a PCJ table will include those triples in its results.
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        final String pcjTableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "testPcj");

        // Create and populate the PCJ table.
        PcjTables.createAndPopulatePcj(ryaConn, accumuloConn, pcjTableName, sparql, new String[]{"name", "age"}, Optional.<PcjVarOrderFactory>absent());

        // Get the variable orders the data was written to.
        PcjMetadata pcjMetadata = PcjTables.getPcjMetadata(accumuloConn, pcjTableName).get();

        // Scan Accumulo for the stored results.
        Multimap<String, BindingSet> fetchedResults = HashMultimap.create();

        for(VariableOrder varOrder : pcjMetadata.getVarOrders()) {
            Scanner scanner = accumuloConn.createScanner(pcjTableName, new Authorizations());
            scanner.fetchColumnFamily( new Text(varOrder.toString()) );

            for(Entry<Key, Value> entry : scanner) {
                byte[] serializedResult = entry.getKey().getRow().getBytes();
                BindingSet result = AccumuloPcjSerializer.deSerialize(serializedResult, varOrder.toArray());
                fetchedResults.put(varOrder.toString(), result);
            }
        }

        // Ensure the expected results match those that were stored.
        MapBindingSet alice = new MapBindingSet();
        alice.addBinding("name", new URIImpl("http://Alice"));
        alice.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));

        MapBindingSet bob = new MapBindingSet();
        bob.addBinding("name", new URIImpl("http://Bob"));
        bob.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));

        MapBindingSet charlie = new MapBindingSet();
        charlie.addBinding("name", new URIImpl("http://Charlie"));
        charlie.addBinding("age", new NumericLiteralImpl(12, XMLSchema.INTEGER));

        Set<BindingSet> results = Sets.<BindingSet>newHashSet(alice, bob, charlie);

        Multimap<String, BindingSet> expectedResults = HashMultimap.create();
        expectedResults.putAll("name;age", results);
        expectedResults.putAll("age;name", results);

        assertEquals(expectedResults, fetchedResults);
    }

    @After
    public void shutdownMiniResources() {
        if(ryaConn != null) {
            try {
                log.info("Shutting down Rya Connection.");
                ryaConn.close();
                log.info("Rya Connection shut down.");
            } catch(final Exception e) {
                log.error("Could not shut down the Rya Connection.", e);
            }
        }

        if(ryaRepo != null) {
            try {
                log.info("Shutting down Rya Repo.");
                ryaRepo.shutDown();
                log.info("Rya Repo shut down.");
            } catch(final Exception e) {
                log.error("Could not shut down the Rya Repo.", e);
            }
        }

        if(accumulo != null) {
            try {
                log.info("Shutting down the Mini Accumulo being used as a Rya store.");
                accumulo.stop();
                log.info("Mini Accumulo being used as a Rya store shut down.");
            } catch(final Exception e) {
                log.error("Could not shut down the Mini Accumulo.", e);
            }
        }
    }

    /**
     * Setup a Mini Accumulo cluster that uses a temporary directory to store its data.
     *
     * @return A Mini Accumulo cluster.
     */
    private static MiniAccumuloCluster startMiniAccumulo() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
        final File miniDataDir = Files.createTempDir();

        // Setup and start the Mini Accumulo.
        final MiniAccumuloCluster accumulo = new MiniAccumuloCluster(miniDataDir, "password");
        accumulo.start();

        // Store a connector to the Mini Accumulo.
        final Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        accumuloConn = instance.getConnector("root", new PasswordToken("password"));

        return accumulo;
    }

    /**
     * Format a Mini Accumulo to be a Rya repository.
     *
     * @param accumulo - The Mini Accumulo cluster Rya will sit on top of. (not null)
     * @return The Rya repository sitting on top of the Mini Accumulo.
     */
    private static RyaSailRepository setupRya(final MiniAccumuloCluster accumulo) throws AccumuloException, AccumuloSecurityException, RepositoryException {
        checkNotNull(accumulo);

        // Setup the Rya Repository that will be used to create Repository Connections.
        final RdfCloudTripleStore ryaStore = new RdfCloudTripleStore();
        final AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
        crdfdao.setConnector(accumuloConn);

        // Setup Rya configuration values.
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix("demo_");
        conf.setDisplayQueryPlan(true);

        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, RYA_TABLE_PREFIX);
        conf.set(ConfigUtils.CLOUDBASE_USER, "root");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "password");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, accumulo.getInstanceName());

        crdfdao.setConf(conf);
        ryaStore.setRyaDAO(crdfdao);

        final RyaSailRepository ryaRepo = new RyaSailRepository(ryaStore);
        ryaRepo.initialize();

        return ryaRepo;
    }
}