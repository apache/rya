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
package org.apache.rya.indexing.pcj.fluo;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoAdmin.AlreadyInitializedException;
import org.apache.fluo.api.client.FluoAdmin.TableExistsException;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaExportParameters;
import org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.zookeeper.ClientCnxn;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.sail.Sail;

import com.google.common.io.Files;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.sail.config.RyaSailFactory;

/**
 * Integration tests that ensure the Fluo application processes PCJs results
 * correctly.
 * <p>
 * This class is being ignored because it doesn't contain any unit tests.
 */
public abstract class ITBase {
    private static final Logger log = Logger.getLogger(ITBase.class);

    // Rya data store and connections.
    protected static final String RYA_INSTANCE_NAME = "demo_";
    protected RyaSailRepository ryaRepo = null;
    protected RepositoryConnection ryaConn = null;

    // Mini Accumulo Cluster
    protected static final String ACCUMULO_USER = "root";
    protected static final String ACCUMULO_PASSWORD = "password";
    protected MiniAccumuloCluster cluster;
    protected static Connector accumuloConn = null;
    protected String instanceName = null;
    protected String zookeepers = null;

    // Fluo data store and connections.
    protected static final String FLUO_APP_NAME = "IntegrationTests";
    protected MiniFluo fluo = null;
    protected FluoClient fluoClient = null;

    @BeforeClass
    public static void killLoudLogs() {
        Logger.getLogger(ClientCnxn.class).setLevel(Level.ERROR);
    }

    @Before
    public void setupMiniResources() throws Exception {
    	// Initialize the Mini Accumulo that will be used to host Rya and Fluo.
    	setupMiniAccumulo();

        // Initialize the Mini Fluo that will be used to store created queries.
        fluo = startMiniFluo();
        fluoClient = FluoFactory.newClient(fluo.getClientConfiguration());

        // Initialize the Rya that will be used by the tests.
        ryaRepo = setupRya(instanceName, zookeepers);
        ryaConn = ryaRepo.getConnection();
    }

    @After
    public void shutdownMiniResources() {
        if (ryaConn != null) {
            try {
                log.info("Shutting down Rya Connection.");
                ryaConn.close();
                log.info("Rya Connection shut down.");
            } catch (final Exception e) {
                log.error("Could not shut down the Rya Connection.", e);
            }
        }

        if (ryaRepo != null) {
            try {
                log.info("Shutting down Rya Repo.");
                ryaRepo.shutDown();
                log.info("Rya Repo shut down.");
            } catch (final Exception e) {
                log.error("Could not shut down the Rya Repo.", e);
            }
        }

        if (fluoClient != null) {
            try {
                log.info("Shutting down Fluo Client.");
                fluoClient.close();
                log.info("Fluo Client shut down.");
            } catch (final Exception e) {
                log.error("Could not shut down the Fluo Client.", e);
            }
        }

        if (fluo != null) {
            try {
                log.info("Shutting down Mini Fluo.");
                fluo.close();
                log.info("Mini Fluo shut down.");
            } catch (final Exception e) {
                log.error("Could not shut down the Mini Fluo.", e);
            }
        }

        if(cluster != null) {
            try {
                log.info("Shutting down the Mini Accumulo being used as a Rya store.");
                cluster.stop();
                log.info("Mini Accumulo being used as a Rya store shut down.");
            } catch(final Exception e) {
                log.error("Could not shut down the Mini Accumulo.", e);
            }
        }
    }

    /**
     * A helper fuction for creating a {@link BindingSet} from an array of
     * {@link Binding}s.
     *
     * @param bindings - The bindings to include in the set. (not null)
     * @return A {@link BindingSet} holding the bindings.
     */
    protected static BindingSet makeBindingSet(final Binding... bindings) {
        final MapBindingSet bindingSet = new MapBindingSet();
        for (final Binding binding : bindings) {
            bindingSet.addBinding(binding);
        }
        return bindingSet;
    }

    /**
     * A helper function for creating a {@link RyaStatement} that represents a
     * Triple.
     *
     * @param subject - The Subject of the Triple. (not null)
     * @param predicate - The Predicate of the Triple. (not null)
     * @param object - The Object of the Triple. (not null)
     * @return A Triple as a {@link RyaStatement}.
     */
    protected static RyaStatement makeRyaStatement(final String subject, final String predicate, final String object) {
        checkNotNull(subject);
        checkNotNull(predicate);
        checkNotNull(object);

        final RyaStatementBuilder builder = RyaStatement.builder().setSubject(new RyaURI(subject))
                .setPredicate(new RyaURI(predicate));

        if (object.startsWith("http://")) {
            builder.setObject(new RyaURI(object));
        } else {
            builder.setObject(new RyaType(object));
        }

        return builder.build();
    }

    /**
     * A helper function for creating a {@link RyaStatement} that represents a
     * Triple.
     *
     * @param subject - The Subject of the Triple. (not null)
     * @param predicate - The Predicate of the Triple. (not null)
     * @param object - The Object of the Triple. (not null)
     * @return A Triple as a {@link RyaStatement}.
     */
    protected static RyaStatement makeRyaStatement(final String subject, final String predicate, final int object) {
        checkNotNull(subject);
        checkNotNull(predicate);

        return RyaStatement.builder().setSubject(new RyaURI(subject)).setPredicate(new RyaURI(predicate))
                .setObject(new RyaType(XMLSchema.INT, "" + object)).build();
    }

    /**
     * A helper function for creating a Sesame {@link Statement} that represents
     * a Triple..
     *
     * @param subject - The Subject of the Triple. (not null)
     * @param predicate - The Predicate of the Triple. (not null)
     * @param object - The Object of the Triple. (not null)
     * @return A Triple as a {@link Statement}.
     */
    protected static Statement makeStatement(final String subject, final String predicate, final String object) {
        checkNotNull(subject);
        checkNotNull(predicate);
        checkNotNull(object);

        final RyaStatement ryaStmt = makeRyaStatement(subject, predicate, object);
        return RyaToRdfConversions.convertStatement(ryaStmt);
    }

    /**
     * Fetches the binding sets that are the results of a specific SPARQL query from the Fluo table.
     *
     * @param fluoClient- A connection to the Fluo table where the results reside. (not null)
     * @param sparql - This query's results will be fetched. (not null)
     * @return The binding sets for the query's results.
     */
    protected static Set<BindingSet> getQueryBindingSetValues(final FluoClient fluoClient, final String sparql) {
        final Set<BindingSet> bindingSets = new HashSet<>();

        try (Snapshot snapshot = fluoClient.newSnapshot()) {
            final String queryId = snapshot.get(Bytes.of(sparql), FluoQueryColumns.QUERY_ID).toString();

            // Fetch the query's variable order.
            final QueryMetadata queryMetadata = new FluoQueryMetadataDAO().readQueryMetadata(snapshot, queryId);
            final VariableOrder varOrder = queryMetadata.getVariableOrder();

            CellScanner cellScanner = snapshot.scanner().fetch(FluoQueryColumns.QUERY_BINDING_SET).build();
            final BindingSetStringConverter converter = new BindingSetStringConverter();

           Iterator<RowColumnValue> iter = cellScanner.iterator();
            
            while (iter.hasNext()) {
            	final String bindingSetString = iter.next().getsValue();
                final BindingSet bindingSet = converter.convert(bindingSetString, varOrder);
                bindingSets.add(bindingSet);
            }
        }

        return bindingSets;
    }

    private void setupMiniAccumulo() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
    	final File miniDataDir = Files.createTempDir();

    	// Setup and start the Mini Accumulo.
    	final MiniAccumuloConfig cfg = new MiniAccumuloConfig(miniDataDir, ACCUMULO_PASSWORD);
    	cluster = new MiniAccumuloCluster(cfg);
    	cluster.start();

    	// Store a connector to the Mini Accumulo.
    	instanceName = cluster.getInstanceName();
    	zookeepers = cluster.getZooKeepers();

    	final Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
    	accumuloConn = instance.getConnector(ACCUMULO_USER, new PasswordToken(ACCUMULO_PASSWORD));
    }

     /**
      * Sets up a Rya instance.
      */
    protected static RyaSailRepository setupRya(final String instanceName, final String zookeepers) throws Exception {
        checkNotNull(instanceName);
        checkNotNull(zookeepers);

        // Install the Rya instance to the mini accumulo cluster.
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(new AccumuloConnectionDetails(
                ACCUMULO_USER,
                ACCUMULO_PASSWORD.toCharArray(),
                instanceName,
                zookeepers), accumuloConn);

        ryaClient.getInstall().install(RYA_INSTANCE_NAME, InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setEnableFreeTextIndex(true)
                .setEnableEntityCentricIndex(true)
                .setEnableGeoIndex(true)
                .setEnableTemporalIndex(true)
                .setEnablePcjIndex(true)
                .setFluoPcjAppName(FLUO_APP_NAME)
                .build());

        // Connect to the Rya instance that was just installed.
        final AccumuloRdfConfiguration conf = makeConfig(instanceName, zookeepers);
        final Sail sail = RyaSailFactory.getInstance(conf);
        final RyaSailRepository ryaRepo = new RyaSailRepository(sail);
        return ryaRepo;
    }

    protected static AccumuloRdfConfiguration makeConfig(final String instanceName, final String zookeepers) {
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(RYA_INSTANCE_NAME);
        // Accumulo connection information.
        conf.set(ConfigUtils.CLOUDBASE_USER, ACCUMULO_USER);
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, ACCUMULO_PASSWORD);
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, instanceName);
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, zookeepers);
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "");
        // PCJ configuration information.
        conf.set(ConfigUtils.USE_PCJ, "true");
        conf.set(ConfigUtils.USE_PCJ_UPDATER_INDEX, "true");
        conf.set(ConfigUtils.USE_PCJ_FLUO_UPDATER, "true");
        conf.set(ConfigUtils.FLUO_APP_NAME, FLUO_APP_NAME);
        conf.set(ConfigUtils.PCJ_STORAGE_TYPE,
                PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType.ACCUMULO.toString());
        conf.set(ConfigUtils.PCJ_UPDATER_TYPE,
                PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType.FLUO.toString());

        return conf;
    }

    /**
     * Setup a Mini Fluo cluster that uses a temporary directory to store its data.
     *
     * @return A Mini Fluo cluster.
     */
    protected MiniFluo startMiniFluo() throws AlreadyInitializedException, TableExistsException {
        // Setup the observers that will be used by the Fluo PCJ Application.
        final List<ObserverSpecification> observers = new ArrayList<>();
        observers.add(new ObserverSpecification(TripleObserver.class.getName()));
        observers.add(new ObserverSpecification(StatementPatternObserver.class.getName()));
        observers.add(new ObserverSpecification(JoinObserver.class.getName()));
        observers.add(new ObserverSpecification(FilterObserver.class.getName()));

        final HashMap<String, String> exportParams = new HashMap<>();
        final RyaExportParameters ryaParams = new RyaExportParameters(exportParams);
        ryaParams.setExportToRya(true);
        ryaParams.setRyaInstanceName(RYA_INSTANCE_NAME);
        ryaParams.setAccumuloInstanceName(instanceName);
        ryaParams.setZookeeperServers(zookeepers);
        ryaParams.setExporterUsername(ITBase.ACCUMULO_USER);
        ryaParams.setExporterPassword(ITBase.ACCUMULO_PASSWORD);
        
        // Configure the export observer to export new PCJ results to the mini accumulo cluster.
        final ObserverSpecification exportObserverConfig = new ObserverSpecification(QueryResultObserver.class.getName(), exportParams);
        observers.add(exportObserverConfig);

        // Configure how the mini fluo will run.
        final FluoConfiguration config = new FluoConfiguration();
        config.setMiniStartAccumulo(false);
        config.setAccumuloInstance(instanceName);
        config.setAccumuloUser(ACCUMULO_USER);
        config.setAccumuloPassword(ACCUMULO_PASSWORD);
        config.setInstanceZookeepers(zookeepers + "/fluo");
        config.setAccumuloZookeepers(zookeepers);

        config.setApplicationName(FLUO_APP_NAME);
        config.setAccumuloTable("fluo" + FLUO_APP_NAME);

        config.addObservers(observers);

        FluoFactory.newAdmin(config).initialize(
        		new FluoAdmin.InitializationOptions().setClearTable(true).setClearZookeeper(true) );
        return FluoFactory.newMiniFluo(config);
    }
}
