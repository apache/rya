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

import io.fluo.api.client.FluoAdmin;
import io.fluo.api.client.FluoAdmin.AlreadyInitializedException;
import io.fluo.api.client.FluoAdmin.TableExistsException;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.mini.MiniFluo;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaStatement.RyaStatementBuilder;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.external.PrecomputedJoinIndexerConfig;
import mvm.rya.rdftriplestore.RyaSailRepository;
import mvm.rya.rdftriplestore.inference.InferenceEngineException;
import mvm.rya.sail.config.RyaSailFactory;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.log4j.Logger;
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
import org.junit.After;
import org.junit.Before;
import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.Sail;

import com.google.common.io.Files;

/**
 * Integration tests that ensure the Fluo application processes PCJs results
 * correctly.
 * <p>
 * This class is being ignored because it doesn't contain any unit tests.
 */
public abstract class ITBase {
    private static final Logger log = Logger.getLogger(ITBase.class);

    protected static final String RYA_TABLE_PREFIX = "demo_";
    
    protected static final String ACCUMULO_USER = "root";
    protected static final String ACCUMULO_PASSWORD = "password";

    // Rya data store and connections.
    protected RyaSailRepository ryaRepo = null;
    protected RepositoryConnection ryaConn = null;
    

    // Mini Accumulo Cluster
    protected MiniAccumuloCluster cluster;
    protected static Connector accumuloConn = null;
    protected String instanceName = null;
    protected String zookeepers = null;
    
    // Fluo data store and connections.
    protected MiniFluo fluo = null;
    protected FluoClient fluoClient = null;
    protected final String appName = "IntegrationTests";

    @Before
    public void setupMiniResources()
            throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException, RepositoryException,
            RyaDAOException, NumberFormatException, InferenceEngineException, AlreadyInitializedException, TableExistsException {
    	// Initialize the Mini Accumulo that will be used to host Rya and Fluo.
    	setupMiniAccumulo();
    	
        // Initialize the Mini Fluo that will be used to store created queries.
        fluo = startMiniFluo();
        fluoClient = FluoFactory.newClient(fluo.getClientConfiguration());

        // Initialize the Rya that will be used by the tests.
        ryaRepo = setupRya(ACCUMULO_USER, ACCUMULO_PASSWORD, instanceName, zookeepers, appName);
        ryaConn = ryaRepo.getConnection();
    }
    
    @After
    public void shutdownMiniResources() {
    	// TODO shutdown the cluster
    	
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
     * @param bindings
     *            - The bindings to include in the set. (not null)
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
     * @param subject
     *            - The Subject of the Triple. (not null)
     * @param predicate
     *            - The Predicate of the Triple. (not null)
     * @param object
     *            - The Object of the Triple. (not null)
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
     * @param subject
     *            - The Subject of the Triple. (not null)
     * @param predicate
     *            - The Predicate of the Triple. (not null)
     * @param object
     *            - The Object of the Triple. (not null)
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
     * @param subject
     *            - The Subject of the Triple. (not null)
     * @param predicate
     *            - The Predicate of the Triple. (not null)
     * @param object
     *            - The Object of the Triple. (not null)
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
     * Fetches the binding sets that are the results of a specific SPARQL query
     * from the Fluo table.
     *
     * @param fluoClient-
     *            A connection to the Fluo table where the results reside. (not
     *            null)
     * @param sparql
     *            - This query's results will be fetched. (not null)
     * @return The binding sets for the query's results.
     */
    protected static Set<BindingSet> getQueryBindingSetValues(final FluoClient fluoClient, final String sparql) {
        final Set<BindingSet> bindingSets = new HashSet<>();

        try (Snapshot snapshot = fluoClient.newSnapshot()) {
            final String queryId = snapshot.get(Bytes.of(sparql), FluoQueryColumns.QUERY_ID).toString();

            // Fetch the query's variable order.
            final QueryMetadata queryMetadata = new FluoQueryMetadataDAO().readQueryMetadata(snapshot, queryId);
            final VariableOrder varOrder = queryMetadata.getVariableOrder();

            // Fetch the Binding Sets for the query.
            final ScannerConfiguration scanConfig = new ScannerConfiguration();
            scanConfig.fetchColumn(FluoQueryColumns.QUERY_BINDING_SET.getFamily(),
                    FluoQueryColumns.QUERY_BINDING_SET.getQualifier());

            BindingSetStringConverter converter = new BindingSetStringConverter();

            final RowIterator rowIter = snapshot.get(scanConfig);
            while (rowIter.hasNext()) {
                final Entry<Bytes, ColumnIterator> row = rowIter.next();
                final String bindingSetString = row.getValue().next().getValue().toString();
                final BindingSet bindingSet = converter.convert(bindingSetString, varOrder);
                bindingSets.add(bindingSet);
            }
        }

        return bindingSets;
    }

    private void setupMiniAccumulo() throws IOException, InterruptedException, AccumuloException, AccumuloSecurityException {
    	File miniDataDir = Files.createTempDir();
    	
    	// Setup and start the Mini Accumulo.
    	MiniAccumuloConfig cfg = new MiniAccumuloConfig(miniDataDir, ACCUMULO_PASSWORD);
    	cluster = new MiniAccumuloCluster(cfg);
    	cluster.start();
    	
    	// Store a connector to the Mini Accumulo.
    	instanceName = cluster.getInstanceName();
    	zookeepers = cluster.getZooKeepers();
    	
    	Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
    	accumuloConn = instance.getConnector(ACCUMULO_USER, new PasswordToken(ACCUMULO_PASSWORD));
    }

     /**
      * Sets up a Rya instance
      *
      * @param user
      * @param password
      * @param instanceName
      * @param zookeepers
      * @param appName
      * @return
      * @throws AccumuloException
      * @throws AccumuloSecurityException
      * @throws RepositoryException
      * @throws RyaDAOException
      * @throws NumberFormatException
      * @throws UnknownHostException
      * @throws InferenceEngineException
      */
    protected static RyaSailRepository setupRya(String user, String password, String instanceName, String zookeepers, String appName)
            throws AccumuloException, AccumuloSecurityException, RepositoryException, RyaDAOException,
            NumberFormatException, UnknownHostException, InferenceEngineException {

        checkNotNull(user);
        checkNotNull(password);
        checkNotNull(instanceName);
        checkNotNull(zookeepers);
        checkNotNull(appName);

        // Setup Rya configuration values.
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(RYA_TABLE_PREFIX);
        conf.setDisplayQueryPlan(true);
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, false);
        conf.set(ConfigUtils.CLOUDBASE_USER, user);
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, password);
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, instanceName);
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, zookeepers);
        conf.set(ConfigUtils.USE_PCJ, "true");
        conf.set(ConfigUtils.USE_PCJ_FLUO_UPDATER, "true");
        conf.set(ConfigUtils.FLUO_APP_NAME, appName);
        conf.set(ConfigUtils.PCJ_STORAGE_TYPE,
                PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType.ACCUMULO.toString());
        conf.set(ConfigUtils.PCJ_UPDATER_TYPE, PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType.FLUO.toString());
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "");

        Sail sail = RyaSailFactory.getInstance(conf);
        final RyaSailRepository ryaRepo = new RyaSailRepository(sail);
        ryaRepo.initialize();

        return ryaRepo;
    }

    /**
     * Override this method to provide an output configuration to the Fluo
     * application.
     * <p>
     * Returns an empty map by default.
     *
     * @return The parameters that will be passed to {@link QueryResultObserver}
     *         at startup.
     */
    protected Map<String, String> makeExportParams() {
        return new HashMap<>();
    }

    /**
     * Setup a Mini Fluo cluster that uses a temporary directory to store its
     * data.ll
     *
     * @return A Mini Fluo cluster.
     */
    protected MiniFluo startMiniFluo() throws AlreadyInitializedException, TableExistsException {
//        final File miniDataDir = Files.createTempDir();

        // Setup the observers that will be used by the Fluo PCJ Application.
        final List<ObserverConfiguration> observers = new ArrayList<>();
        observers.add(new ObserverConfiguration(TripleObserver.class.getName()));
        observers.add(new ObserverConfiguration(StatementPatternObserver.class.getName()));
        observers.add(new ObserverConfiguration(JoinObserver.class.getName()));
        observers.add(new ObserverConfiguration(FilterObserver.class.getName()));

        // Provide export parameters child test classes may provide to the
        // export observer.
        final ObserverConfiguration exportObserverConfig = new ObserverConfiguration(
                QueryResultObserver.class.getName());
        exportObserverConfig.setParameters(makeExportParams());
        observers.add(exportObserverConfig);

        // Configure how the mini fluo will run.
        final FluoConfiguration config = new FluoConfiguration();
        config.setMiniStartAccumulo(false);
        config.setAccumuloInstance(instanceName);
        config.setAccumuloUser(ACCUMULO_USER);
        config.setAccumuloPassword(ACCUMULO_PASSWORD);
        config.setInstanceZookeepers(zookeepers + "/fluo");
        config.setAccumuloZookeepers(zookeepers);
        
        config.setApplicationName(appName);
        config.setAccumuloTable("fluo" + appName);
        
        config.addObservers(observers);

        FluoFactory.newAdmin(config).initialize( 
        		new FluoAdmin.InitOpts().setClearTable(true).setClearZookeeper(true) );
        return FluoFactory.newMiniFluo(config);
    }
}