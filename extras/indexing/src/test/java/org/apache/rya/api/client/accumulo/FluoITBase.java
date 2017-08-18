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
package org.apache.rya.api.client.accumulo;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.MiniAccumuloClusterInstance;
import org.apache.rya.accumulo.MiniAccumuloSingleton;
import org.apache.rya.accumulo.RyaTestInstanceRule;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaExportParameters;
import org.apache.rya.indexing.pcj.fluo.app.observers.ConstructQueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.ProjectionObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver;
import org.apache.zookeeper.ClientCnxn;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoAdmin.AlreadyInitializedException;
import org.apache.fluo.api.client.FluoAdmin.TableExistsException;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.apache.rya.sail.config.RyaSailFactory;

/**
 * Integration tests that ensure the Fluo application processes PCJs results
 * correctly.
 * <p>
 * This class is being ignored because it doesn't contain any unit tests.
 */
public abstract class FluoITBase {
    private static final Logger log = Logger.getLogger(FluoITBase.class);

    // Mini Accumulo Cluster
    private static MiniAccumuloClusterInstance clusterInstance = MiniAccumuloSingleton.getInstance();
    private static MiniAccumuloCluster cluster;

    private static String instanceName = null;
    private static String zookeepers = null;

    protected static Connector accumuloConn = null;

    // Fluo data store and connections.
    protected MiniFluo fluo = null;
    protected FluoClient fluoClient = null;

    // Rya data store and connections.
    protected RyaSailRepository ryaRepo = null;
    protected RepositoryConnection ryaConn = null;

    @Rule
    public RyaTestInstanceRule testInstance = new RyaTestInstanceRule(false);

    @BeforeClass
    public static void beforeClass() throws Exception {
        Logger.getLogger(ClientCnxn.class).setLevel(Level.ERROR);

        // Setup and start the Mini Accumulo.
        cluster = clusterInstance.getCluster();

        // Store a connector to the Mini Accumulo.
        instanceName = cluster.getInstanceName();
        zookeepers = cluster.getZooKeepers();

        final Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
        accumuloConn = instance.getConnector(clusterInstance.getUsername(), new PasswordToken(clusterInstance.getPassword()));
    }

    @Before
    public void setupMiniResources() throws Exception {
        // Initialize the Mini Fluo that will be used to store created queries.
        fluo = startMiniFluo();
        fluoClient = FluoFactory.newClient(fluo.getClientConfiguration());

        // Initialize the Rya that will be used by the tests.
        ryaRepo = setupRya();
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

    }

    public String getRyaInstanceName() {
        return testInstance.getRyaInstanceName();
    }

    public AccumuloConnectionDetails createConnectionDetails() {
        return new AccumuloConnectionDetails(
                clusterInstance.getUsername(),
                clusterInstance.getPassword().toCharArray(),
                clusterInstance.getInstanceName(),
                clusterInstance.getZookeepers());
    }

    /**
     * Override this method to provide an output configuration to the Fluo application.
     * <p>
     * Exports to the Rya instance by default.
     *
     * @return The parameters that will be passed to {@link QueryResultObserver} at startup.
     */
    protected Map<String, String> makeExportParams() {
        final HashMap<String, String> params = new HashMap<>();

        final RyaExportParameters ryaParams = new RyaExportParameters(params);
        ryaParams.setExportToRya(true);
        ryaParams.setAccumuloInstanceName(instanceName);
        ryaParams.setZookeeperServers(zookeepers);
        ryaParams.setExporterUsername(clusterInstance.getUsername());
        ryaParams.setExporterPassword(clusterInstance.getPassword());
        ryaParams.setRyaInstanceName(getRyaInstanceName());
        return params;
    }

    /**
     * Setup a Mini Fluo cluster that uses a temporary directory to store its
     * data.
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
        observers.add(new ObserverSpecification(ProjectionObserver.class.getName()));
        observers.add(new ObserverSpecification(ConstructQueryResultObserver.class.getName()));

        // Provide export parameters child test classes may provide to the
        // export observer.
        final ObserverSpecification exportObserverConfig = new ObserverSpecification(
                QueryResultObserver.class.getName(), makeExportParams());
        observers.add(exportObserverConfig);

        // Configure how the mini fluo will run.
        final FluoConfiguration config = new FluoConfiguration();
        config.setMiniStartAccumulo(false);
        config.setAccumuloInstance(instanceName);
        config.setAccumuloUser(clusterInstance.getUsername());
        config.setAccumuloPassword(clusterInstance.getPassword());
        config.setInstanceZookeepers(zookeepers + "/fluo");
        config.setAccumuloZookeepers(zookeepers);

        config.setApplicationName(getRyaInstanceName());
        config.setAccumuloTable("fluo" + getRyaInstanceName());

        config.addObservers(observers);

        FluoFactory.newAdmin(config).initialize(
                new FluoAdmin.InitializationOptions().setClearTable(true).setClearZookeeper(true) );
        return FluoFactory.newMiniFluo(config);
    }

    /**
     * Sets up a Rya instance.
     */
   protected RyaSailRepository setupRya()
           throws AccumuloException, AccumuloSecurityException, RepositoryException, RyaDAOException,
           NumberFormatException, UnknownHostException, InferenceEngineException, AlreadyInitializedException,
           RyaDetailsRepositoryException, DuplicateInstanceNameException, RyaClientException, SailException {
       checkNotNull(instanceName);
       checkNotNull(zookeepers);

       // Setup Rya configuration values.
       final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
       conf.setTablePrefix(getRyaInstanceName());
       conf.setDisplayQueryPlan(true);
       conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, false);
       conf.set(ConfigUtils.CLOUDBASE_USER, clusterInstance.getUsername());
       conf.set(ConfigUtils.CLOUDBASE_PASSWORD, clusterInstance.getPassword());
       conf.set(ConfigUtils.CLOUDBASE_INSTANCE, clusterInstance.getInstanceName());
       conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, clusterInstance.getZookeepers());
       conf.set(ConfigUtils.USE_PCJ, "true");
       conf.set(ConfigUtils.FLUO_APP_NAME, getRyaInstanceName());
       conf.set(ConfigUtils.PCJ_STORAGE_TYPE, PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType.ACCUMULO.toString());
       conf.set(ConfigUtils.PCJ_UPDATER_TYPE, PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType.FLUO.toString());
       conf.set(ConfigUtils.CLOUDBASE_AUTHS, "");

       // Install the test instance of Rya.
       final Install install = new AccumuloInstall(createConnectionDetails(), accumuloConn);

       final InstallConfiguration installConfig = InstallConfiguration.builder()
               .setEnableTableHashPrefix(true)
               .setEnableEntityCentricIndex(true)
               .setEnableFreeTextIndex(true)
               .setEnableTemporalIndex(true)
               .setEnablePcjIndex(true)
               .setEnableGeoIndex(true)
               .setFluoPcjAppName(getRyaInstanceName())
               .build();
       install.install(getRyaInstanceName(), installConfig);

       // Connect to the instance of Rya that was just installed.
       final Sail sail = RyaSailFactory.getInstance(conf);
       final RyaSailRepository ryaRepo = new RyaSailRepository(sail);

       return ryaRepo;
   }
}