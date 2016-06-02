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
package org.apache.rya.indexing.pcj.fluo.demo;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaExportParameters;
import org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver;
import org.apache.rya.indexing.pcj.fluo.demo.Demo.DemoExecutionException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.google.common.io.Files;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.mini.MiniFluo;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.rdftriplestore.RdfCloudTripleStore;
import mvm.rya.rdftriplestore.RyaSailRepository;

/**
 * Runs {@link Demo}s that require Rya and Fluo.
 */
public class DemoDriver {
    private static final Logger log = Logger.getLogger(DemoDriver.class);

    private static final String RYA_TABLE_PREFIX = "demo_";

    public static final String USE_MOCK_INSTANCE = ".useMockInstance";
    public static final String CLOUDBASE_INSTANCE = "sc.cloudbase.instancename";
    public static final String CLOUDBASE_USER = "sc.cloudbase.username";
    public static final String CLOUDBASE_PASSWORD = "sc.cloudbase.password";

    // Rya data store and connections.
    private static MiniAccumuloCluster accumulo = null;
    private static Connector accumuloConn = null;
    private static RyaSailRepository ryaRepo = null;
    private static RepositoryConnection ryaConn = null;

    // Fluo data store and connections.
    private static MiniFluo fluo = null;
    private static FluoClient fluoClient = null;

    public static void main(final String[] args) {
        setupLogging();

        // Setup the resources required to run the demo.
        try {
            log.info("Initializing resources used by the demo...");
            setupResources();
        } catch (final DemoInitializationException e) {
            log.error("Could not initialize the demo's resources. Exiting.", e);
            System.exit(-1);
        }
        log.info("");

        // Run the demo.
        try {
            new FluoAndHistoricPcjsDemo().execute(accumulo, accumuloConn, RYA_TABLE_PREFIX, ryaRepo, ryaConn, fluo, fluoClient);
        } catch (final DemoExecutionException e) {
            log.error("An exception was thrown durring demo execution. The demo can not continue.", e);
        }

        // Tear down the demo environment.
        log.info("Shutting down the demo...");
        shutdownResources();
        log.info("Demo exiting.");
    }

    private static void setupLogging() {
        // Turn off all the loggers and customize how they write to the console.
        final Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.OFF);
        final ConsoleAppender ca = (ConsoleAppender) rootLogger.getAppender("stdout");
        ca.setLayout(new PatternLayout("%-5p - %m%n"));


        // Turn the loggers used by the demo back on.
        log.setLevel(Level.INFO);
    }

    /**
     * Indicates a problem while initializing the demo's resources prevented it from starting.
     */
    private static final class DemoInitializationException extends Exception {
        private static final long serialVersionUID = 1L;

        public DemoInitializationException(final String message, final Exception cause) {
            super(message, cause);
        }
    }

    private static void setupResources() throws DemoInitializationException {
        try{
            // Initialize the Mini Accumulo that will be used to store Triples and get a connection to it.
            log.debug("Starting up the Mini Accumulo Cluster used by Rya.");
            accumulo = startMiniAccumulo();

            // Setup the Rya library to use the Mini Accumulo.
            log.debug("Starting up the Rya Repository.");
            ryaRepo = setupRya(accumulo);
            ryaConn = ryaRepo.getConnection();

            // Initialize the Mini Fluo that will be used to store created queries.
            log.debug("Starting up the Mini Fluo instance.");
            fluo = startMiniFluo();
            fluoClient = FluoFactory.newClient( fluo.getClientConfiguration() );
        } catch(final Exception e) {
            throw new DemoInitializationException("Could not run the demo because of a problem while initializing the mini resources.", e);
        }
    }

    private static void shutdownResources() {
        if(ryaConn != null) {
            try {
                log.debug("Shutting down Rya Connection.");
                ryaConn.close();
            } catch(final Exception e) {
                log.error("Could not shut down the Rya Connection.", e);
            }
        }

        if(ryaRepo != null) {
            try {
                log.debug("Shutting down Rya Repo.");
                ryaRepo.shutDown();
            } catch(final Exception e) {
                log.error("Could not shut down the Rya Repo.", e);
            }
        }

        if(accumulo != null) {
            try {
                log.debug("Shutting down the Mini Accumulo being used as a Rya store.");
                accumulo.stop();
            } catch(final Exception e) {
                log.error("Could not shut down the Mini Accumulo.", e);
            }
        }

        if(fluoClient != null) {
            try {
                log.debug("Shutting down Fluo Client.");
                fluoClient.close();
            } catch(final Exception e) {
                log.error("Could not shut down the Fluo Client.", e);
            }
        }

        if(fluo != null) {
            try {
                log.debug("Shutting down Mini Fluo.");
                fluo.close();
            } catch (final Exception e) {
                log.error("Could not shut down the Mini Fluo.", e);
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

        conf.setBoolean(USE_MOCK_INSTANCE, true);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, RYA_TABLE_PREFIX);
        conf.set(CLOUDBASE_USER, "root");
        conf.set(CLOUDBASE_PASSWORD, "password");
        conf.set(CLOUDBASE_INSTANCE, accumulo.getInstanceName());

        crdfdao.setConf(conf);
        ryaStore.setRyaDAO(crdfdao);

        final RyaSailRepository ryaRepo = new RyaSailRepository(ryaStore);
        ryaRepo.initialize();

        return ryaRepo;
    }

    /**
     * Setup a Mini Fluo cluster that uses a temporary directory to store its data.ll
     *
     * @return A Mini Fluo cluster.
     */
    private static MiniFluo startMiniFluo() {
        final File miniDataDir = Files.createTempDir();

        // Setup the observers that will be used by the Fluo PCJ Application.
        final List<ObserverConfiguration> observers = new ArrayList<>();
        observers.add(new ObserverConfiguration(TripleObserver.class.getName()));
        observers.add(new ObserverConfiguration(StatementPatternObserver.class.getName()));
        observers.add(new ObserverConfiguration(JoinObserver.class.getName()));
        observers.add(new ObserverConfiguration(FilterObserver.class.getName()));

        // Provide export parameters child test classes may provide to the export observer.
        final HashMap<String, String> exportParams = new HashMap<>();
        final RyaExportParameters ryaParams = new RyaExportParameters(exportParams);
        ryaParams.setExportToRya(true);
        ryaParams.setAccumuloInstanceName(accumulo.getInstanceName());
        ryaParams.setZookeeperServers(accumulo.getZooKeepers());
        ryaParams.setExporterUsername("root");
        ryaParams.setExporterPassword("password");

        final ObserverConfiguration exportObserverConfig = new ObserverConfiguration(QueryResultObserver.class.getName());
        exportObserverConfig.setParameters( exportParams );
        observers.add(exportObserverConfig);

        // Configure how the mini fluo will run.
        final FluoConfiguration config = new FluoConfiguration();
        config.setApplicationName("IntegrationTests");
        config.setMiniDataDir(miniDataDir.getAbsolutePath());
        config.addObservers(observers);

        final MiniFluo miniFluo = FluoFactory.newMiniFluo(config);
        return miniFluo;
    }
}