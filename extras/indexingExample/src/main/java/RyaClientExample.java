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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoAdmin.AlreadyInitializedException;
import org.apache.fluo.api.client.FluoAdmin.TableExistsException;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.log4j.*;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.indexing.accumulo.AccumuloIndexingConfiguration;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaExportParameters;
import org.apache.rya.indexing.pcj.fluo.app.observers.*;
import org.apache.rya.sail.config.RyaSailFactory;
import org.apache.zookeeper.ClientCnxn;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.queryrender.sparql.SPARQLQueryRenderer;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.SailConnection;

/**
 * Demonstrates how a {@link RyaClient} may be used to interact with an instance
 * of Accumulo to install and manage a Rya instance.
 */
public class RyaClientExample {
    private static final Logger log = Logger.getLogger(RyaClientExample.class);

    public static void main(final String[] args) throws Exception {
        setupLogging();

        final String accumuloUsername = "root";
        final String accumuloPassword = "password";

        MiniAccumuloCluster cluster = null;
        MiniFluo fluo = null;
        Sail ryaSail = null;

        try {
            // Setup a Mini Accumulo Cluster to host the Rya instance.
            log.info("Setting up the Mini Accumulo Cluster used by this example.");
            final File miniDataDir = Files.createTempDir();
            final MiniAccumuloConfig cfg = new MiniAccumuloConfig(miniDataDir, accumuloPassword);
            cluster = new MiniAccumuloCluster(cfg);
            cluster.start();

            // Setup a Mini Fluo application that will be used to incrementally update the PCJ indicies.
            log.info("Setting up the Mini Fluo application used by this example.");
            final String fluoAppName = "demoInstance_pcjUpdater";
            fluo = makeMiniFluo(accumuloUsername, accumuloPassword, cluster.getInstanceName(), cluster.getZooKeepers(), fluoAppName);

            // Give the root user the 'U' authorizations.
            final Connector connector = cluster.getConnector(accumuloUsername, accumuloPassword);
            connector.securityOperations().changeUserAuthorizations("root", new Authorizations("U"));

            // Setup a Rya Client that is able to interact with the mini cluster.
            final AccumuloConnectionDetails connectionDetails =
                    new AccumuloConnectionDetails(accumuloUsername, accumuloPassword.toCharArray(), cluster.getInstanceName(), cluster.getZooKeepers());

            final RyaClient ryaClient = AccumuloRyaClientFactory.build(connectionDetails, connector);

            // Install an instance of Rya that has all of the secondary indexers turned on.
            final String ryaInstanceName = "demoInstance_";
            final InstallConfiguration installConfig = InstallConfiguration.builder()
                    .setEnableTableHashPrefix(true)
                    .setEnableEntityCentricIndex(true)
                    .setEnableGeoIndex(true)
                    .setEnableFreeTextIndex(true)
                    .setEnableTemporalIndex(true)
                    .setEnablePcjIndex(true)
                    .setFluoPcjAppName(fluoAppName)
                    .build();

            ryaClient.getInstall().install(ryaInstanceName, installConfig);

            // Add a PCJ index.
            final String sparql =
                    "SELECT ?patron ?employee " +
                     "WHERE { " +
                         "?patron <http://talksTo> ?employee. " +
                         "?employee <http://worksAt> <http://CoffeeShop>. " +
                     "}";

            // Load some statements into the Rya instance.

            final AccumuloIndexingConfiguration conf = AccumuloIndexingConfiguration.builder()
                    .setAuths("U")
                    .setAccumuloUser(accumuloUsername)
                    .setAccumuloPassword(accumuloPassword)
                    .setAccumuloInstance(cluster.getInstanceName())
                    .setAccumuloZooKeepers(cluster.getZooKeepers())
                    .setRyaPrefix(ryaInstanceName)
                    .setPcjUpdaterFluoAppName(fluoAppName)
                    .build();

            ryaSail = RyaSailFactory.getInstance(conf);

            final ValueFactory vf = ryaSail.getValueFactory();
            final List<Statement> statements = Lists.newArrayList(
                    vf.createStatement(vf.createIRI("http://Eve"), vf.createIRI("http://talksTo"), vf.createIRI("http://Charlie")),
                    vf.createStatement(vf.createIRI("http://David"), vf.createIRI("http://talksTo"), vf.createIRI("http://Alice")),
                    vf.createStatement(vf.createIRI("http://Alice"), vf.createIRI("http://worksAt"), vf.createIRI("http://CoffeeShop")),
                    vf.createStatement(vf.createIRI("http://Bob"), vf.createIRI("http://worksAt"), vf.createIRI("http://CoffeeShop")),
                    vf.createStatement(vf.createIRI("http://George"), vf.createIRI("http://talksTo"), vf.createIRI("http://Frank")),
                    vf.createStatement(vf.createIRI("http://Frank"), vf.createIRI("http://worksAt"), vf.createIRI("http://CoffeeShop")),
                    vf.createStatement(vf.createIRI("http://Eve"), vf.createIRI("http://talksTo"), vf.createIRI("http://Bob")),
                    vf.createStatement(vf.createIRI("http://Charlie"), vf.createIRI("http://worksAt"), vf.createIRI("http://CoffeeShop")));

            SailConnection ryaConn = ryaSail.getConnection();
            log.info("");
            log.info("Loading the following statements:");
            ryaConn.begin();
            for(final Statement statement : statements) {
                log.info("    " + statement.toString());
                ryaConn.addStatement(statement.getSubject(), statement.getPredicate(), statement.getObject());
            }
            log.info("");
            ryaConn.close();
            fluo.waitForObservers();

            // Execute the SPARQL query and print the results.
            log.info("Executing the following query: ");
            prettyLogSparql(sparql);
            log.info("");

            final ParsedQuery parsedQuery = new SPARQLParser().parseQuery(sparql, null);
            ryaConn = ryaSail.getConnection();
            final CloseableIteration<? extends BindingSet, QueryEvaluationException> result = ryaConn.evaluate(parsedQuery.getTupleExpr(), null, null, false);

            log.info("Results:");
            while(result.hasNext()) {
                log.info("    " + result.next());
            }
            log.info("");

        } finally {
            if(ryaSail != null) {
                log.info("Shutting down the Rya Sail instance.");
                ryaSail.shutDown();
            }

            if(fluo != null) {
                try {
                    log.info("Shutting down the Mini Fluo instance.");
                    fluo.close();
                } catch (final Exception e) {
                    log.error("Could not shut down the Mini Fluo instance.", e);
                }
            }

            if(cluster != null) {
                log.info("Sutting down the Mini Accumulo Cluster.");
                cluster.stop();
            }
        }
    }

    private static void setupLogging() {
        // Turn off all the loggers and customize how they write to the console.
        final Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.OFF);
        final ConsoleAppender ca = (ConsoleAppender) rootLogger.getAppender("stdout");
        ca.setLayout(new PatternLayout("%-5p - %m%n"));


        // Turn the logger used by the demo back on.
        log.setLevel(Level.INFO);
        Logger.getLogger(ClientCnxn.class).setLevel(Level.ERROR);
    }

    private static void prettyLogSparql(final String sparql) {
        try {
            // Pretty print.
            final String[] lines = prettyFormatSparql(sparql);
            for(final String line : lines) {
                log.info(line);
            }
        } catch (final Exception e) {
            // Pretty print failed, so ugly print instead.
            log.info(sparql);
        }
    }

    private static String[] prettyFormatSparql(final String sparql) throws Exception {
        final SPARQLParser parser = new SPARQLParser();
        final SPARQLQueryRenderer renderer = new SPARQLQueryRenderer();
        final ParsedQuery pq = parser.parseQuery(sparql, null);
        final String prettySparql = renderer.render(pq);
        return StringUtils.split(prettySparql, '\n');
    }

    private static MiniFluo makeMiniFluo(final String username, final String password, final String instanceName, final String zookeepers, final String fluoAppName) throws AlreadyInitializedException, TableExistsException {
        // Setup the observers that will be used by the Fluo PCJ Application.
        final List<ObserverSpecification> observers = new ArrayList<>();
        observers.add(new ObserverSpecification(TripleObserver.class.getName()));
        observers.add(new ObserverSpecification(StatementPatternObserver.class.getName()));
        observers.add(new ObserverSpecification(JoinObserver.class.getName()));
        observers.add(new ObserverSpecification(FilterObserver.class.getName()));

        // Provide export parameters child test classes may provide to the
        // export observer.
        final HashMap<String, String> params = new HashMap<>();
        final RyaExportParameters ryaParams = new RyaExportParameters(params);
        ryaParams.setUseRyaBindingSetExporter(true);
        ryaParams.setAccumuloInstanceName(instanceName);
        ryaParams.setZookeeperServers(zookeepers);
        ryaParams.setExporterUsername(username);
        ryaParams.setExporterPassword(password);
        ryaParams.setRyaInstanceName(fluoAppName);

        final ObserverSpecification exportObserverConfig = new ObserverSpecification(
                QueryResultObserver.class.getName(), params);
        observers.add(exportObserverConfig);

        // Configure how the mini fluo will run.
        final FluoConfiguration config = new FluoConfiguration();
        config.setMiniStartAccumulo(false);
        config.setAccumuloInstance(instanceName);
        config.setAccumuloUser(username);
        config.setAccumuloPassword(password);
        config.setInstanceZookeepers(zookeepers + "/fluo");
        config.setAccumuloZookeepers(zookeepers);

        config.setApplicationName(fluoAppName);
        config.setAccumuloTable("fluo" + fluoAppName);

        config.addObservers(observers);

        FluoFactory.newAdmin(config).initialize(
                new FluoAdmin.InitializationOptions().setClearTable(true).setClearZookeeper(true) );
        return FluoFactory.newMiniFluo(config);
    }
}
