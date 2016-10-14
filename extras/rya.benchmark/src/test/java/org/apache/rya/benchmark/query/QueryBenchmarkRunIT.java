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
package org.apache.rya.benchmark.query;

import java.io.File;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rya.benchmark.query.QueryBenchmark.QueryBenchmarkRun;
import org.apache.rya.benchmark.query.QueryBenchmark.QueryBenchmarkRun.NotEnoughResultsException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.zookeeper.ClientCnxn;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType;
import org.apache.rya.sail.config.RyaSailFactory;

/**
 * Integration tests {@link QueryBenchmarkRun}.
 */
public class QueryBenchmarkRunIT {
    private static final Logger log = Logger.getLogger(QueryBenchmarkRunIT.class);

    private static final String RYA_INSTANCE_NAME = "test_";
    private static final String ACCUMULO_USER = "root";
    private static final String ACCUMULO_PASSWORD = "password";
    private static final String SPARQL_QUERY = "SELECT ?name WHERE { ?name <urn:likes> <urn:icecream> . ?name <urn:hasEyeColor> <urn:blue> . }";

    private static MiniAccumuloCluster cluster = null;
    private static Sail sail = null;

    @BeforeClass
    public static void setup() throws Exception {
        // Squash loud logs.
        Logger.getLogger(ClientCnxn.class).setLevel(Level.ERROR);

        // Setup the Mini Accumulo Cluster.
        final File miniDataDir = com.google.common.io.Files.createTempDir();
        final MiniAccumuloConfig cfg = new MiniAccumuloConfig( miniDataDir, ACCUMULO_PASSWORD);
        cluster = new MiniAccumuloCluster(cfg);
        cluster.start();

        // Create a Rya Client connected to the Mini Accumulo Cluster.
        final AccumuloConnectionDetails connDetails = new AccumuloConnectionDetails(
                ACCUMULO_USER,
                ACCUMULO_PASSWORD.toCharArray(),
                cluster.getInstanceName(),
                cluster.getZooKeepers());
        final Connector connector = cluster.getConnector(ACCUMULO_USER, ACCUMULO_PASSWORD);
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(connDetails, connector);

        // Install an instance of Rya on the mini cluster.
        installRya(ryaClient);

        // Get a Sail object that is backed by the Rya store that is on the mini cluster.
        final AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();
        ryaConf.setTablePrefix(RYA_INSTANCE_NAME);
        ryaConf.set(ConfigUtils.CLOUDBASE_USER, ACCUMULO_USER);
        ryaConf.set(ConfigUtils.CLOUDBASE_PASSWORD, ACCUMULO_PASSWORD);
        ryaConf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, cluster.getZooKeepers());
        ryaConf.set(ConfigUtils.CLOUDBASE_INSTANCE, cluster.getInstanceName());
        ryaConf.set(ConfigUtils.USE_PCJ, "true");
        ryaConf.set(ConfigUtils.PCJ_STORAGE_TYPE, PrecomputedJoinStorageType.ACCUMULO.toString());
        ryaConf.set(ConfigUtils.PCJ_UPDATER_TYPE, PrecomputedJoinUpdaterType.NO_UPDATE.toString());

        sail = RyaSailFactory.getInstance( ryaConf );

        // Load some data into the cluster that will match the query we're testing against.
        loadTestStatements();

        // Add a PCJ to the application that summarizes the query.
        createTestPCJ(ryaClient);
    }

    private static void installRya(final RyaClient ryaClient) throws Exception {
        // Use the client to install the instance of Rya that will be used for the tests.
        ryaClient.getInstall().install(RYA_INSTANCE_NAME, InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setEnableGeoIndex(false)
                .setEnableTemporalIndex(false)
                .setEnableFreeTextIndex(false)
                .setEnableEntityCentricIndex(false)
                .setEnablePcjIndex(true)
                .build());
    }

    private static void loadTestStatements() throws Exception {
        final ValueFactory vf = sail.getValueFactory();

        final SailConnection sailConn = sail.getConnection();
        sailConn.begin();
        sailConn.addStatement(vf.createURI("urn:Alice"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
        sailConn.addStatement(vf.createURI("urn:Bob"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
        sailConn.addStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
        sailConn.addStatement(vf.createURI("urn:David"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
        sailConn.addStatement(vf.createURI("urn:Eve"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
        sailConn.addStatement(vf.createURI("urn:Frank"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
        sailConn.addStatement(vf.createURI("urn:George"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));
        sailConn.addStatement(vf.createURI("urn:Hillary"), vf.createURI("urn:likes"), vf.createURI("urn:icecream"));

        sailConn.addStatement(vf.createURI("urn:Alice"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:blue"));
        sailConn.addStatement(vf.createURI("urn:Bob"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:blue"));
        sailConn.addStatement(vf.createURI("urn:Charlie"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:blue"));
        sailConn.addStatement(vf.createURI("urn:David"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:blue"));
        sailConn.addStatement(vf.createURI("urn:Eve"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:blue"));
        sailConn.addStatement(vf.createURI("urn:Frank"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:blue"));
        sailConn.addStatement(vf.createURI("urn:George"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:green"));
        sailConn.addStatement(vf.createURI("urn:Hillary"), vf.createURI("urn:hasEyeColor"), vf.createURI("urn:brown"));
        sailConn.commit();
        sailConn.close();
    }

    private static void createTestPCJ(final RyaClient ryaClient) throws Exception {
        // Create an empty PCJ within the Rya instance's PCJ storage for the test query.
        final PrecomputedJoinStorage pcjs = new AccumuloPcjStorage(cluster.getConnector(ACCUMULO_USER, ACCUMULO_PASSWORD), RYA_INSTANCE_NAME);
        final String pcjId = pcjs.createPcj(SPARQL_QUERY);


        // Batch update the PCJ using the Rya Client.
        ryaClient.getBatchUpdatePCJ().batchUpdate(RYA_INSTANCE_NAME, pcjId);
    }

    @AfterClass
    public static void teardown() {
        if(sail != null) {
            try {
                log.info("Shutting down the Sail.");
                sail.shutDown();
            } catch (final SailException e) {
                log.error("Could not shut down the Sail.", e);
            }
        }

        if(cluster != null) {
            try {
                log.info("Shutting down the mini accumulo cluster.");
                cluster.stop();
            } catch (final Exception e) {
                log.error("Could not shut down the mini accumulo cluster.", e);
            }
        }
    }

    @Test
    public void read1() throws Exception {
        new QueryBenchmarkRun(sail.getConnection(), SPARQL_QUERY, 1L).run();
    }

    @Test
    public void read5() throws Exception {
        new QueryBenchmarkRun(sail.getConnection(), SPARQL_QUERY, 5L).run();
    }

    @Test(expected = NotEnoughResultsException.class)
    public void read10() throws Exception {
        new QueryBenchmarkRun(sail.getConnection(), SPARQL_QUERY, 10L).run();
    }

    @Test
    public void readAll() throws Exception {
        new QueryBenchmarkRun(sail.getConnection(), SPARQL_QUERY).run();
    }
}