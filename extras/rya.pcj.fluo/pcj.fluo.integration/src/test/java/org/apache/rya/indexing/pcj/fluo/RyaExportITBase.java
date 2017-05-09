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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.recipes.test.AccumuloExportITBase;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaExportParameters;
import org.apache.rya.indexing.pcj.fluo.app.observers.AggregationObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.sail.config.RyaSailFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.openrdf.sail.Sail;

/**
 * The base Integration Test class used for Fluo applications that export to a Rya PCJ Index.
 */
public class RyaExportITBase extends AccumuloExportITBase {

    protected static final String RYA_INSTANCE_NAME = "test_";

    private RyaSailRepository ryaSailRepo = null;

    public RyaExportITBase() {
        // Indicates that MiniFluo should be started before each test.
        super(true);
    }

    @BeforeClass
    public static void setupLogging() {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);
    }

    @Override
    protected void preFluoInitHook() throws Exception {
        // Setup the observers that will be used by the Fluo PCJ Application.
        final List<ObserverSpecification> observers = new ArrayList<>();
        observers.add(new ObserverSpecification(TripleObserver.class.getName()));
        observers.add(new ObserverSpecification(StatementPatternObserver.class.getName()));
        observers.add(new ObserverSpecification(JoinObserver.class.getName()));
        observers.add(new ObserverSpecification(FilterObserver.class.getName()));
        observers.add(new ObserverSpecification(AggregationObserver.class.getName()));

        // Configure the export observer to export new PCJ results to the mini accumulo cluster.
        final HashMap<String, String> exportParams = new HashMap<>();
        final RyaExportParameters ryaParams = new RyaExportParameters(exportParams);
        ryaParams.setExportToRya(true);
        ryaParams.setRyaInstanceName(RYA_INSTANCE_NAME);
        ryaParams.setAccumuloInstanceName(super.getMiniAccumuloCluster().getInstanceName());
        ryaParams.setZookeeperServers(super.getMiniAccumuloCluster().getZooKeepers());
        ryaParams.setExporterUsername(ACCUMULO_USER);
        ryaParams.setExporterPassword(ACCUMULO_PASSWORD);

        final ObserverSpecification exportObserverConfig = new ObserverSpecification(QueryResultObserver.class.getName(), exportParams);
        observers.add(exportObserverConfig);

        // Add the observers to the Fluo Configuration.
        super.getFluoConfiguration().addObservers(observers);
    }

    @Before
    public void setupRya() throws Exception {
        final MiniAccumuloCluster cluster = super.getMiniAccumuloCluster();
        final String instanceName = cluster.getInstanceName();
        final String zookeepers = cluster.getZooKeepers();

        // Install the Rya instance to the mini accumulo cluster.
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(
                    ACCUMULO_USER,
                    ACCUMULO_PASSWORD.toCharArray(),
                    instanceName,
                    zookeepers),
                super.getAccumuloConnector());

        ryaClient.getInstall().install(RYA_INSTANCE_NAME, InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setEnableFreeTextIndex(false)
                .setEnableEntityCentricIndex(false)
                .setEnableGeoIndex(false)
                .setEnableTemporalIndex(false)
                .setEnablePcjIndex(true)
                .setFluoPcjAppName( super.getFluoConfiguration().getApplicationName() )
                .build());

        // Connect to the Rya instance that was just installed.
        final AccumuloRdfConfiguration conf = makeConfig(instanceName, zookeepers);
        final Sail sail = RyaSailFactory.getInstance(conf);
        ryaSailRepo = new RyaSailRepository(sail);
    }

    @After
    public void teardownRya() throws Exception {
        final MiniAccumuloCluster cluster = super.getMiniAccumuloCluster();
        final String instanceName = cluster.getInstanceName();
        final String zookeepers = cluster.getZooKeepers();

        // Uninstall the instance of Rya.
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(
                    ACCUMULO_USER,
                    ACCUMULO_PASSWORD.toCharArray(),
                    instanceName,
                    zookeepers),
                super.getAccumuloConnector());

        ryaClient.getUninstall().uninstall(RYA_INSTANCE_NAME);

        // Shutdown the repo.
        ryaSailRepo.shutDown();
    }

    /**
     * @return A {@link RyaSailRepository} that is connected to the Rya instance that statements are loaded into.
     */
    protected RyaSailRepository getRyaSailRepository() throws Exception {
        return ryaSailRepo;
    }

    protected AccumuloRdfConfiguration makeConfig(final String instanceName, final String zookeepers) {
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(RYA_INSTANCE_NAME);

        // Accumulo connection information.
        conf.setAccumuloUser(AccumuloExportITBase.ACCUMULO_USER);
        conf.setAccumuloPassword(AccumuloExportITBase.ACCUMULO_PASSWORD);
        conf.setAccumuloInstance(super.getAccumuloConnector().getInstance().getInstanceName());
        conf.setAccumuloZookeepers(super.getAccumuloConnector().getInstance().getZooKeepers());
        conf.setAuths("");

        // PCJ configuration information.
        conf.set(ConfigUtils.USE_PCJ, "true");
        conf.set(ConfigUtils.USE_PCJ_UPDATER_INDEX, "true");
        conf.set(ConfigUtils.FLUO_APP_NAME, super.getFluoConfiguration().getApplicationName());
        conf.set(ConfigUtils.PCJ_STORAGE_TYPE,
                PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType.ACCUMULO.toString());
        conf.set(ConfigUtils.PCJ_UPDATER_TYPE,
                PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType.FLUO.toString());

        conf.setDisplayQueryPlan(true);

        return conf;
    }
}