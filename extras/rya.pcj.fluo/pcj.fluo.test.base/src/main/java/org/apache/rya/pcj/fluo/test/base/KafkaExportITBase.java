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
package org.apache.rya.pcj.fluo.test.base;

import static java.util.Objects.requireNonNull;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.core.util.PortUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaExportParameters;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.RyaSubGraphKafkaSerDe;
import org.apache.rya.indexing.pcj.fluo.app.observers.AggregationObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.ConstructQueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.sail.config.RyaSailFactory;
import org.junit.After;
import org.junit.Before;
import org.openrdf.model.Statement;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;

/**
 * The base Integration Test class used for Fluo applications that export to a
 * Kakfa topic.
 * <p>
 * Note, to reduce the amount of garbage in the logs, you can run with
 * -Djava.net.preferIPv4Stack=true to prevent attempting to resolve localhost to an ipv6 address.
 */
public class KafkaExportITBase extends ModifiedAccumuloExportITBase {

    private static final Logger logger = LoggerFactory.getLogger(KafkaExportITBase.class);

    protected static final String RYA_INSTANCE_NAME = "test_";

    private KafkaServer kafkaServer;
    private static final String BROKERHOST = "127.0.0.1";
    private String brokerPort;


    // The Rya instance statements are written to that will be fed into the Fluo
    // app.
    private RyaSailRepository ryaSailRepo = null;
    private AccumuloRyaDAO dao = null;

    /**
     * @return A new Property object containing the correct value for Kafka's
     *         {@link CommonClientConfigs#BOOTSTRAP_SERVERS_CONFIG}.
     */
    protected Properties createBootstrapServerConfig() {
        final Properties config = new Properties();
        config.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BROKERHOST + ":" + brokerPort);
        return config;
    }

    /**
     * Add info about the Kafka queue/topic to receive the export.
     */
    @Override
    protected void preFluoInitHook() throws Exception {
        // Setup the observers that will be used by the Fluo PCJ Application.
        final List<ObserverSpecification> observers = new ArrayList<>();
        observers.add(new ObserverSpecification(TripleObserver.class.getName()));
        observers.add(new ObserverSpecification(StatementPatternObserver.class.getName()));
        observers.add(new ObserverSpecification(JoinObserver.class.getName()));
        observers.add(new ObserverSpecification(FilterObserver.class.getName()));
        observers.add(new ObserverSpecification(AggregationObserver.class.getName()));

        // Configure the export observer to export new PCJ results to the mini
        // accumulo cluster.
        final HashMap<String, String> exportParams = new HashMap<>();

        final KafkaExportParameters kafkaParams = new KafkaExportParameters(exportParams);
        kafkaParams.setExportToKafka(true);

        // Configure the Kafka Producer
        final Properties producerConfig = createBootstrapServerConfig();
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.rya.indexing.pcj.fluo.app.export.kafka.KryoVisibilityBindingSetSerializer");
        kafkaParams.addAllProducerConfig(producerConfig);

        final ObserverSpecification exportObserverConfig = new ObserverSpecification(QueryResultObserver.class.getName(), exportParams);
        observers.add(exportObserverConfig);

        //create construct query observer and tell it not to export to Kafka
        //it will only add results back into Fluo
        final HashMap<String, String> constructParams = new HashMap<>();
        final KafkaExportParameters kafkaConstructParams = new KafkaExportParameters(constructParams);
        kafkaConstructParams.setExportToKafka(true);

        // Configure the Kafka Producer
        final Properties constructProducerConfig = createBootstrapServerConfig();
        constructProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        constructProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RyaSubGraphKafkaSerDe.class.getName());
        kafkaConstructParams.addAllProducerConfig(constructProducerConfig);

        final ObserverSpecification constructExportObserverConfig = new ObserverSpecification(ConstructQueryResultObserver.class.getName(),
                constructParams);
        observers.add(constructExportObserverConfig);

        // Add the observers to the Fluo Configuration.
        super.getFluoConfiguration().addObservers(observers);
    }


    @Override
    @Before
    public void setupMiniFluo() throws Exception {
        setupKafka();
        super.setupMiniFluo();
        installRyaInstance();
    }

    public void setupKafka() throws Exception {
        // grab the connection string for the zookeeper spun up by our parent class.
        final String zkConnect = getMiniAccumuloCluster().getZooKeepers();

        // setup Broker
        brokerPort = Integer.toString(PortUtils.getRandomFreePort());
        final Properties brokerProps = new Properties();
        brokerProps.setProperty(KafkaConfig$.MODULE$.BrokerIdProp(), "0");
        brokerProps.setProperty(KafkaConfig$.MODULE$.HostNameProp(), BROKERHOST);
        brokerProps.setProperty(KafkaConfig$.MODULE$.PortProp(), brokerPort);
        brokerProps.setProperty(KafkaConfig$.MODULE$.ZkConnectProp(), zkConnect);
        brokerProps.setProperty(KafkaConfig$.MODULE$.LogDirsProp(), Files.createTempDirectory(getClass().getSimpleName()+"-").toAbsolutePath().toString());
        final KafkaConfig config = new KafkaConfig(brokerProps);

        final Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        logger.info("Created a Kafka Server: ", config);
    }

    @After
    public void teardownRya() {
        // Uninstall the instance of Rya.
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(super.createConnectionDetails(), super.getAccumuloConnector());

        try {
            ryaClient.getUninstall().uninstall(RYA_INSTANCE_NAME);
            // Shutdown the repo.
            if(ryaSailRepo != null) {ryaSailRepo.shutDown();}
            if(dao != null ) {dao.destroy();}
        } catch (final Exception e) {
            logger.warn("Encountered an exception when shutting down Rya.", e);
        }
    }

    private void installRyaInstance() throws Exception {
        final AccumuloConnectionDetails details = super.createConnectionDetails();

        // Install the Rya instance to the mini accumulo cluster.
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(details,
                super.getAccumuloConnector());

        ryaClient.getInstall().install(RYA_INSTANCE_NAME,
                InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setEnableFreeTextIndex(false)
                .setEnableEntityCentricIndex(false)
                .setEnableGeoIndex(false)
                .setEnableTemporalIndex(false)
                .setEnablePcjIndex(true)
                .setFluoPcjAppName(super.getFluoConfiguration().getApplicationName())
                .build());

        // Connect to the Rya instance that was just installed.
        final AccumuloRdfConfiguration conf = makeConfig(details);
        final Sail sail = RyaSailFactory.getInstance(conf);
        dao = RyaSailFactory.getAccumuloDAOWithUpdatedConfig(conf);
        ryaSailRepo = new RyaSailRepository(sail);
    }

    protected AccumuloRdfConfiguration makeConfig(final AccumuloConnectionDetails details) {
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(RYA_INSTANCE_NAME);

        // Accumulo connection information.
        conf.setAccumuloUser(details.getUsername());
        conf.setAccumuloPassword(new String(details.getPassword()));
        conf.setAccumuloInstance(details.getInstanceName());
        conf.setAccumuloZookeepers(details.getZookeepers());
        conf.setAuths("");

        // PCJ configuration information.
        conf.set(ConfigUtils.USE_PCJ, "true");
        conf.set(ConfigUtils.USE_PCJ_UPDATER_INDEX, "true");
        conf.set(ConfigUtils.FLUO_APP_NAME, super.getFluoConfiguration().getApplicationName());
        conf.set(ConfigUtils.PCJ_STORAGE_TYPE, PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType.ACCUMULO.toString());
        conf.set(ConfigUtils.PCJ_UPDATER_TYPE, PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType.FLUO.toString());

        conf.setDisplayQueryPlan(true);

        return conf;
    }

    /**
     * @return A {@link RyaSailRepository} that is connected to the Rya instance
     *         that statements are loaded into.
     */
    protected RyaSailRepository getRyaSailRepository() throws Exception {
        return ryaSailRepo;
    }

    /**
     * @return A {@link AccumuloRyaDAO} so that RyaStatements with distinct
     *         visibilities can be added to the Rya Instance
     */
    protected AccumuloRyaDAO getRyaDAO() {
        return dao;
    }

    /**
     * Close all the Kafka mini server and mini-zookeeper
     */
    @After
    public void teardownKafka() {
        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }
    }

    protected KafkaConsumer<Integer, VisibilityBindingSet> makeConsumer(final String TopicName) {
        // setup consumer
        final Properties consumerProps = createBootstrapServerConfig();
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.rya.indexing.pcj.fluo.app.export.kafka.KryoVisibilityBindingSetSerializer");

        // to make sure the consumer starts from the beginning of the topic
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<Integer, VisibilityBindingSet> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TopicName));
        return consumer;
    }

    protected String loadData(final String sparql, final Collection<Statement> statements) throws Exception {
        requireNonNull(sparql);
        requireNonNull(statements);

        // Register the PCJ with Rya.
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(super.createConnectionDetails(), super.getAccumuloConnector());

        final String pcjId = ryaClient.getCreatePCJ().createPCJ(RYA_INSTANCE_NAME, sparql);

        // Write the data to Rya.
        final SailRepositoryConnection ryaConn = getRyaSailRepository().getConnection();
        ryaConn.begin();
        ryaConn.add(statements);
        ryaConn.commit();
        ryaConn.close();

        // Wait for the Fluo application to finish computing the end result.
        super.getMiniFluo().waitForObservers();

        // The PCJ Id is the topic name the results will be written to.
        return pcjId;
    }

}
