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
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.recipes.test.AccumuloExportITBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaBindingSetExporterParameters;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaSubGraphExporterParameters;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KryoVisibilityBindingSetSerializer;
import org.apache.rya.indexing.pcj.fluo.app.observers.AggregationObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.ConstructQueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.FilterObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.JoinObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.ProjectionObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.QueryResultObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.StatementPatternObserver;
import org.apache.rya.indexing.pcj.fluo.app.observers.TripleObserver;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.sail.config.RyaSailFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import com.google.common.collect.Sets;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;

/**
 * The base Integration Test class used for Fluo applications that export to a
 * Kakfa topic.
 */
public class KafkaExportITBase extends AccumuloExportITBase {

    protected static final String RYA_INSTANCE_NAME = "test_";

    private static final String ZKHOST = "127.0.0.1";
    private static final String BROKERHOST = "127.0.0.1";
    private static final String BROKERPORT = "9092";
    private ZkUtils zkUtils;
    private KafkaServer kafkaServer;
    private EmbeddedZookeeper zkServer;
    private ZkClient zkClient;

    // The Rya instance statements are written to that will be fed into the Fluo
    // app.
    private RyaSailRepository ryaSailRepo = null;
    private AccumuloRyaDAO dao = null;

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
        observers.add(new ObserverSpecification(ProjectionObserver.class.getName()));
        observers.add(new ObserverSpecification(ConstructQueryResultObserver.class.getName()));

        // Configure the export observer to export new PCJ results to the mini
        // accumulo cluster.
        final HashMap<String, String> exportParams = new HashMap<>();
        final KafkaBindingSetExporterParameters kafkaParams = new KafkaBindingSetExporterParameters(exportParams);
        kafkaParams.setUseKafkaBindingSetExporter(true);
        kafkaParams.setKafkaBootStrapServers(BROKERHOST + ":" + BROKERPORT);
        
        final KafkaSubGraphExporterParameters kafkaConstructParams = new KafkaSubGraphExporterParameters(exportParams);
        kafkaConstructParams.setUseKafkaSubgraphExporter(true);

        final ObserverSpecification exportObserverConfig = new ObserverSpecification(QueryResultObserver.class.getName(), exportParams);
        observers.add(exportObserverConfig);

        // Add the observers to the Fluo Configuration.
        super.getFluoConfiguration().addObservers(observers);
    }

    /**
     * setup mini kafka and call the super to setup mini fluo
     */
    @Before
    public void setupKafka() throws Exception {
        // Install an instance of Rya on the Accumulo cluster.
        installRyaInstance();

        // Setup Kafka.
        zkServer = new EmbeddedZookeeper();
        final String zkConnect = ZKHOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        final Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST + ":" + BROKERPORT);
        final KafkaConfig config = new KafkaConfig(brokerProps);
        final Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
    }

    @After
    public void teardownRya() {
        final MiniAccumuloCluster cluster = getMiniAccumuloCluster();
        final String instanceName = cluster.getInstanceName();
        final String zookeepers = cluster.getZooKeepers();

        // Uninstall the instance of Rya.
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(ACCUMULO_USER, ACCUMULO_PASSWORD.toCharArray(), instanceName, zookeepers),
                super.getAccumuloConnector());

        try {
            ryaClient.getUninstall().uninstall(RYA_INSTANCE_NAME);
            // Shutdown the repo.
            if(ryaSailRepo != null) {ryaSailRepo.shutDown();}
            if(dao != null ) {dao.destroy();}
        } catch (Exception e) {
            System.out.println("Encountered the following Exception when shutting down Rya: " + e.getMessage());
        }
    }

    private void installRyaInstance() throws Exception {
        final MiniAccumuloCluster cluster = super.getMiniAccumuloCluster();
        final String instanceName = cluster.getInstanceName();
        final String zookeepers = cluster.getZooKeepers();

        // Install the Rya instance to the mini accumulo cluster.
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(
                new AccumuloConnectionDetails(ACCUMULO_USER, ACCUMULO_PASSWORD.toCharArray(), instanceName, zookeepers),
                super.getAccumuloConnector());

        ryaClient.getInstall().install(RYA_INSTANCE_NAME,
                InstallConfiguration.builder().setEnableTableHashPrefix(false).setEnableFreeTextIndex(false)
                        .setEnableEntityCentricIndex(false).setEnableGeoIndex(false).setEnableTemporalIndex(false).setEnablePcjIndex(true)
                        .setFluoPcjAppName(super.getFluoConfiguration().getApplicationName()).build());

        // Connect to the Rya instance that was just installed.
        final AccumuloRdfConfiguration conf = makeConfig(instanceName, zookeepers);
        final Sail sail = RyaSailFactory.getInstance(conf);
        dao = RyaSailFactory.getAccumuloDAOWithUpdatedConfig(conf);
        ryaSailRepo = new RyaSailRepository(sail);
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
        if(kafkaServer != null) {kafkaServer.shutdown();}
        if(zkClient != null) {zkClient.close();}
        if(zkServer != null) {zkServer.shutdown();}
    }

    /**
     * Test kafka without rya code to make sure kafka works in this environment.
     * If this test fails then its a testing environment issue, not with Rya.
     * Source: https://github.com/asmaier/mini-kafka
     */
    @Test
    public void embeddedKafkaTest() throws Exception {
        // create topic
        final String topic = "testTopic";
        AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

        // setup producer
        final Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        producerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        final KafkaProducer<Integer, byte[]> producer = new KafkaProducer<>(producerProps);

        // setup consumer
        final Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // to make sure the consumer starts from the beginning of the topic
        consumerProps.put("auto.offset.reset", "earliest");

        final KafkaConsumer<Integer, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(topic));

        // send message
        final ProducerRecord<Integer, byte[]> data = new ProducerRecord<>(topic, 42, "test-message".getBytes(StandardCharsets.UTF_8));
        producer.send(data);
        producer.close();

        // starting consumer
        final ConsumerRecords<Integer, byte[]> records = consumer.poll(3000);
        assertEquals(1, records.count());
        final Iterator<ConsumerRecord<Integer, byte[]>> recordIterator = records.iterator();
        final ConsumerRecord<Integer, byte[]> record = recordIterator.next();
        assertEquals(42, (int) record.key());
        assertEquals("test-message", new String(record.value(), StandardCharsets.UTF_8));
        consumer.close();
    }

    protected KafkaConsumer<String, VisibilityBindingSet> makeConsumer(final String TopicName) {
        // setup consumer
        final Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERHOST + ":" + BROKERPORT);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KryoVisibilityBindingSetSerializer.class.getName());

        // to make sure the consumer starts from the beginning of the topic
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, VisibilityBindingSet> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(TopicName));
        return consumer;
    }

    protected String loadDataAndCreateQuery(final String sparql, final Collection<Statement> statements) throws Exception {
        requireNonNull(sparql);
        requireNonNull(statements);

        // Register the PCJ with Rya.
        final Instance accInstance = super.getAccumuloConnector().getInstance();
        final Connector accumuloConn = super.getAccumuloConnector();

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(new AccumuloConnectionDetails(ACCUMULO_USER,
                ACCUMULO_PASSWORD.toCharArray(), accInstance.getInstanceName(), accInstance.getZooKeepers()), accumuloConn);

        final String pcjId = ryaClient.getCreatePCJ().createPCJ(RYA_INSTANCE_NAME, sparql, Sets.newHashSet(ExportStrategy.KAFKA));

        loadData(statements);

        // The PCJ Id is the topic name the results will be written to.
        return pcjId;
    }
    
    protected void loadData(final Collection<Statement> statements) throws Exception {
        
        requireNonNull(statements);

        final SailRepositoryConnection ryaConn = getRyaSailRepository().getConnection();
        ryaConn.begin();
        ryaConn.add(statements);
        ryaConn.commit();
        ryaConn.close();

        // Wait for the Fluo application to finish computing the end result.
        super.getMiniFluo().waitForObservers();
        
    }

}
