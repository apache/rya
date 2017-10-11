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
package org.apache.rya.periodic.notification.application;

import static org.apache.rya.periodic.notification.application.PeriodicNotificationApplicationConfiguration.KAFKA_BOOTSTRAP_SERVERS;
import static org.apache.rya.periodic.notification.application.PeriodicNotificationApplicationConfiguration.NOTIFICATION_TOPIC;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.pcj.fluo.api.CreatePeriodicQuery;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoClientFactory;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoQueryUtils;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.registration.KafkaNotificationRegistrationClient;
import org.apache.rya.periodic.notification.serialization.BindingSetSerDe;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;
import org.apache.rya.test.kafka.EmbeddedKafkaInstance;
import org.apache.rya.test.kafka.EmbeddedKafkaSingleton;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;;


public class PeriodicNotificationApplicationIT extends RyaExportITBase {

    private PeriodicNotificationApplication app;
    private KafkaNotificationRegistrationClient registrar;
    private KafkaProducer<String, CommandNotification> producer;
    private Properties props;
    private Properties kafkaProps;
    private PeriodicNotificationApplicationConfiguration conf;
    private static EmbeddedKafkaInstance embeddedKafka = EmbeddedKafkaSingleton.getInstance();
    private static String bootstrapServers;

    @Rule
    public KafkaTestInstanceRule rule = new KafkaTestInstanceRule(false);

    @BeforeClass
    public static void initClass() {
        bootstrapServers = embeddedKafka.createBootstrapServerConfig().getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    }

    @Before
    public void init() throws Exception {
        final String topic = rule.getKafkaTopicName();
        rule.createTopic(topic);

        //get user specified props and update with the embedded kafka bootstrap servers and rule generated topic
        props = getProps();
        props.setProperty(NOTIFICATION_TOPIC, topic);
        props.setProperty(KAFKA_BOOTSTRAP_SERVERS, bootstrapServers);
        conf = new PeriodicNotificationApplicationConfiguration(props);

        //create Kafka Producer
        kafkaProps = getKafkaProperties(conf);
        producer = new KafkaProducer<>(kafkaProps, new StringSerializer(), new CommandNotificationSerializer());

        //extract kafka specific properties from application config
        app = PeriodicNotificationApplicationFactory.getPeriodicApplication(props);
        registrar = new KafkaNotificationRegistrationClient(conf.getNotificationTopic(), producer);
    }

    @Test
    public void periodicApplicationWithAggAndGroupByTest() throws Exception {

        final String sparql = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select ?type (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 1, .25, time:minutes)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasObsType> ?type } group by ?type"; // n

        //make data
        final int periodMult = 15;
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        //Sleep until current time aligns nicely with period to makell
        //results more predictable
        while(System.currentTimeMillis() % (periodMult*1000) > 500) {
            ;
        }
        final ZonedDateTime time = ZonedDateTime.now();

        final ZonedDateTime zTime1 = time.minusSeconds(2*periodMult);
        final String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime2 = zTime1.minusSeconds(periodMult);
        final String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime3 = zTime2.minusSeconds(periodMult);
        final String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasObsType"), vf.createLiteral("ship")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasObsType"), vf.createLiteral("airplane")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasObsType"), vf.createLiteral("ship")),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_4"), vf.createURI("uri:hasObsType"), vf.createLiteral("airplane")),
                vf.createStatement(vf.createURI("urn:obs_5"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_5"), vf.createURI("uri:hasObsType"), vf.createLiteral("automobile")));

        try (FluoClient fluo = FluoClientFactory.getFluoClient(conf.getFluoAppName(), Optional.of(conf.getFluoTableName()), conf)) {
            final Connector connector = ConfigUtils.getConnector(conf);
            final PeriodicQueryResultStorage storage = new AccumuloPeriodicQueryResultStorage(connector, conf.getTablePrefix());
            final CreatePeriodicQuery periodicQuery = new CreatePeriodicQuery(fluo, storage);
            final String id = FluoQueryUtils.convertFluoQueryIdToPcjId(periodicQuery.createPeriodicQuery(sparql, registrar).getQueryId());
            addData(statements);
            app.start();

            final Multimap<Long, BindingSet> actual = HashMultimap.create();
            try (KafkaConsumer<String, BindingSet> consumer = new KafkaConsumer<>(kafkaProps, new StringDeserializer(), new BindingSetSerDe())) {
                consumer.subscribe(Arrays.asList(id));
                final long end = System.currentTimeMillis() + 4*periodMult*1000;
                long lastBinId = 0L;
                long binId = 0L;
                final List<Long> ids = new ArrayList<>();
                while (System.currentTimeMillis() < end) {
                    final ConsumerRecords<String, BindingSet> records = consumer.poll(periodMult*1000);
                    for(final ConsumerRecord<String, BindingSet> record: records){
                        final BindingSet result = record.value();
                        binId = Long.parseLong(result.getBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID).getValue().stringValue());
                        if(lastBinId != binId) {
                            lastBinId = binId;
                            ids.add(binId);
                        }
                        actual.put(binId, result);
                    }
                }

                final Map<Long, Set<BindingSet>> expected = new HashMap<>();

                final Set<BindingSet> expected1 = new HashSet<>();
                final QueryBindingSet bs1 = new QueryBindingSet();
                bs1.addBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID, vf.createLiteral(ids.get(0)));
                bs1.addBinding("total", new LiteralImpl("2", XMLSchema.INTEGER));
                bs1.addBinding("type", vf.createLiteral("airplane"));

                final QueryBindingSet bs2 = new QueryBindingSet();
                bs2.addBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID, vf.createLiteral(ids.get(0)));
                bs2.addBinding("total", new LiteralImpl("2", XMLSchema.INTEGER));
                bs2.addBinding("type", vf.createLiteral("ship"));

                final QueryBindingSet bs3 = new QueryBindingSet();
                bs3.addBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID, vf.createLiteral(ids.get(0)));
                bs3.addBinding("total", new LiteralImpl("1", XMLSchema.INTEGER));
                bs3.addBinding("type", vf.createLiteral("automobile"));

                expected1.add(bs1);
                expected1.add(bs2);
                expected1.add(bs3);

                final Set<BindingSet> expected2 = new HashSet<>();
                final QueryBindingSet bs4 = new QueryBindingSet();
                bs4.addBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID, vf.createLiteral(ids.get(1)));
                bs4.addBinding("total", new LiteralImpl("2", XMLSchema.INTEGER));
                bs4.addBinding("type", vf.createLiteral("airplane"));

                final QueryBindingSet bs5 = new QueryBindingSet();
                bs5.addBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID, vf.createLiteral(ids.get(1)));
                bs5.addBinding("total", new LiteralImpl("2", XMLSchema.INTEGER));
                bs5.addBinding("type", vf.createLiteral("ship"));

                expected2.add(bs4);
                expected2.add(bs5);

                final Set<BindingSet> expected3 = new HashSet<>();
                final QueryBindingSet bs6 = new QueryBindingSet();
                bs6.addBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID, vf.createLiteral(ids.get(2)));
                bs6.addBinding("total", new LiteralImpl("1", XMLSchema.INTEGER));
                bs6.addBinding("type", vf.createLiteral("ship"));

                final QueryBindingSet bs7 = new QueryBindingSet();
                bs7.addBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID, vf.createLiteral(ids.get(2)));
                bs7.addBinding("total", new LiteralImpl("1", XMLSchema.INTEGER));
                bs7.addBinding("type", vf.createLiteral("airplane"));

                expected3.add(bs6);
                expected3.add(bs7);

                expected.put(ids.get(0), expected1);
                expected.put(ids.get(1), expected2);
                expected.put(ids.get(2), expected3);

                Assert.assertEquals(3, actual.asMap().size());
                for(final Long ident: ids) {
                    Assert.assertEquals(expected.get(ident), actual.get(ident));
                }
            }

            final Set<BindingSet> expectedResults = new HashSet<>();
            try (CloseableIterator<BindingSet> results = storage.listResults(id, Optional.empty())) {
                results.forEachRemaining(x -> expectedResults.add(x));
                Assert.assertEquals(0, expectedResults.size());
            }
        }
    }


    @Test
    public void periodicApplicationWithAggTest() throws Exception {

        final String sparql = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 1, .25, time:minutes)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id } "; // n

        //make data
        final int periodMult = 15;
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        //Sleep until current time aligns nicely with period to make
        //results more predictable
        while(System.currentTimeMillis() % (periodMult*1000) > 500) {
            ;
        }
        final ZonedDateTime time = ZonedDateTime.now();

        final ZonedDateTime zTime1 = time.minusSeconds(2*periodMult);
        final String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime2 = zTime1.minusSeconds(periodMult);
        final String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime3 = zTime2.minusSeconds(periodMult);
        final String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasId"), vf.createLiteral("id_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasId"), vf.createLiteral("id_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasId"), vf.createLiteral("id_3")));

        try (FluoClient fluo = FluoClientFactory.getFluoClient(conf.getFluoAppName(), Optional.of(conf.getFluoTableName()), conf)) {
            final Connector connector = ConfigUtils.getConnector(conf);
            final PeriodicQueryResultStorage storage = new AccumuloPeriodicQueryResultStorage(connector, conf.getTablePrefix());
            final CreatePeriodicQuery periodicQuery = new CreatePeriodicQuery(fluo, storage);
            final String id = FluoQueryUtils.convertFluoQueryIdToPcjId(periodicQuery.createPeriodicQuery(sparql, registrar).getQueryId());
            addData(statements);
            app.start();

            final Multimap<Long, BindingSet> expected = HashMultimap.create();
            try (KafkaConsumer<String, BindingSet> consumer = new KafkaConsumer<>(kafkaProps, new StringDeserializer(), new BindingSetSerDe())) {
                consumer.subscribe(Arrays.asList(id));
                final long end = System.currentTimeMillis() + 4*periodMult*1000;
                long lastBinId = 0L;
                long binId = 0L;
                final List<Long> ids = new ArrayList<>();
                while (System.currentTimeMillis() < end) {
                    final ConsumerRecords<String, BindingSet> records = consumer.poll(periodMult*1000);
                    for(final ConsumerRecord<String, BindingSet> record: records){
                        final BindingSet result = record.value();
                        binId = Long.parseLong(result.getBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID).getValue().stringValue());
                        if(lastBinId != binId) {
                            lastBinId = binId;
                            ids.add(binId);
                        }
                        expected.put(binId, result);
                    }
                }

                Assert.assertEquals(3, expected.asMap().size());
                int i = 0;
                for(final Long ident: ids) {
                    Assert.assertEquals(1, expected.get(ident).size());
                    final BindingSet bs = expected.get(ident).iterator().next();
                    final Value val = bs.getValue("total");
                    final int total = Integer.parseInt(val.stringValue());
                    Assert.assertEquals(3-i, total);
                    i++;
                }
            }


            final Set<BindingSet> expectedResults = new HashSet<>();
            try (CloseableIterator<BindingSet> results = storage.listResults(id, Optional.empty())) {
                results.forEachRemaining(x -> expectedResults.add(x));
                Assert.assertEquals(0, expectedResults.size());
            }
        }

    }


    @Test
    public void periodicApplicationTest() throws Exception {

        final String sparql = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select ?obs ?id where {" // n
                + "Filter(function:periodic(?time, 1, .25, time:minutes)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id } "; // n

        //make data
        final int periodMult = 15;
        final ValueFactory vf = new ValueFactoryImpl();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        //Sleep until current time aligns nicely with period to make
        //results more predictable
        while(System.currentTimeMillis() % (periodMult*1000) > 500) {
            ;
        }
        final ZonedDateTime time = ZonedDateTime.now();

        final ZonedDateTime zTime1 = time.minusSeconds(2*periodMult);
        final String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime2 = zTime1.minusSeconds(periodMult);
        final String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        final ZonedDateTime zTime3 = zTime2.minusSeconds(periodMult);
        final String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createURI("urn:obs_1"), vf.createURI("uri:hasId"), vf.createLiteral("id_1")),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createURI("urn:obs_2"), vf.createURI("uri:hasId"), vf.createLiteral("id_2")),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createURI("urn:obs_3"), vf.createURI("uri:hasId"), vf.createLiteral("id_3")));

        try (FluoClient fluo = FluoClientFactory.getFluoClient(conf.getFluoAppName(), Optional.of(conf.getFluoTableName()), conf)) {
            final Connector connector = ConfigUtils.getConnector(conf);
            final PeriodicQueryResultStorage storage = new AccumuloPeriodicQueryResultStorage(connector, conf.getTablePrefix());
            final CreatePeriodicQuery periodicQuery = new CreatePeriodicQuery(fluo, storage);
            final String id = FluoQueryUtils.convertFluoQueryIdToPcjId(periodicQuery.createPeriodicQuery(sparql, registrar).getQueryId());
            addData(statements);
            app.start();

            final Multimap<Long, BindingSet> expected = HashMultimap.create();
            try (KafkaConsumer<String, BindingSet> consumer = new KafkaConsumer<>(kafkaProps, new StringDeserializer(), new BindingSetSerDe())) {
                consumer.subscribe(Arrays.asList(id));
                final long end = System.currentTimeMillis() + 4*periodMult*1000;
                long lastBinId = 0L;
                long binId = 0L;
                final List<Long> ids = new ArrayList<>();
                while (System.currentTimeMillis() < end) {
                    final ConsumerRecords<String, BindingSet> records = consumer.poll(periodMult*1000);
                    for(final ConsumerRecord<String, BindingSet> record: records){
                        final BindingSet result = record.value();
                        binId = Long.parseLong(result.getBinding(IncrementalUpdateConstants.PERIODIC_BIN_ID).getValue().stringValue());
                        if(lastBinId != binId) {
                            lastBinId = binId;
                            ids.add(binId);
                        }
                        expected.put(binId, result);
                    }
                }

                Assert.assertEquals(3, expected.asMap().size());
                int i = 0;
                for(final Long ident: ids) {
                    Assert.assertEquals(3-i, expected.get(ident).size());
                    i++;
                }
            }


            final Set<BindingSet> expectedResults = new HashSet<>();
            try (CloseableIterator<BindingSet> results = storage.listResults(id, Optional.empty())) {
                results.forEachRemaining(x -> expectedResults.add(x));
                Assert.assertEquals(0, expectedResults.size());
            }
        }

    }


    @After
    public void shutdown() {
        registrar.close();
        app.stop();
    }

    private void addData(final Collection<Statement> statements) throws DatatypeConfigurationException {
        // add statements to Fluo
        try (FluoClient fluo = new FluoClientImpl(getFluoConfiguration())) {
            final InsertTriples inserter = new InsertTriples();
            statements.forEach(x -> inserter.insert(fluo, RdfToRyaConversions.convertStatement(x)));
            getMiniFluo().waitForObservers();
        }
    }

    private static Properties getKafkaProperties(final PeriodicNotificationApplicationConfiguration conf) {
        final Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, conf.getNotificationGroupId());
        kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return kafkaProps;
    }

    private Properties getProps() throws IOException {

        final Properties props = new Properties();
        try(InputStream in = new FileInputStream("src/test/resources/notification.properties")) {
            props.load(in);
        }

        final FluoConfiguration fluoConf = getFluoConfiguration();
        props.setProperty("accumulo.user", getUsername());
        props.setProperty("accumulo.password", getPassword());
        props.setProperty("accumulo.instance", getMiniAccumuloCluster().getInstanceName());
        props.setProperty("accumulo.zookeepers", getMiniAccumuloCluster().getZooKeepers());
        props.setProperty("accumulo.rya.prefix", getRyaInstanceName());
        props.setProperty(PeriodicNotificationApplicationConfiguration.FLUO_APP_NAME, fluoConf.getApplicationName());
        props.setProperty(PeriodicNotificationApplicationConfiguration.FLUO_TABLE_NAME, fluoConf.getAccumuloTable());
        return props;
    }

}
