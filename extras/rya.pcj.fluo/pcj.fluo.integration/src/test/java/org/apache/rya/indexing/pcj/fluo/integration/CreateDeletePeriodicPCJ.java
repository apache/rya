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
package org.apache.rya.indexing.pcj.fluo.integration;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.xml.datatype.DatatypeFactory;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Span;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.indexing.pcj.fluo.api.CreatePeriodicQuery;
import org.apache.rya.indexing.pcj.fluo.api.DeletePeriodicQuery;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoQueryUtils;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPeriodicQueryResultStorage;
import org.apache.rya.pcj.fluo.test.base.KafkaExportITBase;
import org.apache.rya.periodic.notification.api.PeriodicNotificationClient;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification.Command;
import org.apache.rya.periodic.notification.registration.KafkaNotificationRegistrationClient;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.junit.Test;

import com.google.common.collect.Sets;

public class CreateDeletePeriodicPCJ extends KafkaExportITBase {

    @Test
    public void deletePeriodicPCJ() throws Exception {
        String query = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 2, .5, time:hours)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id }"; // n

        // Create the Statements that will be loaded into Rya.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();
        ZonedDateTime time = ZonedDateTime.now();

        ZonedDateTime zTime1 = time.minusMinutes(30);
        String time1 = zTime1.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime2 = zTime1.minusMinutes(30);
        String time2 = zTime2.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime3 = zTime2.minusMinutes(30);
        String time3 = zTime3.format(DateTimeFormatter.ISO_INSTANT);

        ZonedDateTime zTime4 = zTime3.minusMinutes(30);
        String time4 = zTime4.format(DateTimeFormatter.ISO_INSTANT);

        final Collection<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createIRI("urn:obs_1"), vf.createIRI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time1))),
                vf.createStatement(vf.createIRI("urn:obs_1"), vf.createIRI("uri:hasId"), vf.createLiteral("id_1")),
                vf.createStatement(vf.createIRI("urn:obs_2"), vf.createIRI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time2))),
                vf.createStatement(vf.createIRI("urn:obs_2"), vf.createIRI("uri:hasId"), vf.createLiteral("id_2")),
                vf.createStatement(vf.createIRI("urn:obs_3"), vf.createIRI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time3))),
                vf.createStatement(vf.createIRI("urn:obs_3"), vf.createIRI("uri:hasId"), vf.createLiteral("id_3")),
                vf.createStatement(vf.createIRI("urn:obs_4"), vf.createIRI("uri:hasTime"),
                        vf.createLiteral(dtf.newXMLGregorianCalendar(time4))),
                vf.createStatement(vf.createIRI("urn:obs_4"), vf.createIRI("uri:hasId"), vf.createLiteral("id_4")));

        runTest(query, statements, 30);

    }



    private void runTest(String query, Collection<Statement> statements, int expectedEntries) throws Exception {
        try (FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {

            String topic = "notification_topic";
            PeriodicQueryResultStorage storage = new AccumuloPeriodicQueryResultStorage(super.getAccumuloConnector(), RYA_INSTANCE_NAME);
            PeriodicNotificationClient notificationClient = new KafkaNotificationRegistrationClient(topic,
                    getNotificationProducer("localhost:9092"));

            CreatePeriodicQuery periodicPCJ = new CreatePeriodicQuery(fluoClient, storage);
            String id = periodicPCJ.createPeriodicQuery(query, notificationClient).getQueryId();

            loadData(statements);

            // Ensure the data was loaded.
            final List<Bytes> rows = getFluoTableEntries(fluoClient);
            assertEquals(expectedEntries, rows.size());

            DeletePeriodicQuery deletePeriodic = new DeletePeriodicQuery(fluoClient, storage);
            deletePeriodic.deletePeriodicQuery(FluoQueryUtils.convertFluoQueryIdToPcjId(id), notificationClient);
            getMiniFluo().waitForObservers();

            // Ensure all data related to the query has been removed.
            final List<Bytes> empty_rows = getFluoTableEntries(fluoClient);
            assertEquals(1, empty_rows.size());

            // Ensure that Periodic Service notified to add and delete PeriodicNotification
            Set<CommandNotification> notifications;
            try (KafkaConsumer<String, CommandNotification> consumer = makeNotificationConsumer(topic)) {
                notifications = getKafkaNotifications(topic, 7000, consumer);
            }
            assertEquals(2, notifications.size());

            String notificationId = "";
            boolean addCalled = false;
            boolean deleteCalled = false;
            for (CommandNotification notification : notifications) {
                if (notificationId.length() == 0) {
                    notificationId = notification.getId();
                } else {
                    assertEquals(notificationId, notification.getId());
                }

                if (notification.getCommand() == Command.ADD) {
                    addCalled = true;
                }

                if (notification.getCommand() == Command.DELETE) {
                    deleteCalled = true;
                }
            }

            assertEquals(true, addCalled);
            assertEquals(true, deleteCalled);
        }
    }

    private List<Bytes> getFluoTableEntries(final FluoClient fluoClient) {
        try (Snapshot snapshot = fluoClient.newSnapshot()) {
            final List<Bytes> rows = new ArrayList<>();
            final RowScanner rscanner = snapshot.scanner().over(Span.prefix("")).byRow().build();

            for (final ColumnScanner cscanner : rscanner) {
                rows.add(cscanner.getRow());
            }

            return rows;
        }
    }

    private KafkaProducer<String, CommandNotification> getNotificationProducer(String bootStrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommandNotificationSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, CommandNotification> makeNotificationConsumer(final String topic) {
        // setup consumer
        final Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CommandNotificationSerializer.class.getName());

        // to make sure the consumer starts from the beginning of the topic
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaConsumer<String, CommandNotification> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    private Set<CommandNotification> getKafkaNotifications(String topic, int pollTime,
            KafkaConsumer<String, CommandNotification> consumer) {
        requireNonNull(topic);

        // Read all of the results from the Kafka topic.
        final Set<CommandNotification> results = new HashSet<>();

        final ConsumerRecords<String, CommandNotification> records = consumer.poll(pollTime);
        final Iterator<ConsumerRecord<String, CommandNotification>> recordIterator = records.iterator();
        while (recordIterator.hasNext()) {
            results.add(recordIterator.next().value());
        }

        return results;
    }

}
