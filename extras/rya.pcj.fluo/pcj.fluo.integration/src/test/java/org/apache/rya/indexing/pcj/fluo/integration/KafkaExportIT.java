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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.KafkaExportParameters;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.junit.Test;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.BindingImpl;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

/**
 * Performs integration tests over the Fluo application geared towards Kafka PCJ exporting.
 * <p>
 * These tests are ignored so that they will not run as unit tests while building the application.
 * Run this test from Maven command line:
 * $ cd rya/extras/rya.pcj.fluo/pcj.fluo.integration
 * $ mvn surefire:test -Dtest=KafkaExportIT
 */
public class KafkaExportIT extends ITBase {
    // /**
    // * Run from the command line:
    // *
    // * @param args
    // * @throws Exception
    // */
    // public static void main(String[] args) throws Exception {
    // // Do what Junit would do:
    // KafkaExportIT.killLoudLogs();
    // KafkaExportIT kafkaExportIT = new KafkaExportIT();
    // kafkaExportIT.setupMiniResources();
    // kafkaExportIT.resultsExported();
    // kafkaExportIT.shutdownMiniResources();
    // }

    @Test
    public void resultsExported() throws Exception {
        final String sparql = "SELECT ?customer ?worker ?city " + "{ " + "FILTER(?customer = <http://Alice>) " + "FILTER(?city = <http://London>) " + "?customer <http://talksTo> ?worker. " + "?worker <http://livesIn> ?city. " + "?worker <http://worksAt> <http://Chipotle>. " + "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(makeRyaStatement("http://Alice", "http://talksTo", "http://Bob"), makeRyaStatement("http://Bob", "http://livesIn", "http://London"), makeRyaStatement("http://Bob", "http://worksAt", "http://Chipotle"),

                        makeRyaStatement("http://Alice", "http://talksTo", "http://Charlie"), makeRyaStatement("http://Charlie", "http://livesIn", "http://London"), makeRyaStatement("http://Charlie", "http://worksAt", "http://Chipotle"),

                        makeRyaStatement("http://Alice", "http://talksTo", "http://David"), makeRyaStatement("http://David", "http://livesIn", "http://London"), makeRyaStatement("http://David", "http://worksAt", "http://Chipotle"),

                        makeRyaStatement("http://Alice", "http://talksTo", "http://Eve"), makeRyaStatement("http://Eve", "http://livesIn", "http://Leeds"), makeRyaStatement("http://Eve", "http://worksAt", "http://Chipotle"),

                        makeRyaStatement("http://Frank", "http://talksTo", "http://Alice"), makeRyaStatement("http://Frank", "http://livesIn", "http://London"), makeRyaStatement("http://Frank", "http://worksAt", "http://Chipotle"));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(new BindingImpl("customer", new URIImpl("http://Alice")), new BindingImpl("worker", new URIImpl("http://Bob")), new BindingImpl("city", new URIImpl("http://London"))));
        expected.add(makeBindingSet(new BindingImpl("customer", new URIImpl("http://Alice")), new BindingImpl("worker", new URIImpl("http://Charlie")), new BindingImpl("city", new URIImpl("http://London"))));
        expected.add(makeBindingSet(new BindingImpl("customer", new URIImpl("http://Alice")), new BindingImpl("worker", new URIImpl("http://David")), new BindingImpl("city", new URIImpl("http://London"))));

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(sparql);

        // Tell the Fluo app to maintain the PCJ.
        CreatePcj createPcj = new CreatePcj();
        String QueryIdIsTopicName = createPcj.withRyaIntegration(pcjId, pcjStorage, fluoClient, ryaRepo);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String> absent());

        // Fetch the exported results from Accumulo once the observers finish working.
        fluo.waitForObservers();

        // Copied from RyaExportIT:
        // Fetch expected results from the PCJ table that is in Accumulo.
        // final Set<BindingSet> results = Sets.newHashSet(pcjStorage.listResults(pcjId));
        // Verify the end results of the query match the expected results.
        // assertEquals(expected, results);

        // Grab from Kafka topic instead of RYA
        ITConsumer consumer = new ITConsumer();
        List<ConsumerRecords<String, String>> verifyThese = ITConsumer.consume(QueryIdIsTopicName, "theOnlyGroup"); // TODO what is this group thing and how to ignore/use it?

        System.out.println("Consumed these: " + verifyThese);
        // assert (consumer.verifyTheseRecords;
    }


    /**
     * Add info about the kafka queue/topic to receive the export.
     * Call super to get the Rya parameters.
     * 
     * @see org.apache.rya.indexing.pcj.fluo.ITBase#setExportParameters(java.util.HashMap)
     */
    @Override
    protected void setExportParameters(HashMap<String, String> exportParams) {
        // Get the defaults
        super.setExportParameters(exportParams);
        // Add the kafka parameters
        final KafkaExportParameters kafkaParams = new KafkaExportParameters(exportParams);
        kafkaParams.setExportToKafka(true);
        // Configure the Producer
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.setProducerConfig(producerConfig);
    }
}

final class ITConsumer {
    private static List<ConsumerRecords<String, String>> verifyTheseRecords = new LinkedList<ConsumerRecords<String, String>>();

    public static List<ConsumerRecords<String, String>> consume(String topicName, String groupId) throws Exception {
        ConsumerThread consumerRunnable = new ConsumerThread(topicName, groupId);
        consumerRunnable.start();
        consumerRunnable.wait(100);// cheap way to let things happen. TODO make it depend on something besides time
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join(100); // wait no more than 100ms to terminate.
        return Collections.unmodifiableList(verifyTheseRecords);
    }

    private static class ConsumerThread extends Thread {
        private String topicName;
        private String groupId;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId) {
            this.topicName = topicName;
            this.groupId = groupId;
        }

        @Override
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");

            // Figure out where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));
            // Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    verifyTheseRecords.add(records);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(record.value());
                    }
                }
            } catch (WakeupException ex) {
                System.out.println("Exception caught " + ex.getMessage());
            } finally {
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }

        public KafkaConsumer<String, String> getKafkaConsumer() {
            return this.kafkaConsumer;
        }
    }
}