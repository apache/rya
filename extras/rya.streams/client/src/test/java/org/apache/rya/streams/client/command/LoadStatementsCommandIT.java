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
package org.apache.rya.streams.client.command;

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementDeserializer;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Integration tests the methods of {@link LoadStatementsCommand}.
 */
public class LoadStatementsCommandIT {

    private static final Path TURTLE_FILE = Paths.get("src/test/resources/statements.ttl");

    private final String ryaInstance = UUID.randomUUID().toString();

    private String kafkaIp;
    private String kafkaPort;

    @Rule
    public KafkaTestInstanceRule rule = new KafkaTestInstanceRule(true);

    @Before
    public void setup() {
        final Properties props = rule.createBootstrapServerConfig();
        final String location = props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        final String[] tokens = location.split(":");

        kafkaIp = tokens[0];
        kafkaPort = tokens[1];
    }

    @Test
    public void shortParams() throws Exception {
        // Load a file of statements into Kafka.
        final String visibilities = "a|b|c";
        final String[] args = new String[] {
                "-r", "" + ryaInstance,
                "-i", kafkaIp,
                "-p", kafkaPort,
                "-f", TURTLE_FILE.toString(),
                "-v", visibilities
        };

        new LoadStatementsCommand().execute(args);

        // Show that the statements were loaded into the topic.
        // Read a VisibilityBindingSet from the test topic.
        final List<VisibilityStatement> read = new ArrayList<>();

        final Properties consumerProps = new Properties();
        consumerProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaIp + ":" + kafkaPort);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VisibilityStatementDeserializer.class.getName());

        try(final Consumer<String, VisibilityStatement> consumer = new KafkaConsumer<>(consumerProps)) {
            final String topic = KafkaTopics.statementsTopic(ryaInstance);
            consumer.subscribe(Arrays.asList(topic));
            final ConsumerRecords<String, VisibilityStatement> records = consumer.poll(3000);

            assertEquals(3, records.count());
            final Iterator<ConsumerRecord<String, VisibilityStatement>> iter = records.iterator();
            while(iter.hasNext()) {
                final VisibilityStatement visiSet = iter.next().value();
                read.add(visiSet);
            }
        }

        final ValueFactory VF = ValueFactoryImpl.getInstance();
        final List<VisibilityStatement> expected = new ArrayList<>();
        expected.add(new VisibilityStatement(
                VF.createStatement(VF.createURI("http://example#alice"), VF.createURI("http://example#talksTo"), VF.createURI("http://example#bob")),
                visibilities));
        expected.add(new VisibilityStatement(
                VF.createStatement(VF.createURI("http://example#bob"), VF.createURI("http://example#talksTo"), VF.createURI("http://example#charlie")),
                visibilities));
        expected.add(new VisibilityStatement(
                VF.createStatement(VF.createURI("http://example#charlie"), VF.createURI("http://example#likes"), VF.createURI("http://example#icecream")),
                visibilities));

        // Show the written statements matches the read ones.
        assertEquals(expected, read);
    }

    @Test
    public void longParams() throws Exception {
        // Load a file of statements into Kafka.
        final String visibilities = "a|b|c";
        final String[] args = new String[] {
                "--ryaInstance", "" + ryaInstance,
                "--kafkaHostname", kafkaIp,
                "--kafkaPort", kafkaPort,
                "--statementsFile", TURTLE_FILE.toString(),
                "--visibilities", visibilities
        };

        new LoadStatementsCommand().execute(args);

        // Show that the statements were loaded into the topic.
        // Read a VisibilityBindingSet from the test topic.
        final List<VisibilityStatement> read = new ArrayList<>();

        final Properties consumerProps = new Properties();
        consumerProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaIp + ":" + kafkaPort);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VisibilityStatementDeserializer.class.getName());

        try(final Consumer<String, VisibilityStatement> consumer = new KafkaConsumer<>(consumerProps)) {
            final String topic = KafkaTopics.statementsTopic(ryaInstance);
            consumer.subscribe(Arrays.asList(topic));
            final ConsumerRecords<String, VisibilityStatement> records = consumer.poll(3000);

            assertEquals(3, records.count());
            final Iterator<ConsumerRecord<String, VisibilityStatement>> iter = records.iterator();
            while(iter.hasNext()) {
                final VisibilityStatement visiSet = iter.next().value();
                read.add(visiSet);
            }
        }

        final ValueFactory VF = ValueFactoryImpl.getInstance();
        final List<VisibilityStatement> expected = new ArrayList<>();
        expected.add(new VisibilityStatement(
                VF.createStatement(VF.createURI("http://example#alice"), VF.createURI("http://example#talksTo"), VF.createURI("http://example#bob")),
                visibilities));
        expected.add(new VisibilityStatement(
                VF.createStatement(VF.createURI("http://example#bob"), VF.createURI("http://example#talksTo"), VF.createURI("http://example#charlie")),
                visibilities));
        expected.add(new VisibilityStatement(
                VF.createStatement(VF.createURI("http://example#charlie"), VF.createURI("http://example#likes"), VF.createURI("http://example#icecream")),
                visibilities));

        // Show the written statements matches the read ones.
        assertEquals(expected, read);
    }
}