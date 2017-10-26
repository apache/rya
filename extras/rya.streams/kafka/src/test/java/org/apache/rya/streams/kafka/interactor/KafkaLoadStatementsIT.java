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
package org.apache.rya.streams.kafka.interactor;

import static org.junit.Assert.assertEquals;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementDeserializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementSerializer;
import org.apache.rya.test.kafka.KafkaITBase;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.UnsupportedRDFormatException;

/**
 * Integration tests the {@link KafkaLoadStatements} command
 */
public class KafkaLoadStatementsIT extends KafkaITBase {
    private static final Path TURTLE_FILE = Paths.get("src/test/resources/statements.ttl");

    private static final Path INVALID = Paths.get("src/test/resources/invalid.INVALID");

    @Rule
    public KafkaTestInstanceRule rule = new KafkaTestInstanceRule(true);

    @Test(expected = UnsupportedRDFormatException.class)
    public void test_invalidFile() throws Exception {
        final String topic = rule.getKafkaTopicName();
        final String visibilities = "a|b|c";
        final Properties props = rule.createBootstrapServerConfig();
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VisibilityStatementSerializer.class.getName());
        try (final Producer<Object, VisibilityStatement> producer = new KafkaProducer<>(props)) {
            final KafkaLoadStatements command = new KafkaLoadStatements(topic, producer);
            command.load(INVALID, visibilities);
        }
    }

    @Test
    public void testTurtle() throws Exception {
        final String topic = rule.getKafkaTopicName();
        final String visibilities = "a|b|c";
        final Properties props = rule.createBootstrapServerConfig();
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VisibilityStatementSerializer.class.getName());
        try (final Producer<Object, VisibilityStatement> producer = new KafkaProducer<>(props)) {
            final KafkaLoadStatements command = new KafkaLoadStatements(topic, producer);
            command.load(TURTLE_FILE, visibilities);
        }

        // Read a VisibilityBindingSet from the test topic.
        final List<VisibilityStatement> read = new ArrayList<>();

        final Properties consumerProps = rule.createBootstrapServerConfig();
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VisibilityStatementDeserializer.class.getName());

        try (final KafkaConsumer<String, VisibilityStatement> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Arrays.asList(rule.getKafkaTopicName()));
            final ConsumerRecords<String, VisibilityStatement> records = consumer.poll(2000);

            assertEquals(3, records.count());
            final Iterator<ConsumerRecord<String, VisibilityStatement>> iter = records.iterator();
            while(iter.hasNext()) {
                final VisibilityStatement visiSet = iter.next().value();
                read.add(visiSet);
            }
        }

        final List<VisibilityStatement> original = new ArrayList<>();
        final ValueFactory VF = ValueFactoryImpl.getInstance();

        original.add(new VisibilityStatement(
                VF.createStatement(VF.createURI("http://example#alice"), VF.createURI("http://example#talksTo"), VF.createURI("http://example#bob")),
                visibilities));
        original.add(new VisibilityStatement(
                VF.createStatement(VF.createURI("http://example#bob"), VF.createURI("http://example#talksTo"), VF.createURI("http://example#charlie")),
                visibilities));
        original.add(new VisibilityStatement(
                VF.createStatement(VF.createURI("http://example#charlie"), VF.createURI("http://example#likes"), VF.createURI("http://example#icecream")),
                visibilities));
        // Show the written statement matches the read one.
        assertEquals(original, read);
    }
}
