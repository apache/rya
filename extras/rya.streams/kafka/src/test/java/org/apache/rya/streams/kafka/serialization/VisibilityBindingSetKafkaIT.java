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
package org.apache.rya.streams.kafka.serialization;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.impl.MapBindingSet;

/**
 * Integration tests the {@link VisibilityBindingSetSerde} class' methods.
 */
public class VisibilityBindingSetKafkaIT {

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    @Test
    public void readAndWrite() {
        // Create the object that will be written to the topic.
        final ValueFactory vf = new ValueFactoryImpl();

        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("urn:name", vf.createURI("urn:alice"));
        bs.addBinding("urn:age", vf.createLiteral(32));
        final VisibilityBindingSet original = new VisibilityBindingSet(bs, "a|b|c");

        // Write a VisibilityBindingSet to the test topic.
        final Properties producerProps = kafka.createBootstrapServerConfig();
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VisibilityBindingSetSerializer.class.getName());

        try(final KafkaProducer<String, VisibilityBindingSet> producer = new KafkaProducer<>(producerProps)) {
            producer.send( new ProducerRecord<String, VisibilityBindingSet>(kafka.getKafkaTopicName(), original) );
        }

        // Read a VisibilityBindingSet from the test topic.
        VisibilityBindingSet read;

        final Properties consumerProps = kafka.createBootstrapServerConfig();
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VisibilityBindingSetDeserializer.class.getName());

        try(final KafkaConsumer<String, VisibilityBindingSet> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Arrays.asList(kafka.getKafkaTopicName()));
            final ConsumerRecords<String, VisibilityBindingSet> records = consumer.poll(1000);

            assertEquals(1, records.count());
            read = records.iterator().next().value();
        }

        // Show the written statement matches the read one.
        assertEquals(original, read);
    }
}