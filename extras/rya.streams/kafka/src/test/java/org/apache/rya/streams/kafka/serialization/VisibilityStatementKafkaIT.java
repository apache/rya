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
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.apache.rya.test.kafka.KafkaTestUtil;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Integration tests the {@link VisibilityStatementSerde} class' methods.
 */
public class VisibilityStatementKafkaIT {

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    @Test
    public void readAndWrite() throws Exception {
        // Create the object that will be written to the topic.
        final ValueFactory vf = new ValueFactoryImpl();
        final VisibilityStatement original = new VisibilityStatement(
                vf.createStatement(
                        vf.createURI("urn:alice"),
                        vf.createURI("urn:age"),
                        vf.createLiteral(32),
                        vf.createURI("urn:context")),
                "a|b|c");

        // Write a VisibilityStatement to the test topic.
        try(Producer<String, VisibilityStatement> producer = KafkaTestUtil.makeProducer(
                kafka, StringSerializer.class, VisibilityStatementSerializer.class)) {
            producer.send( new ProducerRecord<String, VisibilityStatement>(kafka.getKafkaTopicName(), original) );
        }

        // Read a VisibilityStatement from the test topic.
        try(Consumer<String, VisibilityStatement> consumer = KafkaTestUtil.fromStartConsumer(
                kafka, StringDeserializer.class, VisibilityStatementDeserializer.class)) {
            // Register the topic.
            consumer.subscribe(Arrays.asList(kafka.getKafkaTopicName()));

            // Poll for the result.
            final List<VisibilityStatement> results = KafkaTestUtil.pollForResults(500, 6, 1, consumer);

            // Show the written statement matches the read one.
            final VisibilityStatement read = results.iterator().next();
            assertEquals(original, read);
        }
    }
}