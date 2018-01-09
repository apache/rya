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
package org.apache.rya.streams.kafka;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.interactor.KafkaLoadStatements;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementSerializer;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.apache.rya.test.kafka.KafkaTestUtil;

import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Utility functions that make it easier to test Rya Streams applications.
 */
@DefaultAnnotation(NonNull.class)
public class RyaStreamsTestUtil {

    /**
     * Runs a Kafka Streams topology, loads statements into the input topic, read the binding sets that come out of
     * the results topic, and ensures the expected results match the read results.
     *
     * @param <T> The type of value that will be consumed from the results topic.
     * @param kafka - The embedded Kafka instance that is being tested with. (not null)
     * @param statementsTopic - The topic statements will be written to. (not null)
     * @param resultsTopic - The topic results will be read from. (not null)
     * @param builder - The streams topology that will be executed. (not null)
     * @param statements - The statements that will be loaded into the topic. (not null)
     * @param expected - The expected results. (not null)
     * @param expectedDeserializerClass - The class of the deserializer that will be used when reading
     *   values from the results topic. (not null)
     * @throws Exception If any exception was thrown while running the test.
     */
    public static <T> void runStreamProcessingTest(
            final KafkaTestInstanceRule kafka,
            final String statementsTopic,
            final String resultsTopic,
            final TopologyBuilder builder,
            final List<VisibilityStatement> statements,
            final Set<T> expected,
            final Class<? extends Deserializer<T>> expectedDeserializerClass) throws Exception {
        requireNonNull(kafka);
        requireNonNull(statementsTopic);
        requireNonNull(resultsTopic);
        requireNonNull(builder);
        requireNonNull(statements);
        requireNonNull(expected);
        requireNonNull(expectedDeserializerClass);

        // Explicitly create the topics that are being used.
        kafka.createTopic(statementsTopic);
        kafka.createTopic(resultsTopic);

        // Start the streams program.
        final Properties props = kafka.createBootstrapServerConfig();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KafkaStreams streams = new KafkaStreams(builder, new StreamsConfig(props));
        streams.cleanUp();
        try {
            streams.start();

            // Wait for the streams application to start. Streams only see data after their consumers are connected.
            Thread.sleep(6000);

            // Load the statements into the input topic.
            try(Producer<String, VisibilityStatement> producer = KafkaTestUtil.makeProducer(
                    kafka, StringSerializer.class, VisibilityStatementSerializer.class)) {
                new KafkaLoadStatements(statementsTopic, producer).fromCollection(statements);
            }

            // Wait for the final results to appear in the output topic and verify the expected Binding Sets were found.
            try(Consumer<String, T> consumer = KafkaTestUtil.fromStartConsumer(kafka, StringDeserializer.class, expectedDeserializerClass)) {
                // Register the topic.
                consumer.subscribe(Arrays.asList(resultsTopic));

                // Poll for the result.
                final Set<T> results = Sets.newHashSet( KafkaTestUtil.pollForResults(500, 6, expected.size(), consumer) );

                // Show the correct binding sets results from the job.
                assertEquals(expected, results);
            }
        } finally {
            streams.close();
        }
    }
}