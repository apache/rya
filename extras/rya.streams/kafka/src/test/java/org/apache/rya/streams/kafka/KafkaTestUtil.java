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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.kafka.interactor.KafkaLoadStatements;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementSerializer;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;

import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A set of utility functions that are useful when writing tests against a Kafka instance.
 */
@DefaultAnnotation(NonNull.class)
public final class KafkaTestUtil {

    private KafkaTestUtil() { }

    /**
     * Create a {@link Producer} that is able to write to a topic that is hosted within an embedded instance of Kafka.
     *
     * @param kafka - The Kafka rule used to connect to the embedded Kafkfa instance. (not null)
     * @param keySerializerClass - Serializes the keys. (not null)
     * @param valueSerializerClass - Serializes the values. (not null)
     * @return A {@link Producer} that can be used to write records to a topic.
     */
    public static <K, V> Producer<K, V> makeProducer(
            final KafkaTestInstanceRule kafka,
            final Class<? extends Serializer<K>> keySerializerClass,
            final Class<? extends Serializer<V>> valueSerializerClass) {
        requireNonNull(kafka);
        requireNonNull(keySerializerClass);
        requireNonNull(valueSerializerClass);

        final Properties props = kafka.createBootstrapServerConfig();
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());
        return new KafkaProducer<>(props);
    }

    /**
     * Create a {@link Consumer} that has a unique group ID and reads everything from a topic that is hosted within an
     * embedded instance of Kafka starting at the earliest point by default.
     *
     * @param kafka - The Kafka rule used to connect to the embedded Kafkfa instance. (not null)
     * @param keyDeserializerClass - Deserializes the keys. (not null)
     * @param valueDeserializerClass - Deserializes the values. (not null)
     * @return A {@link Consumer} that can be used to read records from a topic.
     */
    public static <K, V> Consumer<K, V> fromStartConsumer(
            final KafkaTestInstanceRule kafka,
            final Class<? extends Deserializer<K>> keyDeserializerClass,
            final Class<? extends Deserializer<V>> valueDeserializerClass) {
        requireNonNull(kafka);
        requireNonNull(keyDeserializerClass);
        requireNonNull(valueDeserializerClass);

        final Properties props = kafka.createBootstrapServerConfig();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getName());
        return new KafkaConsumer<>(props);
    }

    /**
     * Polls a {@link Consumer> until it has either polled too many times without hitting the target number
     * of results, or it hits the target number of results.
     *
     * @param pollMs - How long each poll could take.
     * @param pollIterations - The maximum number of polls that will be attempted.
     * @param targetSize - The number of results to read before stopping.
     * @param consumer - The consumer that will be polled.
     * @return The results that were read frmo the consumer.
     * @throws Exception If the poll failed.
     */
    public static <K, V> List<V> pollForResults(
            final int pollMs,
            final int pollIterations,
            final int targetSize,
            final Consumer<K, V> consumer) throws Exception {
        requireNonNull(consumer);

        final List<V> values = new ArrayList<>();

        int i = 0;
        while(values.size() < targetSize && i < pollIterations) {
            for(final ConsumerRecord<K, V> record : consumer.poll(pollMs)) {
                values.add( record.value() );
            }
            i++;
        }

        return values;
    }

    /**
     * Runs a Kafka Streams topology, loads statements into the input topic, read the binding sets that come out of
     * the results topic, and ensures the expected results match the read results.
     *
     * @param kafka - The embedded kafka instance that is being tested with. (not null)
     * @param statementsTopic - The topic statements will be written to. (not null)
     * @param resultsTopic - The topic results will be read from. (not null)
     * @param builder - The streams topology that will be executed. (not null)
     * @param startupMs - How long to wait for the topology to start before writing the statements.
     * @param statements - The statements that will be loaded into the topic. (not null)
     * @param expected - The expected results. (not null)
     * @throws Exception If any exception was thrown while running the test.
     */
    public static void runStreamProcessingTest(
            final KafkaTestInstanceRule kafka,
            final String statementsTopic,
            final String resultsTopic,
            final TopologyBuilder builder,
            final int startupMs,
            final List<VisibilityStatement> statements,
            final Set<VisibilityBindingSet> expected) throws Exception {
        requireNonNull(kafka);
        requireNonNull(statementsTopic);
        requireNonNull(resultsTopic);
        requireNonNull(builder);
        requireNonNull(statements);
        requireNonNull(expected);

        // Explicitly create the topics that are being used.
        kafka.createTopic(statementsTopic);
        kafka.createTopic(resultsTopic);

        // Start the streams program.
        final Properties props = kafka.createBootstrapServerConfig();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StatementPatternProcessorIT");

        final KafkaStreams streams = new KafkaStreams(builder, new StreamsConfig(props));
        streams.cleanUp();
        try {
            streams.start();

            // Wait for the streams application to start. Streams only see data after their consumers are connected.
            Thread.sleep(startupMs);

            // Load the statements into the input topic.
            try(Producer<String, VisibilityStatement> producer = KafkaTestUtil.makeProducer(
                    kafka, StringSerializer.class, VisibilityStatementSerializer.class)) {
                new KafkaLoadStatements(statementsTopic, producer).fromCollection(statements);
            }

            // Wait for the final results to appear in the output topic and verify the expected Binding Sets were found.
            try(Consumer<String, VisibilityBindingSet> consumer = KafkaTestUtil.fromStartConsumer(
                    kafka, StringDeserializer.class, VisibilityBindingSetDeserializer.class)) {
                // Register the topic.
                consumer.subscribe(Arrays.asList(resultsTopic));

                // Poll for the result.
                final Set<VisibilityBindingSet> results = Sets.newHashSet( KafkaTestUtil.pollForResults(500, 6, expected.size(), consumer) );

                // Show the correct binding sets results from the job.
                assertEquals(expected, results);
            }
        } finally {
            streams.close();
        }
    }
}