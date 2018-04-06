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
package org.apache.rya.test.kafka;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
     * @param kafka - The Kafka rule used to connect to the embedded Kafka instance. (not null)
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
     * @param kafka - The Kafka rule used to connect to the embedded Kafka instance. (not null)
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
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getName());
        return new KafkaConsumer<>(props);
    }

    /**
     * Polls a {@link Consumer} until it has either polled too many times without hitting the target number
     * of results, or it hits the target number of results.
     *
     * @param pollMs - How long each poll could take.
     * @param pollIterations - The maximum number of polls that will be attempted.
     * @param targetSize - The number of results to read before stopping.
     * @param consumer - The consumer that will be polled.
     * @return The results that were read from the consumer.
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
}