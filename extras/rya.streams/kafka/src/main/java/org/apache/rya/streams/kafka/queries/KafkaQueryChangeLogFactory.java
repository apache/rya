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
package org.apache.rya.streams.kafka.queries;

import static java.util.Objects.requireNonNull;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeDeserializer;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeSerializer;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Creates instances of {@link KafkaQueryChangeLog}.
 */
@DefaultAnnotation(NonNull.class)
public class KafkaQueryChangeLogFactory {

    /**
     * Creates an instance of {@link KafkaQueryChangeLog} using a new {@link Producer} and {@link Consumer}.
     *
     * @param bootstrapServers - Indicates which instance of Kafka that will be connected to. (not null)
     * @param topic - The topic the QueryChangeLog is persisted to. (not null)
     * @return A new instance of {@link KafkaQueryChangeLog}.
     */
    public static KafkaQueryChangeLog make(
            final String bootstrapServers,
            final String topic) {
        requireNonNull(bootstrapServers);
        requireNonNull(topic);

        final Properties producerProperties = new Properties();
        producerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, QueryChangeSerializer.class.getName());

        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, QueryChangeDeserializer.class.getName());

        final Producer<?, QueryChange> producer = new KafkaProducer<>(producerProperties);
        final Consumer<?, QueryChange> consumer = new KafkaConsumer<>(consumerProperties);
        return new KafkaQueryChangeLog(producer, consumer, topic);
    }
}