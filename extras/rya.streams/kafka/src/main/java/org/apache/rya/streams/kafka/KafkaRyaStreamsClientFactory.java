/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.streams.api.RyaStreamsClient;
import org.apache.rya.streams.api.interactor.defaults.DefaultAddQuery;
import org.apache.rya.streams.api.interactor.defaults.DefaultDeleteQuery;
import org.apache.rya.streams.api.interactor.defaults.DefaultGetQuery;
import org.apache.rya.streams.api.interactor.defaults.DefaultListQueries;
import org.apache.rya.streams.api.interactor.defaults.DefaultStartQuery;
import org.apache.rya.streams.api.interactor.defaults.DefaultStopQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.kafka.interactor.KafkaGetQueryResultStream;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLog;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementDeserializer;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeDeserializer;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Constructs instances of {@link RyaStreamsClient} that are connected to a Kafka cluster.
 */
@DefaultAnnotation(NonNull.class)
public final class KafkaRyaStreamsClientFactory {
    private static final Logger log = LoggerFactory.getLogger(KafkaRyaStreamsClientFactory.class);

    /**
     * Initialize a {@link RyaStreamsClient} that will interact with an instance of Rya Streams
     * that is backed by Kafka.
     *
     * @param ryaInstance - The name of the Rya Instance the client is connected to. (not null)
     * @param kafkaHostname - The hostname of the Kafka Broker.
     * @param kafkaPort - The port of the Kafka Broker.
     * @return The initialized commands.
     */
    public static RyaStreamsClient make(
            final String ryaInstance,
            final String kafkaHostname,
            final int kafkaPort) {
        requireNonNull(ryaInstance);
        requireNonNull(kafkaHostname);

        // Setup Query Repository used by the Kafka Rya Streams subsystem.
        final Producer<?, QueryChange> queryProducer =
                makeProducer(kafkaHostname, kafkaPort, StringSerializer.class, QueryChangeSerializer.class);
        final Consumer<?, QueryChange>queryConsumer =
                fromStartConsumer(kafkaHostname, kafkaPort, StringDeserializer.class, QueryChangeDeserializer.class);
        final String changeLogTopic = KafkaTopics.queryChangeLogTopic(ryaInstance);
        final QueryChangeLog changeLog = new KafkaQueryChangeLog(queryProducer, queryConsumer, changeLogTopic);
        final QueryRepository queryRepo = new InMemoryQueryRepository(changeLog);

        // Create the Rya Streams client that is backed by a Kafka Query Change Log.
        return new RyaStreamsClient(
                new DefaultAddQuery(queryRepo),
                new DefaultGetQuery(queryRepo),
                new DefaultDeleteQuery(queryRepo),
                new KafkaGetQueryResultStream<>(kafkaHostname, "" + kafkaPort, VisibilityStatementDeserializer.class),
                new KafkaGetQueryResultStream<>(kafkaHostname, "" + kafkaPort, VisibilityBindingSetDeserializer.class),
                new DefaultListQueries(queryRepo),
                new DefaultStartQuery(queryRepo),
                new DefaultStopQuery(queryRepo)) {

            /**
             * Close the QueryRepository used by the returned client.
             */
            @Override
            public void close() {
                try {
                    queryRepo.close();
                } catch (final Exception e) {
                    log.warn("Couldn't close a QueryRepository.", e);
                }
            }
        };
    }

    /**
     * Create a {@link Producer} that is able to write to a topic in Kafka.
     *
     * @param kafkaHostname - The Kafka broker hostname. (not null)
     * @param kafkaPort - The Kafka broker port.
     * @param keySerializerClass - Serializes the keys. (not null)
     * @param valueSerializerClass - Serializes the values. (not null)
     * @return A {@link Producer} that can be used to write records to a topic.
     */
    private static <K, V> Producer<K, V> makeProducer(
            final String kafkaHostname,
            final int kakfaPort,
            final Class<? extends Serializer<K>> keySerializerClass,
            final Class<? extends Serializer<V>> valueSerializerClass) {
        requireNonNull(kafkaHostname);
        requireNonNull(keySerializerClass);
        requireNonNull(valueSerializerClass);

        final Properties producerProps = new Properties();
        producerProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaHostname + ":" + kakfaPort);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());
        return new KafkaProducer<>(producerProps);
    }

    /**
     * Create a {@link Consumer} that has a unique group ID and reads everything from a topic in Kafka
     * starting at the earliest point by default.
     *
     * @param kafkaHostname - The Kafka broker hostname. (not null)
     * @param kafkaPort - The Kafka broker port.
     * @param keyDeserializerClass - Deserializes the keys. (not null)
     * @param valueDeserializerClass - Deserializes the values. (not null)
     * @return A {@link Consumer} that can be used to read records from a topic.
     */
    private static <K, V> Consumer<K, V> fromStartConsumer(
            final String kafkaHostname,
            final int kakfaPort,
            final Class<? extends Deserializer<K>> keyDeserializerClass,
            final Class<? extends Deserializer<V>> valueDeserializerClass) {
        requireNonNull(kafkaHostname);
        requireNonNull(keyDeserializerClass);
        requireNonNull(valueDeserializerClass);

        final Properties consumerProps = new Properties();
        consumerProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaHostname + ":" + kakfaPort);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getName());
        return new KafkaConsumer<>(consumerProps);
    }
}