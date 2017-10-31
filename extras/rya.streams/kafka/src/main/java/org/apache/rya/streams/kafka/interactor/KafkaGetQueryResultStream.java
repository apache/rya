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

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.api.entity.QueryResultStream;
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.GetQueryResultStream;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.entity.KafkaQueryResultStream;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Kafka topic implementation of {@link GetQueryResultStream}.
 */
@DefaultAnnotation(NonNull.class)
public class KafkaGetQueryResultStream implements GetQueryResultStream {

    private final String bootstrapServers;

    /**
     * Constructs an instance of {@link KafkaGetQueryResultStream}.
     *
     * @param kafkaHostname - The hostname of the Kafka Broker to connect to. (not null)
     * @param kafkaPort - The port of the Kafka Broker to connect to. (not null)
     */
    public KafkaGetQueryResultStream(final String kafkaHostname, final String kafkaPort) {
        requireNonNull(kafkaHostname);
        requireNonNull(kafkaPort);
        bootstrapServers = kafkaHostname + ":" + kafkaPort;
    }

    @Override
    public QueryResultStream fromStart(final UUID queryId) throws RyaStreamsException {
        requireNonNull(queryId);

        // Always start at the earliest point within the topic.
        return makeStream(queryId, "earliest");
    }

    @Override
    public QueryResultStream fromNow(final UUID queryId) throws RyaStreamsException {
        requireNonNull(queryId);

        // Always start at the latest point within the topic.
        return makeStream(queryId, "latest");
    }

    private QueryResultStream makeStream(final UUID queryId, final String autoOffsetResetConfig) {
        // Configure which instance of Kafka to connect to.
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Nothing meaningful is in the key and the value is a VisibilityBindingSet.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VisibilityBindingSetDeserializer.class);

        // Use a UUID for the Group Id so that we never register as part of the same group as another consumer.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());

        // Set a client id so that server side logging can be traced.
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "Query-Result-Stream-" + queryId);

        // These consumers always start at a specific point and move forwards until the caller is finished with
        // the returned stream, so never commit the consumer's progress.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // We are not closing the consumer here because the returned QueryResultStream is responsible for closing the
        // underlying resources required to process it.
        final KafkaConsumer<Object, VisibilityBindingSet> consumer = new KafkaConsumer<>(props);

        // Register the consumer for the query's results.
        final String resultTopic = KafkaTopics.queryResultsTopic(queryId);
        consumer.subscribe(Arrays.asList(resultTopic));

        // Return the result stream.
        return new KafkaQueryResultStream(queryId, consumer);
    }
}