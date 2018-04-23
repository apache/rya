/**
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

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.function.projection.RandomUUIDFactory;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.kafka.topology.TopologyBuilderFactory;
import org.apache.rya.streams.kafka.topology.TopologyBuilderFactory.TopologyBuilderException;
import org.apache.rya.streams.kafka.topology.TopologyFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Creates {@link KafkaStreams} objects that are able to process {@link StreamsQuery}s
 * using a single thread of execution starting from the earliest point in within the
 * input topic. The Application ID used by the client is based on the Query ID of the
 * query that is being executed so that this job may resume where it left off if it
 * is stopped.
 */
@DefaultAnnotation(NonNull.class)
public class SingleThreadKafkaStreamsFactory implements KafkaStreamsFactory {

    private final TopologyBuilderFactory topologyFactory = new TopologyFactory();

    private final String bootstrapServersConfig;

    /**
     * Constructs an instance of {@link SingleThreadKafkaStreamsFactory}.
     *
     * @param bootstrapServersConfig - Configures which Kafka cluster the jobs will interact with. (not null)
     */
    public SingleThreadKafkaStreamsFactory(final String bootstrapServersConfig) {
        this.bootstrapServersConfig = requireNonNull(bootstrapServersConfig);
    }

    @Override
    public KafkaStreams make(final String ryaInstance, final StreamsQuery query) throws KafkaStreamsFactoryException {
        requireNonNull(ryaInstance);
        requireNonNull(query);

        // Setup the Kafka Stream program.
        final Properties streamsProps = new Properties();

        // Configure the Kafka servers that will be talked to.
        streamsProps.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);

        // Use the Query ID as the Application ID to ensure we resume where we left off the last time this command was run.
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "RyaStreams-Query-" + query.getQueryId());

        // Always start at the beginning of the input topic.
        streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Setup the topology that processes the Query.
        final String statementsTopic = KafkaTopics.statementsTopic(ryaInstance);
        final String resultsTopic = KafkaTopics.queryResultsTopic(ryaInstance, query.getQueryId());

        try {
            final TopologyBuilder topologyBuilder = topologyFactory.build(query.getSparql(), statementsTopic, resultsTopic, new RandomUUIDFactory());
            return new KafkaStreams(topologyBuilder, new StreamsConfig(streamsProps));
        } catch (final MalformedQueryException | TopologyBuilderException e) {
            throw new KafkaStreamsFactoryException("Could not create a KafkaStreams processing topology for query " + query.getQueryId(), e);
        }
    }
}