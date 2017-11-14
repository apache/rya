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

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.function.projection.RandomUUIDFactory;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.RunQuery;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.kafka.topology.TopologyBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A Kafka Streams implementation of {@link RunQuery}.
 */
@DefaultAnnotation(NonNull.class)
public class KafkaRunQuery implements RunQuery {
    private static final Logger log = LoggerFactory.getLogger(KafkaRunQuery.class);

    private final String kafkaHostname;
    private final String kafkaPort;
    private final String statementsTopic;
    private final String resultsTopic;
    private final TopologyBuilderFactory topologyFactory;
    private final QueryRepository queryRepo;

    /**
     * Constructs an instance of {@link KafkaRunQuery}.
     *
     * @param kafkaHostname - The hostname of the Kafka Broker to connect to. (not null)
     * @param kafkaPort - The port of the Kafka Broker to connect to. (not null)
     * @param statementsTopic - The name of the topic that statements will be read from. (not null)
     * @param resultsTopic - The name of the topic that query results will be writen to. (not null)
     * @param queryRepo - The query repository that holds queries that are registered. (not null)
     * @param topologyFactory - Builds Kafka Stream processing topologies from SPARQL. (not null)
     */
    public KafkaRunQuery(
            final String kafkaHostname,
            final String kafkaPort,
            final String statementsTopic,
            final String resultsTopic,
            final QueryRepository queryRepo,
            final TopologyBuilderFactory topologyFactory) {
        this.kafkaHostname = requireNonNull( kafkaHostname );
        this.kafkaPort = requireNonNull( kafkaPort );
        this.statementsTopic = requireNonNull(statementsTopic );
        this.resultsTopic = requireNonNull( resultsTopic );
        this.topologyFactory = requireNonNull( topologyFactory );
        this.queryRepo = requireNonNull( queryRepo );
    }

    @Override
    public void run(final UUID queryId) throws RyaStreamsException {
        requireNonNull(queryId);

        // Fetch the query from the repository. Throw an exception if it isn't present.
        final Optional<StreamsQuery> query = queryRepo.get(queryId);
        if(!query.isPresent()) {
            throw new RyaStreamsException("Could not run the Query with ID " + queryId + " because no such query " +
                    "is currently registered.");
        }

        // Build a processing topology using the SPARQL, provided statements topic, and provided results topic.
        final String sparql = query.get().getSparql();
        final TopologyBuilder topologyBuilder;
        try {
            topologyBuilder = topologyFactory.build(sparql, statementsTopic, resultsTopic, new RandomUUIDFactory());
        } catch (final Exception e) {
            throw new RyaStreamsException("Could not run the Query with ID " + queryId + " because a processing " +
                    "topolgoy could not be built for the SPARQL " + sparql, e);
        }

        // Setup the Kafka Stream program.
        final Properties streamsProps = new Properties();
        streamsProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaHostname + ":" + kafkaPort);
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "KafkaRunQuery-" + UUID.randomUUID());

        final KafkaStreams streams = new KafkaStreams(topologyBuilder, new StreamsConfig(streamsProps));

        // If an unhandled exception is thrown, rethrow it.
        streams.setUncaughtExceptionHandler((t, e) -> {
            // Log the problem and kill the program.
            log.error("Unhandled exception while processing the Rya Streams query. Shutting down.", e);
            System.exit(1);
        });

        // Setup a shutdown hook that kills the streams program at shutdown.
        final CountDownLatch awaitTermination = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                awaitTermination.countDown();
            }
        });

        // Run the streams program and wait for termination.
        streams.start();
        try {
            awaitTermination.await();
        } catch (final InterruptedException e) {
            log.warn("Interrupted while waiting for termination. Shutting down.");
        }
        streams.close();
    }
}