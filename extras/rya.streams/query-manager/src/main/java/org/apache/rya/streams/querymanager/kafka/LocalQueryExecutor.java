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
package org.apache.rya.streams.querymanager.kafka;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.kafka.KafkaStreamsFactory;
import org.apache.rya.streams.kafka.KafkaStreamsFactory.KafkaStreamsFactoryException;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.interactor.CreateKafkaTopic;
import org.apache.rya.streams.querymanager.QueryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import kafka.consumer.KafkaStream;

/**
 * A {@link QueryExecutor} that runs a {@link KafkaStreams} job within its own JVM every
 * time {@link #startQuery(String, StreamsQuery)} is invoked.
 * <p/>
 * This executor may run out of JVM resources if it is used to execute too many queries.
 */
@DefaultAnnotation(NonNull.class)
public class LocalQueryExecutor extends AbstractIdleService implements QueryExecutor {
    private static final Logger log = LoggerFactory.getLogger(LocalQueryExecutor.class);

    /**
     * Provides thread safety when interacting with this class.
     */
    public static ReentrantLock lock = new ReentrantLock();

    /**
     * Lookup the Rya Instance of a specific Query Id.
     */
    private final Map<UUID, String> ryaInstanceById = new HashMap<>();

    /**
     * Lookup the Query IDs that are running for a specific Rya Instance.
     */
    private final Multimap<String, UUID> idByRyaInstance = HashMultimap.create();

    /**
     * Lookup the executing {@link KafkaStreams} job for a running Query Id.
     */
    private final Map<UUID, KafkaStreams> byQueryId = new HashMap<>();

    /**
     * Used to create the input and output topics for a Kafka Streams job.
     */
    private final CreateKafkaTopic createKafkaTopic;

    /**
     * Builds the {@link KafkaStreams} objects that execute {@link KafkaStream}s.
     */
    private final KafkaStreamsFactory streamsFactory;

    /**
     * Constructs an instance of {@link LocalQueryExecutor}.
     *
     * @param createKafkaTopic - Used to create the input and output topics for a Kafka Streams job. (not null)
     * @param streamsFactory - Builds the {@link KafkaStreams} objects that execute {@link KafkaStream}s. (not null)
     */
    public LocalQueryExecutor(
            final CreateKafkaTopic createKafkaTopic,
            final KafkaStreamsFactory streamsFactory) {
        this.createKafkaTopic = requireNonNull(createKafkaTopic);
        this.streamsFactory = requireNonNull(streamsFactory);
    }

    @Override
    protected void startUp() throws Exception {
        log.info("Local Query Executor starting up.");
    }

    @Override
    protected void shutDown() throws Exception {
        log.info("Local Query Executor shutting down. Stopping all jobs...");

        // Stop all of the running queries.
        for(final KafkaStreams job : byQueryId.values()) {
            job.close();
        }

        log.info("Local Query Executor shut down.");
    }

    @Override
    public void startQuery(final String ryaInstance, final StreamsQuery query) throws QueryExecutorException {
        requireNonNull(ryaInstance);
        requireNonNull(query);
        checkState(state() == State.RUNNING, "The service must be RUNNING to execute this method.");

        lock.lock();
        try {
            // Make sure the Statements topic exists for the query.
            final Set<String> topics = Sets.newHashSet(
                    KafkaTopics.statementsTopic(ryaInstance),
                    KafkaTopics.queryResultsTopic(ryaInstance, query.getQueryId()));

            // Make sure the Query Results topic exists for the query.
            // Since this is running in the JVM, the properties are left empty
            //   so the cleanup.policy will default to delete to reduce memory usage.
            createKafkaTopic.createTopics(topics, 1, 1, Optional.empty());

            // Setup the Kafka Streams job that will execute.
            final KafkaStreams streams = streamsFactory.make(ryaInstance, query);
            streams.start();

            // Mark which Rya Instance the Query ID is for.
            ryaInstanceById.put(query.getQueryId(), ryaInstance);

            // Add the Query ID to the collection of running queries for the Rya instance.
            idByRyaInstance.put(ryaInstance, query.getQueryId());

            // Add the running Kafka Streams job for the Query ID.
            byQueryId.put(query.getQueryId(), streams);

        } catch (final KafkaStreamsFactoryException e) {
            throw new QueryExecutorException("Could not start query " + query.getQueryId(), e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stopQuery(final UUID queryId) throws QueryExecutorException {
        requireNonNull(queryId);
        checkState(state() == State.RUNNING, "The service must be RUNNING to execute this method.");

        lock.lock();
        try {
            if(byQueryId.containsKey(queryId)) {
                // Stop the job from running.
                final KafkaStreams streams = byQueryId.get(queryId);
                streams.close();

                // Remove it from the Rya Instance Name lookup.
                final String ryaInstance = ryaInstanceById.remove(queryId);

                // Remove it from the collection of running queries for the Rya Instance.
                idByRyaInstance.remove(ryaInstance, queryId);

                // Remove it from the running Kafka Streams job lookup.
                byQueryId.remove(queryId);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stopAll(final String ryaInstanceName) throws QueryExecutorException {
        requireNonNull(ryaInstanceName);
        checkState(state() == State.RUNNING, "The service must be RUNNING to execute this method.");

        lock.lock();
        try {
            if(idByRyaInstance.containsKey(ryaInstanceName)) {
                // A defensive copy of the queries so that we may remove them from the maps.
                final Set<UUID> queryIds = new HashSet<>( idByRyaInstance.get(ryaInstanceName) );

                // Stop each of them.
                for(final UUID queryId : queryIds) {
                    stopQuery(queryId);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Set<UUID> getRunningQueryIds() throws QueryExecutorException {
        lock.lock();
        checkState(state() == State.RUNNING, "The service must be RUNNING to execute this method.");

        try {
            return new HashSet<>( byQueryId.keySet() );
        } finally {
            lock.unlock();
        }
    }
}