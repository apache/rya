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

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLog;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLogFactory;
import org.apache.rya.streams.querymanager.QueryChangeLogSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Represents a Kafka Server that manages {@link KafkaQueryChangeLog}s.
 * <p/>
 * Thread safe.
 */
@DefaultAnnotation(NonNull.class)
public class KafkaQueryChangeLogSource extends AbstractScheduledService implements QueryChangeLogSource {

    private static final Logger log = LoggerFactory.getLogger(KafkaQueryChangeLogSource.class);

    /**
     * Ensures thread safe interactions with this object.
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Used by the service to configure how often it polls the Kafka Server for topics.
     */
    private final Scheduler scheduler;

    /**
     * Which Kafka Server this source represents.
     */
    private final String kafkaBootstrapServer;

    /**
     * Listeners that need to be notified when logs are created/deleted.
     */
    private final Set<SourceListener> listeners = new HashSet<>();

    /**
     * Maps Rya instance names to the Query Change Log topic name in Kafka. This map is used to
     * keep track of how the change logs change over time within the Kafka Server.
     */
    private final HashMap<String, String> knownChangeLogs = new HashMap<>();

    /**
     * A consumer that is used to poll the Kafka Server for topics.
     */
    private KafkaConsumer<String, String> listTopicsConsumer;

    /**
     * Constructs an instance of {@link KafkaQueryChangeLogSource}.
     *
     * @param kafkaHostname - The hostname of the Kafka Server that is the source. (not null)
     * @param kafkaPort - The port of the Kafka Server that is the source. (not null)
     * @param scheduler - How frequently this source will poll the Kafka Server for topics. (not null)
     */
    public KafkaQueryChangeLogSource(
            final String kafkaHostname,
            final int kafkaPort,
            final Scheduler scheduler) {
        kafkaBootstrapServer = requireNonNull(kafkaHostname) + ":" + kafkaPort;
        this.scheduler = requireNonNull(scheduler);
    }

    @Override
    protected void startUp() throws Exception {
        log.info("Kafka Query Change Log Source watching " + kafkaBootstrapServer + " starting up...");

        // Setup the consumer that is used to list topics for the source.
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServer);
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        listTopicsConsumer = new KafkaConsumer<>(consumerProperties);

        log.info("Kafka Query Change Log Source watching " + kafkaBootstrapServer + " started.");
    }

    @Override
    protected void shutDown() throws Exception {
        log.info("Kafka Query Change Log Source watching " + kafkaBootstrapServer + " shutting down...");

        lock.lock();
        try {
            // Shut down the consumer that's used to list topics.
            listTopicsConsumer.close();
        } finally {
            lock.unlock();
        }

        log.info("Kafka Query Change Log Source watching " + kafkaBootstrapServer + " shut down.");
    }

    @Override
    public void subscribe(final SourceListener listener) {
        requireNonNull(listener);
        lock.lock();
        try {
            // Add the listener to the list of known listeners.
            listeners.add(listener);

            // Notify it with everything that already exists.
            for(final Entry<String, String> entry : knownChangeLogs.entrySet()) {
                final String changeLogTopic = entry.getValue();
                final KafkaQueryChangeLog changeLog = KafkaQueryChangeLogFactory.make(kafkaBootstrapServer, changeLogTopic);
                listener.notifyCreate(entry.getKey(), changeLog);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void unsubscribe(final SourceListener listener) {
        requireNonNull(listener);
        lock.lock();
        try {
            // Remove the listener from the list of known listeners.
            listeners.remove(listener);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void runOneIteration() throws Exception {
        lock.lock();
        try {
            // Get the list of topics from the Kafka Server.
            final Set<String> changeLogTopics = new HashSet<>( listTopicsConsumer.listTopics().keySet() );

            // Remove all topics that are not valid Rya Query Change Log topic names.
            changeLogTopics.removeIf( topic -> !KafkaTopics.getRyaInstanceFromQueryChangeLog(topic).isPresent() );

            // Extract the Rya instance names from the change log topics.
            final Set<String> ryaInstances = changeLogTopics.stream()
                    .map(topic -> KafkaTopics.getRyaInstanceFromQueryChangeLog(topic).get() )
                    .collect(Collectors.toSet());

            // Any Rya instances that are in the old set of topics, but not the new one, have been deleted.
            final Set<String> deletedRyaInstances = new HashSet<>( Sets.difference(knownChangeLogs.keySet(), ryaInstances) );

            // Any Rya instances that are in the new set of topics, but not the old set, have been created.
            final Set<String> createdRyaInstances = new HashSet<>( Sets.difference(ryaInstances, knownChangeLogs.keySet()) );

            // Handle the deletes.
            for(final String deletedRyaInstance : deletedRyaInstances) {
                // Remove the change log from the set of known logs.
                knownChangeLogs.remove(deletedRyaInstance);

                // Notify the listeners of the update so that they may close the previously provided change log.
                for(final SourceListener listener : listeners) {
                    listener.notifyDelete(deletedRyaInstance);
                }
            }

            // Handle the adds.
            for(final String createdRyaInstance : createdRyaInstances) {
                // Create and store the ChangeLog.
                final String changeLogTopic = KafkaTopics.queryChangeLogTopic(createdRyaInstance);
                knownChangeLogs.put(createdRyaInstance, changeLogTopic);

                // Notify the listeners of the update.
                for(final SourceListener listener : listeners) {
                    final KafkaQueryChangeLog changeLog = KafkaQueryChangeLogFactory.make(kafkaBootstrapServer, changeLogTopic);
                    listener.notifyCreate(createdRyaInstance, changeLog);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected Scheduler scheduler() {
        return scheduler;
    }
}