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
package org.apache.rya.periodic.notification.registration.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.rya.periodic.notification.api.LifeCycle;
import org.apache.rya.periodic.notification.api.Notification;
import org.apache.rya.periodic.notification.api.NotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumer group to pull all requests for adding and deleting {@link Notification}s
 * from Kafka.  This Object executes {@link PeriodicNotificationConsumer}s that retrieve
 * the {@link CommandNotification}s and register them with the {@link NotificationCoordinatorExecutor}.
 *
 */
public class KafkaNotificationProvider implements LifeCycle {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationProvider.class);
    private String topic;
    private ExecutorService executor;
    private NotificationCoordinatorExecutor coord;
    private Properties props;
    private int numThreads;
    private boolean running = false;
    Deserializer<String> keyDe;
    Deserializer<CommandNotification> valDe;
    List<PeriodicNotificationConsumer> consumers;

    /**
     * Create KafkaNotificationProvider for reading new notification requests form Kafka
     * @param topic - notification topic    
     * @param keyDe - Kafka message key deserializer
     * @param valDe - Kafka message value deserializer
     * @param props - properties used to creates a {@link KafkaConsumer}
     * @param coord - {@link NotificationCoordinatorExecutor} for managing and generating notifications
     * @param numThreads - number of threads used by this notification provider
     */
    public KafkaNotificationProvider(String topic, Deserializer<String> keyDe, Deserializer<CommandNotification> valDe, Properties props,
            NotificationCoordinatorExecutor coord, int numThreads) {
        this.coord = coord;
        this.numThreads = numThreads;
        this.topic = topic;
        this.props = props;
        this.consumers = new ArrayList<>();
        this.keyDe = keyDe;
        this.valDe = valDe;
    }

    @Override
    public void stop() {
        if (consumers != null && consumers.size() > 0) {
            for (PeriodicNotificationConsumer consumer : consumers) {
                consumer.shutdown();
            }
        }
        if (executor != null) {
            executor.shutdown();
        }
        running = false;
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                LOG.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.info("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void start() {
        if (!running) {
            if (!coord.currentlyRunning()) {
                coord.start();
            }
            // now launch all the threads
            executor = Executors.newFixedThreadPool(numThreads);

            // now create consumers to consume the messages
            int threadNumber = 0;
            for (int i = 0; i < numThreads; i++) {
                LOG.info("Creating consumer:" + threadNumber);
                KafkaConsumer<String, CommandNotification> consumer = new KafkaConsumer<String, CommandNotification>(props, keyDe, valDe);
                PeriodicNotificationConsumer periodicConsumer = new PeriodicNotificationConsumer(topic, consumer, threadNumber, coord);
                consumers.add(periodicConsumer);
                executor.submit(periodicConsumer);
                threadNumber++;
            }
            running = true;
        }
    }

    @Override
    public boolean currentlyRunning() {
        return running;
    }

}
