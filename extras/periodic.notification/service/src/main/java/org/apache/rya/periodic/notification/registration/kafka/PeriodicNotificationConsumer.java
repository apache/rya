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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.rya.periodic.notification.api.NotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumer for the {@link KafkaNotificationProvider}.  This consumer pull messages
 * from Kafka and registers them with the {@link NotificationCoordinatorExecutor}.
 *
 */
public class PeriodicNotificationConsumer implements Runnable {
    private final KafkaConsumer<String, CommandNotification> consumer;
    private final int threadNumber;
    private final String topic;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final NotificationCoordinatorExecutor coord;
    private static final Logger LOG = LoggerFactory.getLogger(PeriodicNotificationConsumer.class);

    /**
     * Creates a new PeriodicNotificationConsumer for consuming new notification requests from
     * Kafka.
     * @param topic - new notification topic
     * @param consumer - consumer for pulling new requests from Kafka
     * @param threadNumber - an identifier for this thread.
     * @param coord - notification coordinator for managing and generating notifications
     */
    public PeriodicNotificationConsumer(final String topic, final KafkaConsumer<String, CommandNotification> consumer, final int threadNumber,
            final NotificationCoordinatorExecutor coord) {
        this.topic = topic;
        this.threadNumber = threadNumber;
        this.consumer = consumer;
        this.coord = coord;
    }

    @Override
    public void run() {

        try {
            LOG.info("Configuring KafkaConsumer on thread: {} to subscribe to topic: {}", threadNumber, topic);
            consumer.subscribe(Arrays.asList(topic));
            while (!closed.get()) {
                final ConsumerRecords<String, CommandNotification> records = consumer.poll(10000);
                // Handle new records
                for(final ConsumerRecord<String, CommandNotification> record: records) {
                    final CommandNotification notification = record.value();
                    LOG.info("Thread {} is adding notification: {}", threadNumber, notification);
                    coord.processNextCommandNotification(notification);
                }
            }
            LOG.info("Finished polling.");
        } catch (final WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) {
                throw e;
            }
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
