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
import org.apache.log4j.Logger;
import org.apache.rya.periodic.notification.api.NotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.notification.CommandNotification;

/**
 * Consumer for the {@link KafkaNotificationProvider}.  This consumer pull messages
 * from Kafka and registers them with the {@link NotificationCoordinatorExecutor}.
 *
 */
public class PeriodicNotificationConsumer implements Runnable {
    private KafkaConsumer<String, CommandNotification> consumer;
    private int m_threadNumber;
    private String topic;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private NotificationCoordinatorExecutor coord;
    private static final Logger LOG = Logger.getLogger(PeriodicNotificationConsumer.class);

    /**
     * Creates a new PeriodicNotificationConsumer for consuming new notification requests from
     * Kafka.
     * @param topic - new notification topic
     * @param consumer - consumer for pulling new requests from Kafka
     * @param a_threadNumber - number of consumer threads to be used
     * @param coord - notification coordinator for managing and generating notifications
     */
    public PeriodicNotificationConsumer(String topic, KafkaConsumer<String, CommandNotification> consumer, int a_threadNumber,
            NotificationCoordinatorExecutor coord) {
        this.topic = topic;
        m_threadNumber = a_threadNumber;
        this.consumer = consumer;
        this.coord = coord;
    }

    public void run() {
        
        try {
            LOG.info("Creating kafka stream for consumer:" + m_threadNumber);
            consumer.subscribe(Arrays.asList(topic));
            while (!closed.get()) {
                ConsumerRecords<String, CommandNotification> records = consumer.poll(10000);
                // Handle new records
                for(ConsumerRecord<String, CommandNotification> record: records) {
                    CommandNotification notification = record.value();
                    LOG.info("Thread " + m_threadNumber + " is adding notification " + notification + " to queue.");
                    LOG.info("Message: " + notification);
                    coord.processNextCommandNotification(notification);
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }
    
    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }
}
