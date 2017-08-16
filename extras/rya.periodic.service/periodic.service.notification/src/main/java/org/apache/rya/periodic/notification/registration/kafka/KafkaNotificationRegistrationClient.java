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

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.rya.periodic.notification.api.Notification;
import org.apache.rya.periodic.notification.api.PeriodicNotificationClient;
import org.apache.rya.periodic.notification.notification.BasicNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification.Command;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Implementation of {@link PeriodicNotificaitonClient} used to register new notification
 *  requests with the PeriodicQueryService.
 *
 */
public class KafkaNotificationRegistrationClient implements PeriodicNotificationClient {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaNotificationRegistrationClient.class);


    private final KafkaProducer<String, CommandNotification> producer;
    private final String topic;

    public KafkaNotificationRegistrationClient(final String topic, final KafkaProducer<String, CommandNotification> producer) {
        this.topic = topic;
        this.producer = producer;
    }

    @Override
    public void addNotification(final PeriodicNotification notification) {
        processNotification(new CommandNotification(Command.ADD, notification));

    }

    @Override
    public void deleteNotification(final BasicNotification notification) {
        processNotification(new CommandNotification(Command.DELETE, notification));
    }

    @Override
    public void deleteNotification(final String notificationId) {
        processNotification(new CommandNotification(Command.DELETE, new BasicNotification(notificationId)));
    }

    @Override
    public void addNotification(final String id, final long period, final long delay, final TimeUnit unit) {
        final Notification notification = PeriodicNotification.builder().id(id).period(period).initialDelay(delay).timeUnit(unit).build();
        processNotification(new CommandNotification(Command.ADD, notification));
    }


    private void processNotification(final CommandNotification notification) {
        LOG.info("Publishing to topic '{}' notification: {}", topic, notification);
        producer.send(new ProducerRecord<String, CommandNotification>(topic, notification.getId(), notification));
    }

    @Override
    public void close() {
        producer.close();
    }


}
