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

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.pcj.fluo.test.base.KafkaExportITBase;
import org.apache.rya.periodic.notification.coordinator.PeriodicNotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.apache.rya.periodic.notification.serialization.CommandNotificationSerializer;
import org.junit.Assert;
import org.junit.Test;

public class PeriodicCommandNotificationConsumerIT extends KafkaExportITBase {

    private static final String topic = "topic-" + UUID.randomUUID();  // tests are dependent on each other
    private KafkaNotificationRegistrationClient registration;
    private PeriodicNotificationCoordinatorExecutor coord;
    private KafkaNotificationProvider provider;

    @Test
    public void kafkaNotificationProviderTest() throws InterruptedException {
        final BlockingQueue<TimestampedNotification> notifications = new LinkedBlockingQueue<>();
        final Properties props = createKafkaConfig();
        final KafkaProducer<String, CommandNotification> producer = new KafkaProducer<>(props);
        registration = new KafkaNotificationRegistrationClient(topic, producer);
        coord = new PeriodicNotificationCoordinatorExecutor(1, notifications);
        provider = new KafkaNotificationProvider(topic, new StringDeserializer(), new CommandNotificationSerializer(), props, coord, 1);
        provider.start();

        registration.addNotification("1", 1, 0, TimeUnit.SECONDS);
        Thread.sleep(4000);
        // check that notifications are being added to the blocking queue
        Assert.assertEquals(true, notifications.size() > 0);

        registration.deleteNotification("1");
        Thread.sleep(2000);
        final int size = notifications.size();
        // sleep for 2 seconds to ensure no more messages being produced
        Thread.sleep(2000);
        Assert.assertEquals(size, notifications.size());

        tearDown();
    }

    @Test
    public void kafkaNotificationMillisProviderTest() throws InterruptedException {
        final BlockingQueue<TimestampedNotification> notifications = new LinkedBlockingQueue<>();
        final Properties props = createKafkaConfig();
        final KafkaProducer<String, CommandNotification> producer = new KafkaProducer<>(props);
        registration = new KafkaNotificationRegistrationClient(topic, producer);
        coord = new PeriodicNotificationCoordinatorExecutor(1, notifications);
        provider = new KafkaNotificationProvider(topic, new StringDeserializer(), new CommandNotificationSerializer(), props, coord, 1);
        provider.start();

        registration.addNotification("1", 1000, 0, TimeUnit.MILLISECONDS);
        Thread.sleep(4000);
        // check that notifications are being added to the blocking queue
        Assert.assertEquals(true, notifications.size() > 0);

        registration.deleteNotification("1");
        Thread.sleep(2000);
        final int size = notifications.size();
        // sleep for 2 seconds to ensure no more messages being produced
        Thread.sleep(2000);
        Assert.assertEquals(size, notifications.size());

        tearDown();
    }

    private void tearDown() {
        registration.close();
        provider.stop();
        coord.stop();
    }

    private Properties createKafkaConfig() {
        final Properties props = createBootstrapServerConfig();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group0");
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "consumer0");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommandNotificationSerializer.class.getName());

        return props;
    }
}