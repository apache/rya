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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.querymanager.QueryChangeLogSource;
import org.apache.rya.streams.querymanager.QueryChangeLogSource.SourceListener;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

/**
 * Integration tests the methods of {@link KafkaQueryChangeLogSource}.
 */
public class KafkaQueryChangeLogSourceIT {

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(false);

    @Before
    public void clearTopics() throws InterruptedException {
        kafka.deleteAllTopics();
    }

    @Test
    public void discoverExistingLogs() throws Exception {
        // Create a valid Query Change Log topic.
        final String ryaInstance = UUID.randomUUID().toString();
        final String topic = KafkaTopics.queryChangeLogTopic(ryaInstance);
        kafka.createTopic(topic);

        // Create the source.
        final QueryChangeLogSource source = new KafkaQueryChangeLogSource(
                kafka.getKafkaHostname(),
                Integer.parseInt( kafka.getKafkaPort() ),
                Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS));

        // Register a listener that counts down a latch if it sees the new topic.
        final CountDownLatch created = new CountDownLatch(1);
        source.subscribe(new SourceListener() {
            @Override
            public void notifyCreate(final String ryaInstanceName, final QueryChangeLog log) {
                assertEquals(ryaInstance, ryaInstanceName);
                created.countDown();
            }

            @Override
            public void notifyDelete(final String ryaInstanceName) { }
        });

        try {
            // Start the source.
            source.startAndWait();

            // If the latch isn't counted down, then fail the test.
            assertTrue( created.await(5, TimeUnit.SECONDS) );

        } finally {
            source.stopAndWait();
        }
    }

    @Test
    public void discoverNewLogs() throws Exception {
        // Create the source.
        final QueryChangeLogSource source = new KafkaQueryChangeLogSource(
                kafka.getKafkaHostname(),
                Integer.parseInt( kafka.getKafkaPort() ),
                Scheduler.newFixedRateSchedule(0, 100, TimeUnit.MILLISECONDS));

        // Register a listener that counts down a latch if it sees the new topic.
        final String ryaInstance = UUID.randomUUID().toString();
        final CountDownLatch created = new CountDownLatch(1);
        source.subscribe(new SourceListener() {
            @Override
            public void notifyCreate(final String ryaInstanceName, final QueryChangeLog log) {
                assertEquals(ryaInstance, ryaInstanceName);
                created.countDown();
            }

            @Override
            public void notifyDelete(final String ryaInstanceName) { }
        });

        try {
            // Start the source.
            source.startAndWait();

            // Wait twice the polling duration to ensure it iterates at least once.
            Thread.sleep(200);

            // Create a valid Query Change Log topic.
            final String topic = KafkaTopics.queryChangeLogTopic(ryaInstance);
            kafka.createTopic(topic);

            // If the latch isn't counted down, then fail the test.
            assertTrue( created.await(5, TimeUnit.SECONDS) );
        } finally {
            source.stopAndWait();
        }
    }

    @Test
    public void discoverLogDeletions() throws Exception {
        // Create a valid Query Change Log topic.
        final String ryaInstance = UUID.randomUUID().toString();
        final String topic = KafkaTopics.queryChangeLogTopic(ryaInstance);
        kafka.createTopic(topic);

        // Create the source.
        final QueryChangeLogSource source = new KafkaQueryChangeLogSource(
                kafka.getKafkaHostname(),
                Integer.parseInt( kafka.getKafkaPort() ),
                Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS));

        // Register a listener that uses latches to indicate when the topic is created and deleted.
        final CountDownLatch created = new CountDownLatch(1);
        final CountDownLatch deleted = new CountDownLatch(1);
        source.subscribe(new SourceListener() {
            @Override
            public void notifyCreate(final String ryaInstanceName, final QueryChangeLog log) {
                assertEquals(ryaInstance, ryaInstanceName);
                created.countDown();
            }

            @Override
            public void notifyDelete(final String ryaInstanceName) {
                assertEquals(ryaInstance, ryaInstanceName);
                deleted.countDown();
            }
        });

        try {
            // Start the source
            source.startAndWait();

            // Wait for it to indicate the topic was created.
            assertTrue( created.await(5, TimeUnit.SECONDS) );

            // Delete the topic.
            kafka.deleteTopic(topic);

            // If the latch isn't counted down, then fail the test.
            assertTrue( deleted.await(5, TimeUnit.SECONDS) );

        } finally {
            source.stopAndWait();
        }
    }

    @Test
    public void newListenerReceivesAllKnownLogs() throws Exception {
        // Create a valid Query Change Log topic.
        final String ryaInstance = UUID.randomUUID().toString();
        final String topic = KafkaTopics.queryChangeLogTopic(ryaInstance);
        kafka.createTopic(topic);

        // Create the source.
        final QueryChangeLogSource source = new KafkaQueryChangeLogSource(
                kafka.getKafkaHostname(),
                Integer.parseInt( kafka.getKafkaPort() ),
                Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS));

        // Register a listener that counts down a latch if it sees the new topic.
        final CountDownLatch created = new CountDownLatch(1);
        source.subscribe(new SourceListener() {
            @Override
            public void notifyCreate(final String ryaInstanceName, final QueryChangeLog log) {
                assertEquals(ryaInstance, ryaInstanceName);
                created.countDown();
            }

            @Override
            public void notifyDelete(final String ryaInstanceName) { }
        });

        try {
            // Start the source
            source.startAndWait();

            // Wait for that first listener to indicate the topic was created. This means that one has been cached.
            assertTrue( created.await(5, TimeUnit.SECONDS) );

            // Register a second listener that counts down when that same topic is encountered. This means the
            // newly subscribed listener was notified with the already known change log.
            final CountDownLatch newListenerCreated = new CountDownLatch(1);
            source.subscribe(new SourceListener() {
                @Override
                public void notifyCreate(final String ryaInstanceName, final QueryChangeLog log) {
                    assertEquals(ryaInstance, ryaInstanceName);
                    newListenerCreated.countDown();
                }

                @Override
                public void notifyDelete(final String ryaInstanceName) { }
            });
            assertTrue( newListenerCreated.await(5, TimeUnit.SECONDS) );

        } finally {
            source.stopAndWait();
        }
    }

    @Test
    public void unsubscribedDoesNotReceiveNotifications() throws Exception {
        // Create the source.
        final QueryChangeLogSource source = new KafkaQueryChangeLogSource(
                kafka.getKafkaHostname(),
                Integer.parseInt( kafka.getKafkaPort() ),
                Scheduler.newFixedRateSchedule(0, 100, TimeUnit.MILLISECONDS));

        try {
            // Start the source.
            source.startAndWait();

            // Create a listener that flips a boolean to true when it is notified.
            final AtomicBoolean notified = new AtomicBoolean(false);
            final SourceListener listener = new SourceListener() {
                @Override
                public void notifyCreate(final String ryaInstanceName, final QueryChangeLog log) {
                    notified.set(true);
                }

                @Override
                public void notifyDelete(final String ryaInstanceName) {
                    notified.set(true);
                }
            };

            // Register and then unregister it.
            source.subscribe(listener);
            source.unsubscribe(listener);

            // Create a topic.
            final String ryaInstance = UUID.randomUUID().toString();
            final String topic = KafkaTopics.queryChangeLogTopic(ryaInstance);
            kafka.createTopic(topic);

            //Wait longer than the polling time for the listener to be notified.
            Thread.sleep(300);

            // Show the boolean was never flipped to true.
            assertFalse(notified.get());
        } finally {
            source.stopAndWait();
        }
    }
}