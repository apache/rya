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
package org.apache.rya.streams.querymanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.querymanager.QueryManager.LogEvent;
import org.apache.rya.streams.querymanager.QueryManager.LogEvent.LogEventType;
import org.apache.rya.streams.querymanager.QueryManager.LogEventWorkGenerator;
import org.junit.Test;

/**
 * Unit tests the methods of {@link LogEventWorkGenerator}.
 */
public class LogEventWorkGeneratorTest {

    @Test
    public void shutdownSignalKillsThread() {
        // The signal that will kill the notifying thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue generated work is offered to.
        final BlockingQueue<LogEvent> queue = new ArrayBlockingQueue<>(1);

        // The listener that will perform the LogEventWorkGenerator work.
        final LogEventWorkGenerator generator = new LogEventWorkGenerator(queue, 50, TimeUnit.MILLISECONDS, shutdownSignal);

        // A thread that will attempt to notify the generator with a created change log.
        final Thread notifyThread = new Thread(() -> {
            generator.notifyCreate("rya", mock(QueryChangeLog.class));
        });

        // Fill the queue so that nothing may be offered to it.
        queue.offer(LogEvent.delete("rya"));

        // Start the thread and show that it is still alive after the offer period.
        notifyThread.start();
        assertTrue( ThreadUtil.stillAlive(notifyThread, 200) );

        // Set the shutdown signal to true and join the thread. If we were able to join, then it shut down.
        shutdownSignal.set(true);
        assertFalse( ThreadUtil.stillAlive(notifyThread, 1000) );
    }

    @Test
    public void notifyCreate() throws Exception {
        // The signal that will kill the notifying thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue generated work is offered to.
        final BlockingQueue<LogEvent> queue = new ArrayBlockingQueue<>(1);

        // The listener that will perform the LogEventWorkGenerator work.
        final LogEventWorkGenerator generator = new LogEventWorkGenerator(queue, 50, TimeUnit.MILLISECONDS, shutdownSignal);

        // A thread that will attempt to notify the generator with a created change log.
        final CountDownLatch notified = new CountDownLatch(1);
        final Thread notifyThread = new Thread(() -> {
            generator.notifyCreate("rya", mock(QueryChangeLog.class));
            notified.countDown();
        });

        try {
            // Start the thread that performs the notification.
            notifyThread.start();

            // Wait for the thread to indicate it has notified and check the queue for the value.
            notified.await(200, TimeUnit.MILLISECONDS);
            final LogEvent event = queue.poll(200, TimeUnit.MILLISECONDS);
            assertEquals(LogEventType.CREATE, event.getEventType());
            assertEquals("rya", event.getRyaInstanceName());
        } finally {
            shutdownSignal.set(true);
            notifyThread.join();
        }
    }

    @Test
    public void notifyDelete() throws Exception {
        // The signal that will kill the notifying thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue generated work is offered to.
        final BlockingQueue<LogEvent> queue = new ArrayBlockingQueue<>(1);

        // The listener that will perform the LogEventWorkGenerator work.
        final LogEventWorkGenerator generator = new LogEventWorkGenerator(queue, 50, TimeUnit.MILLISECONDS, shutdownSignal);

        // A thread that will attempt to notify the generator with a deleted change log.
        final CountDownLatch notified = new CountDownLatch(1);
        final Thread notifyThread = new Thread(() -> {
            generator.notifyDelete("rya");
            notified.countDown();
        });

        try {
            // Start the thread that performs the notification.
            notifyThread.start();

            // Wait for the thread to indicate it has notified and check the queue for the value.
            notified.await(200, TimeUnit.MILLISECONDS);
            final LogEvent event = queue.poll(200, TimeUnit.MILLISECONDS);
            assertEquals(LogEventType.DELETE, event.getEventType());
            assertEquals("rya", event.getRyaInstanceName());
        } finally {
            shutdownSignal.set(true);
            notifyThread.join();
        }
    }
}