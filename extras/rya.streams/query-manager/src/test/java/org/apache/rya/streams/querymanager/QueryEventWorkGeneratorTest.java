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

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.ChangeLogEntry;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.querymanager.QueryManager.QueryEvent;
import org.apache.rya.streams.querymanager.QueryManager.QueryEventWorkGenerator;
import org.junit.Test;

/**
 * Unit tests the methods of {@link QueryEventWorkGenerator}.
 */
public class QueryEventWorkGeneratorTest {

    @Test
    public void shutdownSignalKillsThread() throws Exception {
        // The signal that will kill the notifying thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue generated work is offered to.
        final BlockingQueue<QueryEvent> queue = new ArrayBlockingQueue<>(1);

        // The listener that will perform the QueryEventWorkGenerator work.
        final QueryEventWorkGenerator generator =
                new QueryEventWorkGenerator("rya", new CountDownLatch(1), queue, 50, TimeUnit.MILLISECONDS, shutdownSignal);

        // A thread that will attempt to notify the generator with a created query.
        final Thread notifyThread = new Thread(() -> {
            generator.notify(mock(ChangeLogEntry.class), Optional.empty());
        });

        // Fill the queue so that nothing may be offered to it.
        queue.offer(QueryEvent.stopALL("rya"));

        // Start the thread and show that it is still alive after the offer period.
        notifyThread.start();
        assertTrue( ThreadUtil.stillAlive(notifyThread, 200) );

        // Set the shutdown signal to true and join the thread. If we were able to join, then it shut down.
        shutdownSignal.set(true);
        assertFalse( ThreadUtil.stillAlive(notifyThread, 1000) );
    }

    @Test
    public void waitsForSubscriptionWork() throws Exception {
        // The signal that will kill the notifying thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue generated work is offered to.
        final BlockingQueue<QueryEvent> queue = new ArrayBlockingQueue<>(1);

        // The listener that will perform the QueryEventWorkGenerator work.
        final CountDownLatch latch = new CountDownLatch(1);
        final QueryEventWorkGenerator generator =
                new QueryEventWorkGenerator("rya", latch, queue, 50, TimeUnit.MILLISECONDS, shutdownSignal);

        // A thread that will attempt to notify the generator with a created query.
        final UUID queryId = UUID.randomUUID();
        final StreamsQuery query = new StreamsQuery(queryId, "query", true, false);
        final Thread notifyThread = new Thread(() -> {
            final QueryChange change = QueryChange.create(queryId, query.getSparql(), query.isActive(), query.isInsert());
            final ChangeLogEntry<QueryChange> entry = new ChangeLogEntry<>(0, change);
            generator.notify(entry, Optional.of(query));
        });

        // Start the thread.
        notifyThread.start();

        try {
            // Wait longer than the blocking period and show the thread is still alive and nothing has been added
            // to the work queue.
            Thread.sleep(150);
            assertTrue( notifyThread.isAlive() );

            // Count down the latch.
            latch.countDown();

            // Show work was added to the queue and the notifying thread died.
            final QueryEvent event = queue.poll(500, TimeUnit.MILLISECONDS);
            final QueryEvent expected = QueryEvent.executing("rya", new StreamsQuery(queryId, query.getSparql(), query.isActive(), query.isInsert()));
            assertEquals(expected, event);
        } finally {
            shutdownSignal.set(true);
            notifyThread.join();
        }
    }

    @Test
    public void notifyCreate() throws Exception {
        // The signal that will kill the notifying thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue generated work is offered to.
        final BlockingQueue<QueryEvent> queue = new ArrayBlockingQueue<>(1);

        // The listener that will perform the QueryEventWorkGenerator work.
        final CountDownLatch latch = new CountDownLatch(1);
        latch.countDown();
        final QueryEventWorkGenerator generator =
                new QueryEventWorkGenerator("rya", latch, queue, 50, TimeUnit.MILLISECONDS, shutdownSignal);

        // A thread that will attempt to notify the generator with a created query.
        final UUID queryId = UUID.randomUUID();
        final StreamsQuery query = new StreamsQuery(queryId, "query", true, false);
        final Thread notifyThread = new Thread(() -> {
            final QueryChange change = QueryChange.create(queryId, query.getSparql(), query.isActive(), query.isInsert());
            final ChangeLogEntry<QueryChange> entry = new ChangeLogEntry<>(0, change);
            generator.notify(entry, Optional.of(query));
        });

        // Start the thread.
        notifyThread.start();

        try {
            // Show work was added to the queue and the notifying thread died.
            final QueryEvent event = queue.poll(500, TimeUnit.MILLISECONDS);
            final QueryEvent expected = QueryEvent.executing("rya", new StreamsQuery(queryId, query.getSparql(), query.isActive(), query.isInsert()));
            assertEquals(expected, event);
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
        final BlockingQueue<QueryEvent> queue = new ArrayBlockingQueue<>(1);

        // The listener that will perform the QueryEventWorkGenerator work.
        final CountDownLatch latch = new CountDownLatch(1);
        latch.countDown();
        final QueryEventWorkGenerator generator =
                new QueryEventWorkGenerator("rya", latch, queue, 50, TimeUnit.MILLISECONDS, shutdownSignal);

        // A thread that will attempt to notify the generator with a deleted query.
        final UUID queryId = UUID.randomUUID();
        final Thread notifyThread = new Thread(() -> {
            final QueryChange change = QueryChange.delete(queryId);
            final ChangeLogEntry<QueryChange> entry = new ChangeLogEntry<>(0, change);
            generator.notify(entry, Optional.empty());
        });

        // Start the thread.
        notifyThread.start();

        try {
            // Show work was added to the queue and the notifying thread died.
            final QueryEvent event = queue.poll(500, TimeUnit.MILLISECONDS);
            final QueryEvent expected = QueryEvent.stopped("rya", queryId);
            assertEquals(expected, event);
        } finally {
            shutdownSignal.set(true);
            notifyThread.join();
        }
    }

    @Test
    public void notifyUpdate_isActive() throws Exception {
        // The signal that will kill the notifying thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue generated work is offered to.
        final BlockingQueue<QueryEvent> queue = new ArrayBlockingQueue<>(1);

        // The listener that will perform the QueryEventWorkGenerator work.
        final CountDownLatch latch = new CountDownLatch(1);
        latch.countDown();
        final QueryEventWorkGenerator generator =
                new QueryEventWorkGenerator("rya", latch, queue, 50, TimeUnit.MILLISECONDS, shutdownSignal);

        // A thread that will attempt to notify the generator with an update query change.
        final UUID queryId = UUID.randomUUID();
        final StreamsQuery query = new StreamsQuery(queryId, "query", true, false);
        final Thread notifyThread = new Thread(() -> {
            final QueryChange change = QueryChange.update(queryId, true);
            final ChangeLogEntry<QueryChange> entry = new ChangeLogEntry<>(0, change);
            generator.notify(entry, Optional.of(query));
        });

        // Start the thread.
        notifyThread.start();

        try {
            // Show work was added to the queue and the notifying thread died.
            final QueryEvent event = queue.poll(500, TimeUnit.MILLISECONDS);
            final QueryEvent expected = QueryEvent.executing("rya", new StreamsQuery(queryId, query.getSparql(), query.isActive(), query.isInsert()));
            assertEquals(expected, event);
        } finally {
            shutdownSignal.set(true);
            notifyThread.join();
        }
    }

    @Test
    public void notifyUpdate_isNotActive() throws Exception {
        // The signal that will kill the notifying thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue generated work is offered to.
        final BlockingQueue<QueryEvent> queue = new ArrayBlockingQueue<>(1);

        // The listener that will perform the QueryEventWorkGenerator work.
        final CountDownLatch latch = new CountDownLatch(1);
        latch.countDown();
        final QueryEventWorkGenerator generator =
                new QueryEventWorkGenerator("rya", latch, queue, 50, TimeUnit.MILLISECONDS, shutdownSignal);

        // A thread that will attempt to notify the generator with an update query change.
        final UUID queryId = UUID.randomUUID();
        final StreamsQuery query = new StreamsQuery(queryId, "query", false, false);
        final Thread notifyThread = new Thread(() -> {
            final QueryChange change = QueryChange.update(queryId, false);
            final ChangeLogEntry<QueryChange> entry = new ChangeLogEntry<>(0, change);
            generator.notify(entry, Optional.of(query));
        });

        // Start the thread.
        notifyThread.start();

        try {
            // Show work was added to the queue and the notifying thread died.
            final QueryEvent event = queue.poll(500, TimeUnit.MILLISECONDS);
            final QueryEvent expected = QueryEvent.stopped("rya", queryId);
            assertEquals(expected, event);
        } finally {
            shutdownSignal.set(true);
            notifyThread.join();
        }
    }
}