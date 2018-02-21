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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryChangeLog;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.querymanager.QueryManager.LogEvent;
import org.apache.rya.streams.querymanager.QueryManager.LogEventWorker;
import org.apache.rya.streams.querymanager.QueryManager.QueryEvent;
import org.junit.Test;

/**
 * Unit tests the methods of {@link LogEventWorker}.
 */
public class LogEventWorkerTest {

    @Test
    public void shutdownSignalKillsThread() {
        // The signal that will kill the working thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The thread that will perform the LogEventWorker task.
        final Thread logEventWorker = new Thread(new LogEventWorker(new ArrayBlockingQueue<>(1),
                new ArrayBlockingQueue<>(1), 50, TimeUnit.MILLISECONDS, shutdownSignal));
        logEventWorker.start();

        // Wait longer than the poll time to see if the thread died. Show that it is still running.
        assertTrue(ThreadUtil.stillAlive(logEventWorker, 200));

        // Set the shutdown signal to true and join the thread. If we were able to join, then it shut down.
        shutdownSignal.set(true);
        assertFalse(ThreadUtil.stillAlive(logEventWorker, 500));
    }

    @Test
    public void nofity_logCreated_doesNotExist() throws Exception {
        // The signal that will kill the working thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue used to feed work.
        final BlockingQueue<LogEvent> logEventQueue = new ArrayBlockingQueue<>(10);

        // The queue work is written to.
        final BlockingQueue<QueryEvent> queryEventQueue = new ArrayBlockingQueue<>(10);

        // The Query Change Log that will be watched.
        final QueryChangeLog changeLog = new InMemoryQueryChangeLog();

        // Write a message that indicates a new query should be active.
        final UUID firstQueryId = UUID.randomUUID();
        changeLog.write(QueryChange.create(firstQueryId, "select * where { ?a ?b ?c . }", true));

        // Write a message that adds an active query, but then makes it inactive. Because both of these
        // events are written to the log before the worker subscribes to the repository for updates, they
        // must result in a single query stopped event.
        final UUID secondQueryId = UUID.randomUUID();
        changeLog.write(QueryChange.create(secondQueryId, "select * where { ?d ?e ?f . }", true));
        changeLog.write(QueryChange.update(secondQueryId, false));

        // Start the worker that will be tested.
        final Thread logEventWorker = new Thread(new LogEventWorker(logEventQueue,
                queryEventQueue, 50, TimeUnit.MILLISECONDS, shutdownSignal));
        logEventWorker.start();

        try {
            // Write a unit of work that indicates a log was created.
            final LogEvent createLogEvent = LogEvent.create("rya", changeLog);
            logEventQueue.offer(createLogEvent);

            // We must see the following Query Events added to the work queue.
            // Query 1, executing.
            // Query 2, stopped.
            Set<QueryEvent> expectedEvents = new HashSet<>();
            expectedEvents.add(QueryEvent.executing("rya",
                    new StreamsQuery(firstQueryId, "select * where { ?a ?b ?c . }", true)));
            expectedEvents.add(QueryEvent.stopped("rya", secondQueryId));

            Set<QueryEvent> queryEvents = new HashSet<>();
            queryEvents.add( queryEventQueue.poll(500, TimeUnit.MILLISECONDS) );
            queryEvents.add( queryEventQueue.poll(500, TimeUnit.MILLISECONDS) );

            assertEquals(expectedEvents, queryEvents);

            // Write an event to the change log that stops the first query.
            changeLog.write(QueryChange.update(firstQueryId, false));

            // Show it was also reflected in the changes.
            // Query 1, stopped.
            expectedEvents = new HashSet<>();
            expectedEvents.add(QueryEvent.stopped("rya", firstQueryId));

            queryEvents = new HashSet<>();
            queryEvents.add( queryEventQueue.poll(500, TimeUnit.MILLISECONDS) );

            assertEquals(expectedEvents, queryEvents);
        } finally {
            shutdownSignal.set(true);
            logEventWorker.join();
        }
    }

    @Test
    public void nofity_logCreated_exists() throws Exception {
        // The signal that will kill the working thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue used to feed work.
        final BlockingQueue<LogEvent> logEventQueue = new ArrayBlockingQueue<>(10);

        // The queue work is written to.
        final BlockingQueue<QueryEvent> queryEventQueue = new ArrayBlockingQueue<>(10);

        // The Query Change Log that will be watched.
        final QueryChangeLog changeLog = new InMemoryQueryChangeLog();

        // Write a message that indicates a new query should be active.
        final UUID firstQueryId = UUID.randomUUID();
        changeLog.write(QueryChange.create(firstQueryId, "select * where { ?a ?b ?c . }", true));

        // Start the worker that will be tested.
        final Thread logEventWorker = new Thread(new LogEventWorker(logEventQueue,
                queryEventQueue, 50, TimeUnit.MILLISECONDS, shutdownSignal));
        logEventWorker.start();

        try {
            // Write a unit of work that indicates a log was created.
            final LogEvent createLogEvent = LogEvent.create("rya", changeLog);
            logEventQueue.offer(createLogEvent);

            // Say the same log was created a second time.
            logEventQueue.offer(createLogEvent);

            // Show that only a single unit of work was added for the log. This indicates the
            // second message was effectively skipped as it would have add its work added twice otherwise.
            final Set<QueryEvent> expectedEvents = new HashSet<>();
            expectedEvents.add(QueryEvent.executing("rya",
                    new StreamsQuery(firstQueryId, "select * where { ?a ?b ?c . }", true)));

            final Set<QueryEvent> queryEvents = new HashSet<>();
            queryEvents.add( queryEventQueue.poll(500, TimeUnit.MILLISECONDS) );

            assertNull(queryEventQueue.poll(500, TimeUnit.MILLISECONDS));
            assertEquals(expectedEvents, queryEvents);
        } finally {
            shutdownSignal.set(true);
            logEventWorker.join();
        }
    }

    @Test
    public void notify_logDeleted_exists() throws Exception {
        // The signal that will kill the working thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue used to feed work.
        final BlockingQueue<LogEvent> logEventQueue = new ArrayBlockingQueue<>(10);

        // The queue work is written to.
        final BlockingQueue<QueryEvent> queryEventQueue = new ArrayBlockingQueue<>(10);

        // The Query Change Log that will be watched.
        final QueryChangeLog changeLog = new InMemoryQueryChangeLog();

        // Start the worker that will be tested.
        final Thread logEventWorker = new Thread(new LogEventWorker(logEventQueue,
                queryEventQueue, 50, TimeUnit.MILLISECONDS, shutdownSignal));
        logEventWorker.start();

        try {
            // Write a unit of work that indicates a log was created.
            final LogEvent createLogEvent = LogEvent.create("rya", changeLog);
            logEventQueue.offer(createLogEvent);

            // Write a unit of work that indicates a log was deleted.
            logEventQueue.offer(LogEvent.delete("rya"));

            // Show that a single unit of work was created for deleting everything for "rya".
            assertEquals(QueryEvent.stopALL("rya"),  queryEventQueue.poll(500, TimeUnit.MILLISECONDS));
            assertNull(queryEventQueue.poll(500, TimeUnit.MILLISECONDS));
        } finally {
            shutdownSignal.set(true);
            logEventWorker.join();
        }
    }

    @Test
    public void notify_logDeleted_doesNotExist() throws Exception {
        // The signal that will kill the working thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue used to feed work.
        final BlockingQueue<LogEvent> logEventQueue = new ArrayBlockingQueue<>(10);

        // The queue work is written to.
        final BlockingQueue<QueryEvent> queryEventQueue = new ArrayBlockingQueue<>(10);

        // Start the worker that will be tested.
        final Thread logEventWorker = new Thread(new LogEventWorker(logEventQueue,
                queryEventQueue, 50, TimeUnit.MILLISECONDS, shutdownSignal));
        logEventWorker.start();

        try {
            // Write a unit of work that indicates a log was deleted. Since it was never created,
            // this will not cause anything to be written to the QueryEvent queue.
            logEventQueue.offer(LogEvent.delete("rya"));

            // Show that a single unit of work was created for deleting everything for "rya".
            assertNull(queryEventQueue.poll(500, TimeUnit.MILLISECONDS));
        } finally {
            shutdownSignal.set(true);
            logEventWorker.join();
        }
    }
}