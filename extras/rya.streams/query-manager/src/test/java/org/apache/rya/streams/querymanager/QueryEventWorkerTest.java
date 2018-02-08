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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.querymanager.QueryManager.QueryEvent;
import org.apache.rya.streams.querymanager.QueryManager.QueryEventWorker;
import org.junit.Test;

/**
 * Unit tests the methods of {@link QueryManager.QueryEventWorker}.
 */
public class QueryEventWorkerTest {

    @Test
    public void shutdownSignalKillsThread() {
        // The signal that will kill the working thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The thread that will perform the QueryEventWorker task.
        final Thread queryEventWorker = new Thread(new QueryEventWorker(new ArrayBlockingQueue<>(1),
                mock(QueryExecutor.class), 50, TimeUnit.MILLISECONDS, shutdownSignal));
        queryEventWorker.start();

        // Wait longer than the poll time to see if the thread died. Show that it is still running.
        assertTrue(ThreadUtil.stillAlive(queryEventWorker, 200));

        // Set the shutdown signal to true and join the thread. If we were able to join, then it shut down.
        shutdownSignal.set(true);
        assertFalse(ThreadUtil.stillAlive(queryEventWorker, 1000));
    }

    @Test
    public void executingWork() throws Exception {
        // The signal that will kill the working thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue used to send the execute work to the thread.
        final BlockingQueue<QueryEvent> queue = new ArrayBlockingQueue<>(1);

        // The message that indicates a query needs to be executed.
        final String ryaInstance = "rya";
        final StreamsQuery query = new StreamsQuery(UUID.randomUUID(), "sparql", true, false);
        final QueryEvent executingEvent = QueryEvent.executing(ryaInstance, query);

        // Release a latch if the startQuery method on the queryExecutor is invoked with the correct values.
        final CountDownLatch startQueryInvoked = new CountDownLatch(1);
        final QueryExecutor queryExecutor = mock(QueryExecutor.class);
        doAnswer(invocation -> {
            startQueryInvoked.countDown();
            return null;
        }).when(queryExecutor).startQuery(ryaInstance, query);

        // The thread that will perform the QueryEventWorker task.
        final Thread queryEventWorker = new Thread(new QueryEventWorker(queue,
                queryExecutor, 50, TimeUnit.MILLISECONDS, shutdownSignal));
        try {
            queryEventWorker.start();

            // Provide a message indicating a query needs to be executing.
            queue.put(executingEvent);

            // Verify the Query Executor was told to start the query.
            assertTrue( startQueryInvoked.await(150, TimeUnit.MILLISECONDS) );
        } finally {
            shutdownSignal.set(true);
            queryEventWorker.join();
        }
    }

    @Test
    public void stoppedWork() throws Exception {
        // The signal that will kill the working thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue used to send the execute work to the thread.
        final BlockingQueue<QueryEvent> queue = new ArrayBlockingQueue<>(1);

        // The message that indicates a query needs to be stopped.
        final UUID queryId = UUID.randomUUID();
        final QueryEvent stoppedEvent = QueryEvent.stopped("rya", queryId);

        // Release a latch if the stopQuery method on the queryExecutor is invoked with the correct values.
        final CountDownLatch stopQueryInvoked = new CountDownLatch(1);
        final QueryExecutor queryExecutor = mock(QueryExecutor.class);
        doAnswer(invocation -> {
            stopQueryInvoked.countDown();
            return null;
        }).when(queryExecutor).stopQuery(queryId);

        final Thread queryEventWorker = new Thread(new QueryEventWorker(queue,
                queryExecutor, 50, TimeUnit.MILLISECONDS, shutdownSignal));
        try {
            // The thread that will perform the QueryEventWorker task.
            queryEventWorker.start();

            // Provide a message indicating a query needs to be executing.
            queue.put(stoppedEvent);

            // Verify the Query Executor was told to stop the query.
            assertTrue( stopQueryInvoked.await(150, TimeUnit.MILLISECONDS) );
        } finally {
            shutdownSignal.set(true);
            queryEventWorker.join();
        }
    }

    @Test
    public void stopAllWork() throws Exception {
        // The signal that will kill the working thread.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

        // The queue used to send the execute work to the thread.
        final BlockingQueue<QueryEvent> queue = new ArrayBlockingQueue<>(1);

        // The message that indicates all queries for a rya instance need to be stopped.
        final String ryaInstance = "rya";
        final QueryEvent stopAllEvent = QueryEvent.stopALL(ryaInstance);

        // Release a latch if the stopQuery method on the queryExecutor is invoked with the correct values.
        final CountDownLatch testMethodInvoked = new CountDownLatch(1);
        final QueryExecutor queryExecutor = mock(QueryExecutor.class);
        doAnswer(invocation -> {
            testMethodInvoked.countDown();
            return null;
        }).when(queryExecutor).stopAll(ryaInstance);

        final Thread queryEventWorker = new Thread(new QueryEventWorker(queue,
                queryExecutor, 50, TimeUnit.MILLISECONDS, shutdownSignal));
        try {
            // The thread that will perform the QueryEventWorker task.
            queryEventWorker.start();

            // Provide a message indicating a query needs to be executing.
            queue.put(stopAllEvent);

            // Verify the Query Executor was told to stop all the queries.
            assertTrue( testMethodInvoked.await(150, TimeUnit.MILLISECONDS) );
        } finally {
            shutdownSignal.set(true);
            queryEventWorker.join();
        }
    }
}