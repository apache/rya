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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryChangeLog;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.querymanager.QueryChangeLogSource.SourceListener;
import org.junit.Test;

/**
 * Unit tests the methods of {@link QueryManager}.
 */
public class QueryManagerTest {

    /**
     * Tests when the query manager is notified to create a new query, the query
     * is created and started.
     */
    @Test
    public void testCreateQuery() throws Exception {
        //The new QueryChangeLog
        final QueryChangeLog newChangeLog = new InMemoryQueryChangeLog();
        final String ryaInstance = "ryaTestInstance";
        final StreamsQuery query = new StreamsQuery(UUID.randomUUID(), "some query", true, false);

        // when the query executor is told to start the test query on the test
        // rya instance, count down on the countdown latch
        final QueryExecutor qe = mock(QueryExecutor.class);
        when(qe.isRunning()).thenReturn(true);

        final CountDownLatch queryStarted = new CountDownLatch(1);
        doAnswer(invocation -> {
            queryStarted.countDown();
            return null;
        }).when(qe).startQuery(eq(ryaInstance), eq(query));
        final QueryChangeLogSource source = mock(QueryChangeLogSource.class);

        //When the QueryChangeLogSource is subscribed to in the QueryManager, mock notify of a new QueryChangeLog
        doAnswer(invocation -> {
            //The listener created by the Query Manager
            final SourceListener listener = (SourceListener) invocation.getArguments()[0];
            listener.notifyCreate(ryaInstance, newChangeLog);
            newChangeLog.write(QueryChange.create(query.getQueryId(), query.getSparql(), query.isActive(), query.isInsert()));
            return null;
        }).when(source).subscribe(any(SourceListener.class));

        final QueryManager qm = new QueryManager(qe, source, 50, TimeUnit.MILLISECONDS);
        try {
            qm.startAndWait();
            queryStarted.await(5, TimeUnit.SECONDS);
            verify(qe).startQuery(ryaInstance, query);
        } finally {
            qm.stopAndWait();
        }
    }

    /**
     * Tests when the query manager is notified to delete a new query, the query
     * is stopped and deleted.
     */
    @Test
    public void testDeleteQuery() throws Exception {
        //The new QueryChangeLog
        final QueryChangeLog newChangeLog = new InMemoryQueryChangeLog();
        final StreamsQuery query = new StreamsQuery(UUID.randomUUID(), "some query", true, false);
        final String ryaInstance = "ryaTestInstance";

        // when the query executor is told to start the test query on the test
        // rya instance, count down on the countdown latch
        final QueryExecutor qe = mock(QueryExecutor.class);
        when(qe.isRunning()).thenReturn(true);

        final CountDownLatch queryStarted = new CountDownLatch(1);
        final CountDownLatch queryDeleted = new CountDownLatch(1);
        doAnswer(invocation -> {
            queryDeleted.countDown();
            return null;
        }).when(qe).stopQuery(query.getQueryId());
        final QueryChangeLogSource source = mock(QueryChangeLogSource.class);

        // when the query executor is told to start the test query on the test
        // rya instance, count down on the countdown latch
        doAnswer(invocation -> {
            queryStarted.countDown();
            return null;
        }).when(qe).startQuery(eq(ryaInstance), eq(query));

        //When the QueryChangeLogSource is subscribed to in the QueryManager, mock notify of a new QueryChangeLog
        // add the query, so it can be removed
        doAnswer(invocation -> {
            //The listener created by the Query Manager
            final SourceListener listener = (SourceListener) invocation.getArguments()[0];
            listener.notifyCreate(ryaInstance, newChangeLog);
            Thread.sleep(1000);
            newChangeLog.write(QueryChange.create(query.getQueryId(), query.getSparql(), query.isActive(), query.isInsert()));
            queryStarted.await(5, TimeUnit.SECONDS);
            newChangeLog.write(QueryChange.delete(query.getQueryId()));
            return null;
        }).when(source).subscribe(any(SourceListener.class));

        final QueryManager qm = new QueryManager(qe, source, 50, TimeUnit.MILLISECONDS);
        try {
            qm.startAndWait();
            queryDeleted.await(5, TimeUnit.SECONDS);
            verify(qe).stopQuery(query.getQueryId());
        } finally {
            qm.stopAndWait();
        }
    }

    /**
     * Tests when the query manager is notified to update an existing query, the
     * query is stopped.
     */
    @Test
    public void testUpdateQuery() throws Exception {
        // The new QueryChangeLog
        final QueryChangeLog newChangeLog = new InMemoryQueryChangeLog();
        final StreamsQuery query = new StreamsQuery(UUID.randomUUID(), "some query", true, false);
        final String ryaInstance = "ryaTestInstance";

        // when the query executor is told to start the test query on the test
        // rya instance, count down on the countdown latch
        final QueryExecutor qe = mock(QueryExecutor.class);
        when(qe.isRunning()).thenReturn(true);

        final CountDownLatch queryStarted = new CountDownLatch(1);
        final CountDownLatch queryDeleted = new CountDownLatch(1);
        doAnswer(invocation -> {
            queryDeleted.countDown();
            return null;
        }).when(qe).stopQuery(query.getQueryId());
        final QueryChangeLogSource source = mock(QueryChangeLogSource.class);

        // when the query executor is told to start the test query on the test
        // rya instance, count down on the countdown latch
        doAnswer(invocation -> {
            queryStarted.countDown();
            return null;
        }).when(qe).startQuery(eq(ryaInstance), eq(query));

        // When the QueryChangeLogSource is subscribed to in the QueryManager,
        // mock notify of a new QueryChangeLog
        // add the query, so it can be removed
        doAnswer(invocation -> {
            // The listener created by the Query Manager
            final SourceListener listener = (SourceListener) invocation.getArguments()[0];
            listener.notifyCreate(ryaInstance, newChangeLog);
            Thread.sleep(1000);
            newChangeLog.write(QueryChange.create(query.getQueryId(), query.getSparql(), query.isActive(), query.isInsert()));
            queryStarted.await(5, TimeUnit.SECONDS);
            newChangeLog.write(QueryChange.update(query.getQueryId(), false));
            return null;
        }).when(source).subscribe(any(SourceListener.class));

        final QueryManager qm = new QueryManager(qe, source, 50, TimeUnit.MILLISECONDS);
        try {
            qm.startAndWait();
            queryDeleted.await(10, TimeUnit.SECONDS);
            verify(qe).stopQuery(query.getQueryId());
        } finally {
            qm.stopAndWait();
        }
    }
}