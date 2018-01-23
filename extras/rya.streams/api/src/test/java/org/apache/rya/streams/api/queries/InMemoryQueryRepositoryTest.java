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
package org.apache.rya.streams.api.queries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.QueryChangeLog.QueryChangeLogException;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

/**
 * Unit tests the methods of {@link InMemoryQueryRepository}.
 */
public class InMemoryQueryRepositoryTest {
    private static final Scheduler SCHEDULE = Scheduler.newFixedRateSchedule(0L, 100, TimeUnit.MILLISECONDS);

    @Test
    public void canReadAddedQueries() throws Exception {
        // Setup a totally in memory QueryRepository.
        final QueryRepository queries = new InMemoryQueryRepository( new InMemoryQueryChangeLog(), SCHEDULE );
        // Add some queries to it.
        final Set<StreamsQuery> expected = new HashSet<>();
        expected.add( queries.add("query 1", true) );
        expected.add( queries.add("query 2", false) );
        expected.add( queries.add("query 3", true) );

        // Show they are in the list of all queries.
        final Set<StreamsQuery> stored = queries.list();
        assertEquals(expected, stored);
    }

    @Test
    public void deletedQueriesDisappear() throws Exception {
        // Setup a totally in memory QueryRepository.
        final QueryRepository queries = new InMemoryQueryRepository( new InMemoryQueryChangeLog(), SCHEDULE );
        // Add some queries to it. The second one we will delete.
        final Set<StreamsQuery> expected = new HashSet<>();
        expected.add( queries.add("query 1", true) );
        final UUID deletedMeId = queries.add("query 2", false).getQueryId();
        expected.add( queries.add("query 3", true) );

        // Delete the second query.
        queries.delete( deletedMeId );

        // Show only queries 1 and 3 are in the list.
        final Set<StreamsQuery> stored = queries.list();
        assertEquals(expected, stored);
    }

    @Test
    public void initializedWithPopulatedChangeLog() throws Exception {
        // Setup a totally in memory QueryRepository. Hold onto the change log so that we can use it again later.
        final QueryChangeLog changeLog = new InMemoryQueryChangeLog();
        final QueryRepository queries = new InMemoryQueryRepository( changeLog, SCHEDULE );
        try {
            queries.startAndWait();
            // Add some queries and deletes to it.
            final Set<StreamsQuery> expected = new HashSet<>();
            expected.add( queries.add("query 1", true) );
            final UUID deletedMeId = queries.add("query 2", false).getQueryId();
            expected.add( queries.add("query 3", true) );
            queries.delete( deletedMeId );

            // Create a new totally in memory QueryRepository.
            final QueryRepository initializedQueries = new InMemoryQueryRepository( changeLog, SCHEDULE );
            try {
                // Listing the queries should work using an initialized change log.
                final Set<StreamsQuery> stored = initializedQueries.list();
                assertEquals(expected, stored);
            } finally {
                queries.stop();
            }
        } finally {
            queries.stop();
        }
    }

    @Test(expected = RuntimeException.class)
    public void changeLogThrowsExceptions() throws Exception {
        // Create a mock change log that throws an exception when you try to list what is in it.
        final QueryChangeLog changeLog = mock(QueryChangeLog.class);
        when(changeLog.readFromStart()).thenThrow(new QueryChangeLogException("Mocked exception."));

        // Create the QueryRepository and invoke one of the methods.
        final QueryRepository queries = new InMemoryQueryRepository( changeLog, SCHEDULE );
        queries.list();
    }

    @Test
    public void get_present() throws Exception {
        // Setup a totally in memory QueryRepository.
        final QueryRepository queries = new InMemoryQueryRepository( new InMemoryQueryChangeLog(), SCHEDULE );
        // Add a query to it.
        final StreamsQuery query = queries.add("query 1", true);

        // Show the fetched query matches the expected ones.
        final Optional<StreamsQuery> fetched = queries.get(query.getQueryId());
        assertEquals(query, fetched.get());
    }

    @Test
    public void get_notPresent() throws Exception {
        // Setup a totally in memory QueryRepository.
        final QueryRepository queries = new InMemoryQueryRepository( new InMemoryQueryChangeLog(), SCHEDULE );
        // Fetch a query that was never added to the repository.
        final Optional<StreamsQuery> query = queries.get(UUID.randomUUID());

        // Show it could not be found.
        assertFalse(query.isPresent());
    }

    @Test
    public void update() throws Exception {
        // Setup a totally in memory QueryRepository.
        final QueryRepository queries = new InMemoryQueryRepository( new InMemoryQueryChangeLog(), SCHEDULE );
        // Add a query to it.
        final StreamsQuery query = queries.add("query 1", true);

        // Change the isActive state of that query.
        queries.updateIsActive(query.getQueryId(), false);

        // Show the fetched query matches the expected one.
        final Optional<StreamsQuery> fetched = queries.get(query.getQueryId());
        final StreamsQuery expected = new StreamsQuery(query.getQueryId(), query.getSparql(), false);
        assertEquals(expected, fetched.get());
    }

    @Test
    public void updateListenerNotify() throws Exception {
        // Setup a totally in memory QueryRepository.
        final QueryRepository queries = new InMemoryQueryRepository( new InMemoryQueryChangeLog(), SCHEDULE );
        try {
            queries.startAndWait();

            // Add a query to it.
            final StreamsQuery query = queries.add("query 1", true);

            final Set<StreamsQuery> existing = queries.subscribe(new QueryChangeLogListener() {
                @Override
                public void notify(final ChangeLogEntry<QueryChange> queryChangeEvent) {
                    final ChangeLogEntry<QueryChange> expected = new ChangeLogEntry<QueryChange>(1L,
                            QueryChange.create(queryChangeEvent.getEntry().getQueryId(), "query 2", true));
                    assertEquals(expected, queryChangeEvent);
                }
            });

            assertEquals(Sets.newHashSet(query), existing);

            queries.add("query 2", true);
        } finally {
            queries.stop();
        }
    }

    @Test
    public void updateListenerNotify_multiClient() throws Exception {
        // Setup a totally in memory QueryRepository.
        final QueryChangeLog changeLog = new InMemoryQueryChangeLog();
        final QueryRepository queries = new InMemoryQueryRepository( changeLog, SCHEDULE );
        final QueryRepository queries2 = new InMemoryQueryRepository( changeLog, SCHEDULE );

        try {
            queries.startAndWait();
            queries2.startAndWait();

            //show listener on repo that query was added to is being notified of the new query.
            final CountDownLatch repo1Latch = new CountDownLatch(1);
            queries.subscribe(new QueryChangeLogListener() {
                @Override
                public void notify(final ChangeLogEntry<QueryChange> queryChangeEvent) {
                    final ChangeLogEntry<QueryChange> expected = new ChangeLogEntry<QueryChange>(0L,
                            QueryChange.create(queryChangeEvent.getEntry().getQueryId(), "query 2", true));
                    assertEquals(expected, queryChangeEvent);
                    repo1Latch.countDown();
                }
            });

            //show listener not on the repo that query was added to is being notified as well.
            final CountDownLatch repo2Latch = new CountDownLatch(1);
            queries2.subscribe(new QueryChangeLogListener() {
                @Override
                public void notify(final ChangeLogEntry<QueryChange> queryChangeEvent) {
                    final ChangeLogEntry<QueryChange> expected = new ChangeLogEntry<QueryChange>(0L,
                            QueryChange.create(queryChangeEvent.getEntry().getQueryId(), "query 2", true));
                    assertEquals(expected, queryChangeEvent);
                    repo2Latch.countDown();
                }
            });

            queries.add("query 2", true);

            assertTrue(repo1Latch.await(5, TimeUnit.SECONDS));
            assertTrue(repo2Latch.await(5, TimeUnit.SECONDS));
        } catch(final InterruptedException e ) {
            System.out.println("PING");
        } finally {
            queries.stop();
            queries2.stop();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void subscribe_notStarted() throws Exception {
        // Setup a totally in memory QueryRepository.
        final QueryRepository queries = new InMemoryQueryRepository(new InMemoryQueryChangeLog(), SCHEDULE);
        queries.subscribe(new QueryChangeLogListener() {
            @Override
            public void notify(final ChangeLogEntry<QueryChange> queryChangeEvent) {}
        });

        queries.add("query 2", true);
    }
}