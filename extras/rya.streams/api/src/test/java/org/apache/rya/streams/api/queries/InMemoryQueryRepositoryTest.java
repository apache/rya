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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.QueryChangeLog.QueryChangeLogException;
import org.apache.rya.streams.api.queries.QueryRepository.QueryRepositoryException;
import org.junit.Test;

/**
 * Unit tests the methods of {@link InMemoryQueryRepository}.
 */
public class InMemoryQueryRepositoryTest {

    @Test
    public void canReadAddedQueries() throws QueryRepositoryException {
        // Setup a totally in memory QueryRepository.
        final QueryRepository queries = new InMemoryQueryRepository( new InMemoryQueryChangeLog() );

        // Add some queries to it.
        final Set<StreamsQuery> expected = new HashSet<>();
        expected.add( queries.add("query 1") );
        expected.add( queries.add("query 2") );
        expected.add( queries.add("query 3") );

        // Show they are in the list of all queries.
        final Set<StreamsQuery> stored = queries.list();
        assertEquals(expected, stored);
    }

    @Test
    public void deletedQueriesDisappear() throws QueryRepositoryException {
        // Setup a totally in memory QueryRepository.
        final QueryRepository queries = new InMemoryQueryRepository( new InMemoryQueryChangeLog() );

        // Add some queries to it. The second one we will delete.
        final Set<StreamsQuery> expected = new HashSet<>();
        expected.add( queries.add("query 1") );
        final UUID deletedMeId = queries.add("query 2").getQueryId();
        expected.add( queries.add("query 3") );

        // Delete the second query.
        queries.delete( deletedMeId );

        // Show only queries 1 and 3 are in the list.
        final Set<StreamsQuery> stored = queries.list();
        assertEquals(expected, stored);
    }

    @Test
    public void initializedWithPopulatedChnageLog() throws QueryRepositoryException {
        // Setup a totally in memory QueryRepository. Hold onto the change log so that we can use it again later.
        final QueryChangeLog changeLog = new InMemoryQueryChangeLog();
        final QueryRepository queries = new InMemoryQueryRepository( changeLog );

        // Add some queries and deletes to it.
        final Set<StreamsQuery> expected = new HashSet<>();
        expected.add( queries.add("query 1") );
        final UUID deletedMeId = queries.add("query 2").getQueryId();
        expected.add( queries.add("query 3") );
        queries.delete( deletedMeId );

        // Create a new totally in memory QueryRepository.
        final QueryRepository initializedQueries = new InMemoryQueryRepository( changeLog );

        // Listing the queries should work using an initialized change log.
        final Set<StreamsQuery> stored = initializedQueries.list();
        assertEquals(expected, stored);
    }

    @Test(expected = RuntimeException.class)
    public void changeLogThrowsExceptions() throws QueryChangeLogException, QueryRepositoryException {
        // Create a mock change log that throws an exception when you try to list what is in it.
        final QueryChangeLog changeLog = mock(QueryChangeLog.class);
        when(changeLog.readFromStart()).thenThrow(new QueryChangeLogException("Mocked exception."));

        // Create the QueryRepository and invoke one of the methods.
        final QueryRepository queries = new InMemoryQueryRepository( changeLog );
        queries.list();
    }
}