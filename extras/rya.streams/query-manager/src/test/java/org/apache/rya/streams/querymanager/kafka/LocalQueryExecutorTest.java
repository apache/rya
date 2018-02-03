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
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.UUID;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.kafka.KafkaStreamsFactory;
import org.apache.rya.streams.kafka.interactor.CreateKafkaTopic;
import org.apache.rya.streams.querymanager.QueryExecutor;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Unit tests the methods of {@link LocalQueryExecutor}.
 */
public class LocalQueryExecutorTest {

    @Test(expected = IllegalStateException.class)
    public void startQuery_serviceNotStarted() throws Exception {
        final QueryExecutor executor = new LocalQueryExecutor(mock(CreateKafkaTopic.class), mock(KafkaStreamsFactory.class));
        executor.startQuery("rya", new StreamsQuery(UUID.randomUUID(), "query", true));
    }

    @Test
    public void startQuery() throws Exception {
        // Test values.
        final String ryaInstance = "rya";
        final StreamsQuery query = new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true);

        // Mock the streams factory so that we can tell if the start function is invoked by the executor.
        final KafkaStreamsFactory jobFactory = mock(KafkaStreamsFactory.class);
        final KafkaStreams queryJob = mock(KafkaStreams.class);
        when(jobFactory.make(eq(ryaInstance), eq(query))).thenReturn(queryJob);

        // Start the executor that will be tested.
        final QueryExecutor executor = new LocalQueryExecutor(mock(CreateKafkaTopic.class), jobFactory);
        executor.startAndWait();
        try {
            // Tell the executor to start the query.
            executor.startQuery(ryaInstance, query);

            // Show a job was started for that query's ID.
            verify(queryJob).start();
        } finally {
            executor.stopAndWait();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void stopQuery_serviceNotStarted() throws Exception {
        final QueryExecutor executor = new LocalQueryExecutor(mock(CreateKafkaTopic.class), mock(KafkaStreamsFactory.class));
        executor.stopQuery(UUID.randomUUID());
    }

    @Test
    public void stopQuery_queryNotRunning() throws Exception {
        // Start an executor.
        final QueryExecutor executor = new LocalQueryExecutor(mock(CreateKafkaTopic.class), mock(KafkaStreamsFactory.class));
        executor.startAndWait();
        try {
            // Try to stop a query that was never stareted.
            executor.stopQuery(UUID.randomUUID());
        } finally {
            executor.stopAndWait();
        }
    }

    @Test
    public void stopQuery() throws Exception {
        // Test values.
        final String ryaInstance = "rya";
        final StreamsQuery query = new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true);

        // Mock the streams factory so that we can tell if the stop function is invoked by the executor.
        final KafkaStreamsFactory jobFactory = mock(KafkaStreamsFactory.class);
        final KafkaStreams queryJob = mock(KafkaStreams.class);
        when(jobFactory.make(eq(ryaInstance), eq(query))).thenReturn(queryJob);

        // Start the executor that will be tested.
        final QueryExecutor executor = new LocalQueryExecutor(mock(CreateKafkaTopic.class), jobFactory);
        executor.startAndWait();
        try {
            // Tell the executor to start the query.
            executor.startQuery(ryaInstance, query);

            // Tell the executor to stop the query.
            executor.stopQuery(query.getQueryId());

            // Show a job was stopped for that query's ID.
            verify(queryJob).close();
        } finally {
            executor.stopAndWait();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void stopAll_serviceNotStarted() throws Exception {
        final QueryExecutor executor = new LocalQueryExecutor(mock(CreateKafkaTopic.class), mock(KafkaStreamsFactory.class));
        executor.stopAll("rya");
    }

    @Test
    public void stopAll_noneForThatRyaInstance() throws Exception {
        // Test values.
        final String ryaInstance = "rya";
        final StreamsQuery query1= new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true);
        final StreamsQuery query2= new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true);

        // Mock the streams factory so that we can tell if the stop function is invoked by the executor.
        final KafkaStreamsFactory jobFactory = mock(KafkaStreamsFactory.class);
        final KafkaStreams queryJob1 = mock(KafkaStreams.class);
        final KafkaStreams queryJob2 = mock(KafkaStreams.class);
        when(jobFactory.make(eq(ryaInstance), eq(query1))).thenReturn(queryJob1);
        when(jobFactory.make(eq(ryaInstance), eq(query2))).thenReturn(queryJob2);

        // Start the executor that will be tested.
        final QueryExecutor executor = new LocalQueryExecutor(mock(CreateKafkaTopic.class), jobFactory);
        executor.startAndWait();
        try {
            // Tell the executor to start the queries.
            executor.startQuery(ryaInstance, query1);
            executor.startQuery(ryaInstance, query2);

            // Verify both are running.
            verify(queryJob1).start();
            verify(queryJob2).start();

            // Tell the executor to stop queries running under rya2.
            executor.stopAll("someOtherRyaInstance");

            // Show none of the queries were stopped.
            verify(queryJob1, never()).close();
            verify(queryJob2, never()).close();

        } finally {
            executor.stopAndWait();
        }
    }

    @Test
    public void stopAll() throws Exception {
        // Test values.
        final String ryaInstance1 = "rya1";
        final StreamsQuery query1= new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true);
        final String ryaInstance2 = "rya2";
        final StreamsQuery query2= new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true);

        // Mock the streams factory so that we can tell if the stop function is invoked by the executor.
        final KafkaStreamsFactory jobFactory = mock(KafkaStreamsFactory.class);
        final KafkaStreams queryJob1 = mock(KafkaStreams.class);
        when(jobFactory.make(eq(ryaInstance1), eq(query1))).thenReturn(queryJob1);
        final KafkaStreams queryJob2 = mock(KafkaStreams.class);
        when(jobFactory.make(eq(ryaInstance2), eq(query2))).thenReturn(queryJob2);

        // Start the executor that will be tested.
        final QueryExecutor executor = new LocalQueryExecutor(mock(CreateKafkaTopic.class), jobFactory);
        executor.startAndWait();
        try {
            // Tell the executor to start the queries.
            executor.startQuery(ryaInstance1, query1);
            executor.startQuery(ryaInstance2, query2);

            // Verify both are running.
            verify(queryJob1).start();
            verify(queryJob2).start();

            // Tell the executor to stop queries running under rya2.
            executor.stopAll(ryaInstance2);

            // Show the first query is still running, but the second isn't.
            verify(queryJob1, never()).close();
            verify(queryJob2).close();

        } finally {
            executor.stopAndWait();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void getRunningQueryIds_serviceNotStarted() throws Exception {
        final QueryExecutor executor = new LocalQueryExecutor(mock(CreateKafkaTopic.class), mock(KafkaStreamsFactory.class));
        executor.getRunningQueryIds();
    }

    @Test
    public void getRunningQueryIds_noneStarted() throws Exception {
        // Start an executor.
        final QueryExecutor executor = new LocalQueryExecutor(mock(CreateKafkaTopic.class), mock(KafkaStreamsFactory.class));
        executor.startAndWait();
        try {
            // Get the list of running queries.
            final Set<UUID> runningQueries = executor.getRunningQueryIds();

            // Show no queries are reported as running.
            assertTrue(runningQueries.isEmpty());
        } finally {
            executor.stopAndWait();
        }
    }

    @Test
    public void getRunningQueryIds_noneStopped() throws Exception {
        // Test values.
        final String ryaInstance = "rya";
        final StreamsQuery query1 = new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true);
        final StreamsQuery query2 = new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true);
        final StreamsQuery query3 = new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true);

        // Mock the streams factory so that we can figure out what is started.
        final KafkaStreamsFactory jobFactory = mock(KafkaStreamsFactory.class);
        when(jobFactory.make(eq(ryaInstance), eq(query1))).thenReturn(mock(KafkaStreams.class));
        when(jobFactory.make(eq(ryaInstance), eq(query2))).thenReturn(mock(KafkaStreams.class));
        when(jobFactory.make(eq(ryaInstance), eq(query3))).thenReturn(mock(KafkaStreams.class));

        // Start the executor that will be tested.
        final QueryExecutor executor = new LocalQueryExecutor(mock(CreateKafkaTopic.class), jobFactory);
        executor.startAndWait();
        try {
            // Start the queries.
            executor.startQuery(ryaInstance, query1);
            executor.startQuery(ryaInstance, query2);
            executor.startQuery(ryaInstance, query3);

            // All of those query IDs should be reported as running.
            final Set<UUID> expected = Sets.newHashSet(
                    query1.getQueryId(),
                    query2.getQueryId(),
                    query3.getQueryId());
            assertEquals(expected, executor.getRunningQueryIds());

        } finally {
            executor.stopAndWait();
        }
    }

    @Test
    public void getRunningQueryIds_stoppedNoLongerListed() throws Exception {
        // Test values.
        final String ryaInstance = "rya";
        final StreamsQuery query1 = new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true);
        final StreamsQuery query2 = new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true);
        final StreamsQuery query3 = new StreamsQuery(UUID.randomUUID(), "SELECT * WHERE { ?a ?b ?c. }", true);

        // Mock the streams factory so that we can figure out what is started.
        final KafkaStreamsFactory jobFactory = mock(KafkaStreamsFactory.class);
        when(jobFactory.make(eq(ryaInstance), eq(query1))).thenReturn(mock(KafkaStreams.class));
        when(jobFactory.make(eq(ryaInstance), eq(query2))).thenReturn(mock(KafkaStreams.class));
        when(jobFactory.make(eq(ryaInstance), eq(query3))).thenReturn(mock(KafkaStreams.class));

        // Start the executor that will be tested.
        final QueryExecutor executor = new LocalQueryExecutor(mock(CreateKafkaTopic.class), jobFactory);
        executor.startAndWait();
        try {
            // Start the queries.
            executor.startQuery(ryaInstance, query1);
            executor.startQuery(ryaInstance, query2);
            executor.startQuery(ryaInstance, query3);

            // Stop the second query.
            executor.stopQuery(query2.getQueryId());

            // Only the first and third queries are running.
            final Set<UUID> expected = Sets.newHashSet(
                    query1.getQueryId(),
                    query3.getQueryId());
            assertEquals(expected, executor.getRunningQueryIds());

        } finally {
            executor.stopAndWait();
        }
    }
}