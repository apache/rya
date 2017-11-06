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
package org.apache.rya.streams.kafka.interactor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.api.entity.QueryResultStream;
import org.apache.rya.streams.api.interactor.GetQueryResultStream;
import org.apache.rya.streams.kafka.KafkaTestUtil;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetSerializer;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.impl.MapBindingSet;

/**
 * Integration tests the methods of {@link KafkaGetQueryResultStream}.
 */
public class KafkaGetQueryResultStreamIT {

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    /**
     * Polls a {@link QueryResultStream} until it has either polled too many times without hitting
     * the target number of results, or it hits the target number of results.
     *
     * @param pollMs - How long each poll could take.
     * @param pollIterations - The maximum nubmer of polls that will be attempted.
     * @param targetSize - The number of results to read before stopping.
     * @param stream - The stream that will be polled.
     * @return The results that were read from the stream.
     * @throws Exception If the poll failed.
     */
    private List<VisibilityBindingSet> pollForResults(
            final int pollMs,
            final int pollIterations,
            final int targetSize,
            final QueryResultStream stream)  throws Exception{
        final List<VisibilityBindingSet> read = new ArrayList<>();

        int i = 0;
        while(read.size() < targetSize && i < pollIterations) {
            for(final VisibilityBindingSet visBs : stream.poll(pollMs)) {
                read.add( visBs );
            }
            i++;
        }

        return read;
    }

    @Test
    public void fromStart() throws Exception {
        // Create an ID for the query.
        final UUID queryId = UUID.randomUUID();

        // Create a list of test VisibilityBindingSets.
        final List<VisibilityBindingSet> original = new ArrayList<>();

        final ValueFactory vf = new ValueFactoryImpl();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("urn:name", vf.createLiteral("Alice"));
        original.add(new VisibilityBindingSet(bs, "a|b|c"));

        bs = new MapBindingSet();
        bs.addBinding("urn:name", vf.createLiteral("Bob"));
        original.add(new VisibilityBindingSet(bs, "a"));

        bs = new MapBindingSet();
        bs.addBinding("urn:name", vf.createLiteral("Charlie"));
        original.add(new VisibilityBindingSet(bs, "b|c"));

        // Write some entries to the query result topic in Kafka.
        try(final Producer<?, VisibilityBindingSet> producer =
                KafkaTestUtil.makeProducer(kafka, StringSerializer.class, VisibilityBindingSetSerializer.class)) {
            final String resultTopic = KafkaTopics.queryResultsTopic(queryId);
            for(final VisibilityBindingSet visBs : original) {
                producer.send(new ProducerRecord<>(resultTopic, visBs));
            }
        }

        // Use the interactor that is being tested to read all of the visibility binding sets.
        final GetQueryResultStream interactor = new KafkaGetQueryResultStream(kafka.getKafkaHostname(), kafka.getKafkaPort());
        final List<VisibilityBindingSet> read = pollForResults(500, 3, 3, interactor.fromStart(queryId));

        // Show the fetched binding sets match the original, as well as their order.
        assertEquals(original, read);
    }

    @Test
    public void fromNow() throws Exception {
        // Create an ID for the query.
        final UUID queryId = UUID.randomUUID();

        try(final Producer<?, VisibilityBindingSet> producer =
                KafkaTestUtil.makeProducer(kafka, StringSerializer.class, VisibilityBindingSetSerializer.class)) {
            final String resultTopic = KafkaTopics.queryResultsTopic(queryId);

            // Write a single visibility binding set to the query's result topic. This will not appear in the expected results.
            final ValueFactory vf = new ValueFactoryImpl();
            MapBindingSet bs = new MapBindingSet();
            bs.addBinding("urn:name", vf.createLiteral("Alice"));
            producer.send(new ProducerRecord<>(resultTopic, new VisibilityBindingSet(bs, "a|b|c")));
            producer.flush();

            // Use the interactor that is being tested to read all of the visibility binding sets that appear after this point.
            final GetQueryResultStream interactor = new KafkaGetQueryResultStream(kafka.getKafkaHostname(), kafka.getKafkaPort());
            try(QueryResultStream results = interactor.fromNow(queryId)) {
                // Read results from the stream.
                List<VisibilityBindingSet> read = new ArrayList<>();
                for(final VisibilityBindingSet visBs : results.poll(500)) {
                    read.add(visBs);
                }

                // Show nothing has been read.
                assertTrue(read.isEmpty());

                // Write two more entries to the result topic. These will be seen by the result stream.
                final List<VisibilityBindingSet> original = new ArrayList<>();

                bs = new MapBindingSet();
                bs.addBinding("urn:name", vf.createLiteral("Bob"));
                original.add(new VisibilityBindingSet(bs, "a"));

                bs = new MapBindingSet();
                bs.addBinding("urn:name", vf.createLiteral("Charlie"));
                original.add(new VisibilityBindingSet(bs, "b|c"));

                for(final VisibilityBindingSet visBs : original) {
                    producer.send(new ProducerRecord<>(resultTopic, visBs));
                }
                producer.flush();

                // Read the results from the result stream.
                read = pollForResults(500, 3, 2, results);

                // Show the new entries were read.
                assertEquals(original, read);
            }
        }
    }

    @Test(expected = IllegalStateException.class)
    public void pollClosedStream() throws Exception {
        // Create an ID for the query.
        final UUID queryId = UUID.randomUUID();

        // Use the interactor that is being tested to create a result stream and immediately close it.
        final GetQueryResultStream interactor = new KafkaGetQueryResultStream(kafka.getKafkaHostname(), kafka.getKafkaPort());
        final QueryResultStream results = interactor.fromStart(queryId);
        results.close();

        // Try to poll the closed stream.
        results.poll(1);
    }
}