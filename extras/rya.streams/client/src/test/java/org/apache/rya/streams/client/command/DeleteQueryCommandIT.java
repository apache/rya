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
package org.apache.rya.streams.client.command;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLog;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeDeserializer;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeSerializer;
import org.apache.rya.test.kafka.KafkaTestInstanceRule;
import org.apache.rya.test.kafka.KafkaTestUtil;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration Test for deleting a query from Rya Streams through a command.
 */
public class DeleteQueryCommandIT {

    @Rule
    public KafkaTestInstanceRule kafka = new KafkaTestInstanceRule(true);

    /**
     * This test simulates executing many commands and each of them use their own InMemoryQueryRepository. We need
     * to re-create the repo outside of the command to ensure it has the most up to date values inside of it.
     *
     * @param ryaInstance - The rya instance the repository is connected to. (not null)
     * @param createTopic - Set this to true if the topic doesn't exist yet.
     */
    private QueryRepository makeQueryRepository(final String ryaInstance, final boolean createTopic) {
        requireNonNull(ryaInstance);

        // Make sure the topic that the change log uses exists.
        final String changeLogTopic = KafkaTopics.queryChangeLogTopic("" + ryaInstance);
        if(createTopic) {
            kafka.createTopic(changeLogTopic);
        }

        // Setup the QueryRepository used by the test.
        final Producer<?, QueryChange> queryProducer = KafkaTestUtil.makeProducer(kafka, StringSerializer.class, QueryChangeSerializer.class);
        final Consumer<?, QueryChange>queryConsumer = KafkaTestUtil.fromStartConsumer(kafka, StringDeserializer.class, QueryChangeDeserializer.class);
        final QueryChangeLog changeLog = new KafkaQueryChangeLog(queryProducer, queryConsumer, changeLogTopic);
        return new InMemoryQueryRepository(changeLog);
    }

    @Test
    public void shortParams() throws Exception {
        final String ryaInstance = UUID.randomUUID().toString();

        // Add a few queries to Rya Streams.
        try(QueryRepository repo = makeQueryRepository(ryaInstance, true)) {
            repo.add("query1");
            final UUID query2Id = repo.add("query2").getQueryId();
            repo.add("query3");

            // Show that all three of the queries were added.
            Set<StreamsQuery> queries = repo.list();
            assertEquals(3, queries.size());

            // Delete query 2 using the delete query command.
            final String[] deleteArgs = new String[] {
                    "-r", "" + ryaInstance,
                    "-i", kafka.getKafkaHostname(),
                    "-p", kafka.getKafkaPort(),
                    "-q", query2Id.toString()
            };

            final DeleteQueryCommand deleteCommand = new DeleteQueryCommand();
            deleteCommand.execute(deleteArgs);

            // Show query2 was deleted.
            try(QueryRepository repo2 = makeQueryRepository(ryaInstance, false)) {
                queries = repo2.list();
                assertEquals(2, queries.size());

                for(final StreamsQuery query : queries) {
                    assertNotEquals(query2Id, query.getQueryId());
                }
            }
        }
    }

    @Test
    public void longParams() throws Exception {
        final String ryaInstance = UUID.randomUUID().toString();

        // Add a few queries to Rya Streams.
        try(QueryRepository repo = makeQueryRepository(ryaInstance, true)) {
            repo.add("query1");
            final UUID query2Id = repo.add("query2").getQueryId();
            repo.add("query3");

            // Show that all three of the queries were added.
            Set<StreamsQuery> queries = repo.list();
            assertEquals(3, queries.size());

            // Delete query 2 using the delete query command.
            final String[] deleteArgs = new String[] {
                    "--ryaInstance", "" + ryaInstance,
                    "--kafkaHostname", kafka.getKafkaHostname(),
                    "--kafkaPort", kafka.getKafkaPort(),
                    "--queryID", query2Id.toString()
            };

            final DeleteQueryCommand deleteCommand = new DeleteQueryCommand();
            deleteCommand.execute(deleteArgs);

            // Show query2 was deleted.
            try(QueryRepository repo2 = makeQueryRepository(ryaInstance, false)) {
                queries = repo2.list();
                assertEquals(2, queries.size());

                for(final StreamsQuery query : queries) {
                    assertNotEquals(query2Id, query.getQueryId());
                }
            }
        }
    }
}