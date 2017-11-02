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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Integration Test for deleting a query from Rya Streams through a command.
 */
public class DeleteQueryCommandIT {

    private final String ryaInstance = UUID.randomUUID().toString();

    private String kafkaIp;
    private String kafkaPort;

    private Producer<?, QueryChange> queryProducer = null;
    private Consumer<?, QueryChange> queryConsumer = null;

    @Rule
    public KafkaTestInstanceRule rule = new KafkaTestInstanceRule(true);

    @Before
    public void setup() {
        final Properties props = rule.createBootstrapServerConfig();
        final String location = props.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        final String[] tokens = location.split(":");

        kafkaIp = tokens[0];
        kafkaPort = tokens[1];
    }

    /**
     * This test simulates executing many commands and each of them use their own InMemoryQueryRepository. We need
     * to re-create the repo outside of the command to ensure it has the most up to date values inside of it.
     */
    private QueryRepository makeQueryRepository() {
        final Properties producerProperties = new Properties();
        producerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaIp + ":" + kafkaPort);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, QueryChangeSerializer.class.getName());

        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafkaIp + ":" + kafkaPort);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, QueryChangeDeserializer.class.getName());

        cleanup();
        queryProducer = new KafkaProducer<>(producerProperties);
        queryConsumer = new KafkaConsumer<>(consumerProperties);

        final String changeLogTopic = KafkaTopics.queryChangeLogTopic("" + ryaInstance);
        final QueryChangeLog changeLog = new KafkaQueryChangeLog(queryProducer, queryConsumer, changeLogTopic);
        return new InMemoryQueryRepository(changeLog);
    }

    @After
    public void cleanup() {
        if(queryProducer != null) {
            queryProducer.close();
        }
        if(queryConsumer != null) {
            queryConsumer.close();
        }
    }

    @Test
    public void shortParams() throws Exception {
        // Add a few queries to Rya Streams.
        QueryRepository repo = makeQueryRepository();
        repo.add("query1");
        final UUID query2Id = repo.add("query2").getQueryId();
        repo.add("query3");

        // Show that all three of the queries were added.
        Set<StreamsQuery> queries = repo.list();
        assertEquals(3, queries.size());

        // Delete query 2 using the delete query command.
        final String[] deleteArgs = new String[] {
                "-r", "" + ryaInstance,
                "-i", kafkaIp,
                "-p", kafkaPort,
                "-q", query2Id.toString()
        };

        final DeleteQueryCommand deleteCommand = new DeleteQueryCommand();
        deleteCommand.execute(deleteArgs);

        // Show query2 was deleted.
        repo = makeQueryRepository();
        queries = repo.list();
        assertEquals(2, queries.size());

        for(final StreamsQuery query : queries) {
            assertNotEquals(query2Id, query.getQueryId());
        }
    }

    @Test
    public void longParams() throws Exception {
        // Add a few queries to Rya Streams.
        QueryRepository repo = makeQueryRepository();
        repo.add("query1");
        final UUID query2Id = repo.add("query2").getQueryId();
        repo.add("query3");

        // Show that all three of the queries were added.
        Set<StreamsQuery> queries = repo.list();
        assertEquals(3, queries.size());

        // Delete query 2 using the delete query command.
        final String[] deleteArgs = new String[] {
                "--ryaInstance", "" + ryaInstance,
                "--kafkaHostname", kafkaIp,
                "--kafkaPort", kafkaPort,
                "--queryID", query2Id.toString()
        };

        final DeleteQueryCommand deleteCommand = new DeleteQueryCommand();
        deleteCommand.execute(deleteArgs);

        // Show query2 was deleted.
        repo = makeQueryRepository();
        queries = repo.list();
        assertEquals(2, queries.size());

        for(final StreamsQuery query : queries) {
            assertNotEquals(query2Id, query.getQueryId());
        }
    }
}