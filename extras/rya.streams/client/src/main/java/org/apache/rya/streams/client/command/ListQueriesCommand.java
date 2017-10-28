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

import java.util.Properties;
import java.util.Set;

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
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.ListQueries;
import org.apache.rya.streams.api.interactor.defaults.DefaultListQueries;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.client.RyaStreamsCommand;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLog;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeDeserializer;
import org.apache.rya.streams.kafka.serialization.queries.QueryChangeSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A command that lists all queries currently in Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public class ListQueriesCommand implements RyaStreamsCommand {
    private static final Logger log = LoggerFactory.getLogger(ListQueriesCommand.class);

    @Override
    public String getCommand() {
        return "list-queries";
    }

    @Override
    public String getDescription() {
        return "Lists all queries currently in Rya Streams.";
    }

    @Override
    public void execute(final String[] args) throws ArgumentsException, ExecutionException {
        requireNonNull(args);

        // Parse the command line arguments.
        final KafkaParameters params = new KafkaParameters();
        try {
            new JCommander(params, args);
        } catch (final ParameterException e) {
            throw new ArgumentsException("Could not list the queries because of invalid command line parameters.", e);
        }
        log.trace("Executing the List Query Command.\n" + params.toString());

        // Create properties for interacting with Kafka.
        final Properties producerProperties = new Properties();
        producerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, params.kafkaIP + ":" + params.kafkaPort);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, QueryChangeSerializer.class.getName());

        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, params.kafkaIP + ":" + params.kafkaPort);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, QueryChangeDeserializer.class.getName());

        final Producer<?, QueryChange> queryProducer = new KafkaProducer<>(producerProperties);
        final Consumer<?, QueryChange> queryConsumer = new KafkaConsumer<>(consumerProperties);

        final QueryChangeLog changeLog = new KafkaQueryChangeLog(queryProducer, queryConsumer, KafkaTopics.queryChangeLogTopic(params.ryaInstance));
        final QueryRepository repo = new InMemoryQueryRepository(changeLog);

        // Execute the list queries command.
        final ListQueries listQueries = new DefaultListQueries(repo);
        try {
            final Set<StreamsQuery> queries = listQueries.all();
            System.out.println( formatQueries(queries) );
        } catch (final RyaStreamsException e) {
            log.error("Unable to retrieve the queries.", e);
        }
    }

    private String formatQueries(final Set<StreamsQuery> queries) {
        final StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("Queries in Rya Streams:\n");
        sb.append("---------------------------------------------------------\n");
        queries.forEach(query -> {
            sb.append("ID: ");
            sb.append(query.getQueryId());
            sb.append("\t\t");
            sb.append("Query: ");
            sb.append(query.getSparql());
            sb.append("\n");
        });
        return sb.toString();
    }
}