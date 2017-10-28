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
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.DeleteQuery;
import org.apache.rya.streams.api.interactor.defaults.DefaultDeleteQuery;
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
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A command that removes a query from Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public class DeleteQueryCommand implements RyaStreamsCommand {
    private static final Logger log = LoggerFactory.getLogger(DeleteQueryCommand.class);

    /**
     * Command line parameters that are used by this command to configure itself.
     */
    private class RemoveParameters extends RyaStreamsCommand.KafkaParameters {
        @Parameter(names = { "--queryID", "-q" }, required = true, description = "The ID of the query to remove from Rya Streams.")
        private String queryId;

        @Override
        public String toString() {
            final StringBuilder parameters = new StringBuilder();
            parameters.append(super.toString());

            if (!Strings.isNullOrEmpty(queryId)) {
                parameters.append("\tQueryID: " + queryId);
                parameters.append("\n");
            }
            return parameters.toString();
        }
    }

    @Override
    public String getCommand() {
        return "remove-query";
    }

    @Override
    public String getDescription() {
        return "Removes a query from Rya Streams.";
    }

    @Override
    public String getUsage() {
        final JCommander parser = new JCommander(new RemoveParameters());

        final StringBuilder usage = new StringBuilder();
        parser.usage(usage);
        return usage.toString();
    }

    @Override
    public void execute(final String[] args) throws ArgumentsException, ExecutionException {
        requireNonNull(args);

        // Parse the command line arguments.
        final RemoveParameters params = new RemoveParameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not add a new query because of invalid command line parameters.", e);
        }
        log.trace("Executing the Add Query Command\n" + params.toString());

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

        // Execute the delete query command.
        final DeleteQuery deleteQuery = new DefaultDeleteQuery(repo);
        try {
            deleteQuery.delete(UUID.fromString(params.queryId));
            log.trace("Deleted query: " + params.queryId);
        } catch (final RyaStreamsException e) {
            log.error("Unable to delete query with ID: " + params.queryId, e);
        }

        log.trace("Finished executing the Delete Query Command.");
    }
}