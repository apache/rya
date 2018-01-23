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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.client.RyaStreamsCommand;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.interactor.KafkaRunQuery;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLogFactory;
import org.apache.rya.streams.kafka.topology.TopologyFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A command that runs a Rya Streams processing topology on the node the client is executed on until it has finished.
 */
@DefaultAnnotation(NonNull.class)
public class RunQueryCommand implements RyaStreamsCommand {

    private class RunParameters extends RyaStreamsCommand.KafkaParameters {
        @Parameter(names = { "--queryID", "-q" }, required = true, description = "The ID of the query to run.")
        private String queryId;

        @Parameter(names = {"--zookeepers", "-z"}, required = true, description = "The servers that Zookeeper runs on.")
        private String zookeeperServers;

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
        return "run-query";
    }

    @Override
    public String getDescription() {
        return "Runs a Rya Streams query until the command is killed. This command also creates the input and output " +
                "topics required to execute the query.";
    }

    @Override
    public String getUsage() {
        final JCommander parser = new JCommander(new RunParameters());

        final StringBuilder usage = new StringBuilder();
        parser.usage(usage);
        return usage.toString();
    }

    @Override
    public boolean validArguments(final String[] args) {
        boolean valid = true;
        try {
            new JCommander(new RunParameters(), args);
        } catch(final ParameterException e) {
            valid = false;
        }
        return valid;
    }

    @Override
    public void execute(final String[] args) throws ArgumentsException, ExecutionException {
        requireNonNull(args);

        // Parse the command line arguments.
        final RunParameters params = new RunParameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not add a new query because of invalid command line parameters.", e);
        }

        // Create the Kafka backed QueryChangeLog.
        final String bootstrapServers = params.kafkaIP + ":" + params.kafkaPort;
        final String topic = KafkaTopics.queryChangeLogTopic(params.ryaInstance);
        final QueryChangeLog queryChangeLog = KafkaQueryChangeLogFactory.make(bootstrapServers, topic);

        //The RunQuery command doesn't use the scheduled service feature.
        final Scheduler scheduler = Scheduler.newFixedRateSchedule(0L, 5, TimeUnit.SECONDS);
        final QueryRepository queryRepo = new InMemoryQueryRepository(queryChangeLog, scheduler);
        // Look up the query to be executed from the change log.
        try {
            try {
                final UUID queryId = UUID.fromString( params.queryId );
                final Optional<StreamsQuery> query = queryRepo.get(queryId);

                if(!query.isPresent()) {
                    throw new ArgumentsException("There is no registered query for queryId " + params.queryId);
                }

                // Make sure the topics required by the application exists for the specified Rya instances.
                final Set<String> topics = new HashSet<>();
                topics.add( KafkaTopics.statementsTopic(params.ryaInstance) );
                topics.add( KafkaTopics.queryResultsTopic(queryId) );
                KafkaTopics.createTopic(params.zookeeperServers, topics, 1, 1);

                // Run the query that uses those topics.
                final KafkaRunQuery runQuery = new KafkaRunQuery(
                        params.kafkaIP,
                        params.kafkaPort,
                        KafkaTopics.statementsTopic(params.ryaInstance),
                        KafkaTopics.queryResultsTopic(queryId),
                        queryRepo,
                        new TopologyFactory());
                runQuery.run(queryId);
            } catch(final Exception e) {
                throw new ExecutionException("Could not execute the Run Query command.", e);
            }
        } catch(final ExecutionException e) {
            // Rethrow the exceptions that are advertised by execute.
            throw e;
        } catch (final Exception e) {
            throw new ExecutionException("Problem encountered while closing the QueryRepository.", e);
        }
    }
}