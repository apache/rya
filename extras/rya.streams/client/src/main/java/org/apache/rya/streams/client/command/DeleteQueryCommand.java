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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.DeleteQuery;
import org.apache.rya.streams.api.interactor.defaults.DefaultDeleteQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.client.RyaStreamsCommand;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLogFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A command that removes a query from Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public class DeleteQueryCommand implements RyaStreamsCommand {

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
    public boolean validArguments(final String[] args) {
        boolean valid = true;
        try {
            new JCommander(new RemoveParameters(), args);
        } catch(final ParameterException e) {
            valid = false;
        }
        return valid;
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

        // Create the Kafka backed QueryChangeLog.
        final String bootstrapServers = params.kafkaIP + ":" + params.kafkaPort;
        final String topic = KafkaTopics.queryChangeLogTopic(params.ryaInstance);
        final QueryChangeLog queryChangeLog = KafkaQueryChangeLogFactory.make(bootstrapServers, topic);

        //The DeleteQuery command doesn't use the scheduled service feature.
        final Scheduler scheduler = Scheduler.newFixedRateSchedule(0L, 5, TimeUnit.SECONDS);
        final QueryRepository queryRepo = new InMemoryQueryRepository(queryChangeLog, scheduler);
        // Execute the delete query command.
        try {
            final DeleteQuery deleteQuery = new DefaultDeleteQuery(queryRepo);
            try {
                deleteQuery.delete(UUID.fromString(params.queryId));
                System.out.println("Deleted query: " + params.queryId);
            } catch (final RyaStreamsException e) {
                System.err.println("Unable to delete query with ID: " + params.queryId);
                e.printStackTrace();
                System.exit(1);
            }
        } catch (final Exception e) {
            System.err.println("Problem encountered while closing the QueryRepository.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}