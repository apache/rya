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

import java.util.concurrent.TimeUnit;

import org.apache.rya.api.utils.QueryInvestigator;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.AddQuery;
import org.apache.rya.streams.api.interactor.defaults.DefaultAddQuery;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.client.RyaStreamsCommand;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLogFactory;
import org.openrdf.query.MalformedQueryException;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A command that adds a new query into Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public class AddQueryCommand implements RyaStreamsCommand {

    /**
     * Command line parameters that are used by this command to configure itself.
     */
    private class AddParameters extends RyaStreamsCommand.KafkaParameters {
        @Parameter(names = { "--query", "-q" }, required = true, description = "The SPARQL query to add to Rya Streams.")
        private String query;

        @Parameter(names = {"--isActive", "-a"}, required = false, description = "True if the added query will be started.")
        private String isActive;

        @Parameter(names = {"--isInsert", "-n"}, required = false, description = "True if the reuslts of the query will be written back to Rya.")
        private String isInsert;

        @Override
        public String toString() {
            final StringBuilder parameters = new StringBuilder();
            parameters.append(super.toString());

            if (!Strings.isNullOrEmpty(query)) {
                parameters.append("\tQuery: " + query + "\n");
            }
            parameters.append("\tIs Active: " + isActive + "\n");
            parameters.append("\tis Insert: " + isInsert + "\n");
            return parameters.toString();
        }
    }

    @Override
    public String getCommand() {
        return "add-query";
    }

    @Override
    public String getDescription() {
        return "Add a new query to Rya Streams.";
    }

    @Override
    public String getUsage() {
        final JCommander parser = new JCommander(new AddParameters());

        final StringBuilder usage = new StringBuilder();
        parser.usage(usage);
        return usage.toString();
    }

    @Override
    public boolean validArguments(final String[] args) {
        boolean valid = true;
        try {
            new JCommander(new AddParameters(), args);
        } catch(final ParameterException e) {
            valid = false;
        }
        return valid;
    }

    @Override
    public void execute(final String[] args) throws ArgumentsException, ExecutionException {
        requireNonNull(args);

        // Parse the command line arguments.
        final AddParameters params = new AddParameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not add a new query because of invalid command line parameters.", e);
        }

        // Create the Kafka backed QueryChangeLog.
        final String bootstrapServers = params.kafkaIP + ":" + params.kafkaPort;
        final String topic = KafkaTopics.queryChangeLogTopic(params.ryaInstance);
        final QueryChangeLog queryChangeLog = KafkaQueryChangeLogFactory.make(bootstrapServers, topic);

        //The AddQuery command doesn't use the scheduled service feature.
        final Scheduler scheduler = Scheduler.newFixedRateSchedule(0L, 5, TimeUnit.SECONDS);
        final QueryRepository queryRepo = new InMemoryQueryRepository(queryChangeLog, scheduler);
        // Execute the add query command.
        try {
            final AddQuery addQuery = new DefaultAddQuery(queryRepo);
            try {
                final Boolean isActive = Boolean.parseBoolean(params.isActive);
                final Boolean isInsert = Boolean.parseBoolean(params.isInsert);

                // If the query's results are meant to be written back to Rya, make sure it creates statements.
                if(isInsert) {
                    final boolean isConstructQuery = QueryInvestigator.isConstruct(params.query);
                    final boolean isInsertQuery = QueryInvestigator.isInsertWhere(params.query);

                    if(isConstructQuery) {
                        System.out.println(
                                "WARNING: CONSTRUCT is part of the SPARQL Query API, so they do not normally\n" +
                                "get written back to the triple store. Consider using an INSERT, which is\n" +
                                "part of the SPARQL Update API, in the future.");
                    }

                    if(!(isConstructQuery || isInsertQuery)) {
                        throw new ArgumentsException("Only CONSTRUCT queries and INSERT updates may be inserted back to the triple store.");
                    }
                }

                final StreamsQuery query = addQuery.addQuery(params.query, isActive, isInsert);
                System.out.println("Added query: " + query.getSparql());
            } catch (final RyaStreamsException e) {
                throw new ExecutionException("Unable to add the query to Rya Streams.", e);
            }
        } catch(final MalformedQueryException e) {
            throw new ArgumentsException("Could not parse the provided query.", e);
        }
    }
}