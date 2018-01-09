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

import java.util.Set;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.ListQueries;
import org.apache.rya.streams.api.interactor.defaults.DefaultListQueries;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.client.RyaStreamsCommand;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLogFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A command that lists all queries currently in Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public class ListQueriesCommand implements RyaStreamsCommand {

    @Override
    public String getCommand() {
        return "list-queries";
    }

    @Override
    public String getDescription() {
        return "Lists all queries currently in Rya Streams.";
    }

    @Override
    public boolean validArguments(final String[] args) {
        boolean valid = true;
        try {
            new JCommander(new KafkaParameters(), args);
        } catch(final ParameterException e) {
            valid = false;
        }
        return valid;
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

        // Create the Kafka backed QueryChangeLog.
        final String bootstrapServers = params.kafkaIP + ":" + params.kafkaPort;
        final String topic = KafkaTopics.queryChangeLogTopic(params.ryaInstance);
        final QueryChangeLog queryChangeLog = KafkaQueryChangeLogFactory.make(bootstrapServers, topic);

        // Execute the list queries command.
        try(QueryRepository queryRepo = new InMemoryQueryRepository(queryChangeLog)) {
            final ListQueries listQueries = new DefaultListQueries(queryRepo);
            try {
                final Set<StreamsQuery> queries = listQueries.all();
                System.out.println( formatQueries(queries) );
            } catch (final RyaStreamsException e) {
                System.err.println("Unable to retrieve the queries.");
                e.printStackTrace();
                System.exit(1);
            }
        } catch (final Exception e) {
            System.err.println("Problem encountered while closing the QueryRepository.");
            e.printStackTrace();
            System.exit(1);
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