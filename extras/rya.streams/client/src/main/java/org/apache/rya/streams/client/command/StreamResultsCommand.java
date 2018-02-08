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

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rya.streams.api.entity.QueryResultStream;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.interactor.GetQueryResultStream;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.client.RyaStreamsCommand;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.interactor.KafkaGetQueryResultStream;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLogFactory;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementDeserializer;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Reduced;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A command that streams the results of a query to the console.
 */
@DefaultAnnotation(NonNull.class)
public class StreamResultsCommand implements RyaStreamsCommand {

    /**
     * Command line parameters that are used by this command to configure itself.
     */
    private static final class StreamResultsParameters extends RyaStreamsCommand.KafkaParameters {

        @Parameter(names = {"--queryId", "-q"}, required = true, description = "The query whose results will be streamed to the console.")
        private String queryId;

        @Override
        public String toString() {
            final StringBuilder parameters = new StringBuilder();
            parameters.append(super.toString());

            if (!Strings.isNullOrEmpty(queryId)) {
                parameters.append("\tQuery ID: " + queryId);
                parameters.append("\n");
            }

            return parameters.toString();
        }
    }

    @Override
    public String getCommand() {
        return "stream-results";
    }

    @Override
    public String getDescription() {
        return "Stream the results of a query to the console.";
    }

    @Override
    public String getUsage() {
        final JCommander parser = new JCommander(new StreamResultsParameters());

        final StringBuilder usage = new StringBuilder();
        parser.usage(usage);
        return usage.toString();
    }

    @Override
    public boolean validArguments(final String[] args) {
        boolean valid = true;
        try {
            new JCommander(new StreamResultsParameters(), args);
        } catch(final ParameterException e) {
            valid = false;
        }
        return valid;
    }

    @Override
    public void execute(final String[] args) throws ArgumentsException, ExecutionException {
        requireNonNull(args);

        // Parse the command line arguments.
        final StreamResultsParameters params = new StreamResultsParameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not stream the query's results because of invalid command line parameters.", e);
        }

        // Create the Kafka backed QueryChangeLog.
        final String bootstrapServers = params.kafkaIP + ":" + params.kafkaPort;
        final String topic = KafkaTopics.queryChangeLogTopic(params.ryaInstance);
        final QueryChangeLog queryChangeLog = KafkaQueryChangeLogFactory.make(bootstrapServers, topic);

        // Parse the Query ID from the command line parameters.
        final UUID queryId;
        try {
            queryId = UUID.fromString( params.queryId );
        } catch(final IllegalArgumentException e) {
            throw new ArgumentsException("Invalid Query ID " + params.queryId);
        }

        //The DeleteQuery command doesn't use the scheduled service feature.
        final Scheduler scheduler = Scheduler.newFixedRateSchedule(0L, 5, TimeUnit.SECONDS);
        final QueryRepository queryRepo = new InMemoryQueryRepository(queryChangeLog, scheduler);
        // Fetch the SPARQL of the query whose results will be streamed.
        final String sparql;
        try {
            final Optional<StreamsQuery> sQuery = queryRepo.get(queryId);
            if(!sQuery.isPresent()) {
                throw new ExecutionException("Could not read the results for query with ID " + queryId +
                        " because no such query exists.");
            }
            sparql = sQuery.get().getSparql();
        } catch (final Exception e) {
            throw new ExecutionException("Problem encountered while closing the QueryRepository.", e);
        }

        // This command executes until the application is killed, so create a kill boolean.
        final AtomicBoolean finished = new AtomicBoolean(false);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                finished.set(true);
            }
        });

        // Build the interactor based on the type of result the query produces.
        final GetQueryResultStream<?> getQueryResultStream;
        try {
            final TupleExpr tupleExpr = new SPARQLParser().parseQuery(sparql, null).getTupleExpr();
            if(tupleExpr instanceof Reduced) {
                getQueryResultStream = new KafkaGetQueryResultStream<>(params.kafkaIP, params.kafkaPort, VisibilityStatementDeserializer.class);
            } else {
                getQueryResultStream = new KafkaGetQueryResultStream<>(params.kafkaIP, params.kafkaPort, VisibilityBindingSetDeserializer.class);
            }
        } catch (final MalformedQueryException e) {
            throw new ExecutionException("Could not parse the SPARQL for the query: " + sparql, e);
        }

        // Iterate through the results and print them to the console until the program or the stream ends.
        try (final QueryResultStream<?> stream = getQueryResultStream.fromStart(params.ryaInstance, queryId)) {
            while(!finished.get()) {
                for(final Object result : stream.poll(1000)) {
                    System.out.println(result);
                }
            }
        } catch (final Exception e) {
            System.err.println("Error while reading the results from the stream.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}