/**
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

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.api.utils.QueryInvestigator;
import org.apache.rya.streams.api.entity.QueryResultStream;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.interactor.GetQueryResultStream;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.client.RyaStreamsCommand;
import org.apache.rya.streams.client.util.QueryResultsOutputUtil;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.interactor.KafkaGetQueryResultStream;
import org.apache.rya.streams.kafka.queries.KafkaQueryChangeLogFactory;
import org.apache.rya.streams.kafka.serialization.VisibilityBindingSetDeserializer;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementDeserializer;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;

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

        @Parameter(names = {"--file", "-f"}, required = false, description = "If provided, the output file the results will stream into.")
        private String outputPath;

        @Override
        public String toString() {
            final StringBuilder parameters = new StringBuilder();
            parameters.append(super.toString());

            if (!Strings.isNullOrEmpty(queryId)) {
                parameters.append("\tQuery ID: " + queryId + "\n");
            }

            if(!Strings.isNullOrEmpty(outputPath)) {
                parameters.append("\tOutput Path: " + outputPath + "\n");
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

        // Fetch the SPARQL of the query whose results will be streamed.
        final String sparql;
        try(QueryRepository queryRepo = new InMemoryQueryRepository(queryChangeLog)) {
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
        boolean isStatementResults = false;

        final GetQueryResultStream<?> getQueryResultStream;
        try {
            isStatementResults = QueryInvestigator.isConstruct(sparql) | QueryInvestigator.isInsertWhere(sparql);
            if(isStatementResults) {
                getQueryResultStream = new KafkaGetQueryResultStream<>(params.kafkaIP, params.kafkaPort, VisibilityStatementDeserializer.class);
            } else {
                getQueryResultStream = new KafkaGetQueryResultStream<>(params.kafkaIP, params.kafkaPort, VisibilityBindingSetDeserializer.class);
            }
        } catch (final MalformedQueryException e) {
            throw new ExecutionException("Could not parse the SPARQL for the query: " + sparql, e);
        }

        // Iterate through the results and print them to the configured output mechanism.
        try (final QueryResultStream<?> resultsStream = getQueryResultStream.fromStart(queryId)) {
            final TupleExpr tupleExpr = new SPARQLParser().parseQuery(sparql, null).getTupleExpr();
            if(params.outputPath != null) {
                final Path file = Paths.get(params.outputPath);
                try(OutputStream out = Files.newOutputStream(file)) {
                    if(isStatementResults) {
                        final QueryResultStream<VisibilityStatement> stmtStream = (QueryResultStream<VisibilityStatement>) resultsStream;
                        QueryResultsOutputUtil.toNtriplesFile(out, stmtStream, finished);
                    } else {
                        final QueryResultStream<VisibilityBindingSet> bsStream = (QueryResultStream<VisibilityBindingSet>) resultsStream;
                        QueryResultsOutputUtil.toBindingSetJSONFile(out, tupleExpr, bsStream, finished);
                    }
                }
            } else {
                streamToSystemOut(resultsStream, finished);
            }
        } catch (final Exception e) {
            System.err.println("Error while reading the results from the stream.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void streamToSystemOut(final QueryResultStream<?> stream, final AtomicBoolean shutdownSignal) throws Exception {
        requireNonNull(stream);
        requireNonNull(shutdownSignal);

        while(!shutdownSignal.get()) {
            for(final Object result : stream.poll(1000)) {
                System.out.println(result);
            }
        }
    }
}