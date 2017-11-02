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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.api.entity.QueryResultStream;
import org.apache.rya.streams.api.interactor.GetQueryResultStream;
import org.apache.rya.streams.client.RyaStreamsCommand;
import org.apache.rya.streams.kafka.interactor.KafkaGetQueryResultStream;

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

        final UUID queryId;
        try {
            queryId = UUID.fromString( params.queryId );
        } catch(final IllegalArgumentException e) {
            throw new ArgumentsException("Invalid Query ID " + params.queryId);
        }

        // This command executes until the application is killed, so create a kill boolean.
        final AtomicBoolean finished = new AtomicBoolean(false);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                finished.set(true);
            }
        });

        // Execute the command.
        final GetQueryResultStream getQueryResultStream = new KafkaGetQueryResultStream(params.kafkaIP, params.kafkaPort);

        try (final QueryResultStream stream = getQueryResultStream.fromNow(queryId)) {
            while(!finished.get()) {
                for(final VisibilityBindingSet visBs : stream.poll(1000)) {
                    System.out.println(visBs);
                }
            }
        } catch (final Exception e) {
            System.err.println("Error while reading the results from the stream.");
            e.printStackTrace();
            System.exit(1);
        }
    }
}