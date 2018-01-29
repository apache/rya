/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.shell;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.RyaStreamsDetails;
import org.apache.rya.shell.SharedShellState.ConnectionState;
import org.apache.rya.shell.util.SparqlPrompt;
import org.apache.rya.shell.util.StreamsQueryFormatter;
import org.apache.rya.streams.api.RyaStreamsClient;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.kafka.KafkaRyaStreamsClientFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import com.google.common.base.Optional;

/**
 * Rya Shell commands used to interact with the Rya Streams subsystem.
 */
@Component
public class RyaStreamsCommands implements CommandMarker {

    public static final String STREAMS_CONFIGURE_CMD = "streams-configure";
    public static final String STREAMS_DETAILS_CMD = "streams-details";
    public static final String STREAM_QUERIES_ADD_CMD = "streams-queries-add";
    public static final String STREAM_QUERIES_DELETE_CMD = "streams-queries-delete";
    public static final String STREAM_QUERIES_START_CMD = "streams-queries-start";
    public static final String STREAM_QUERIES_STOP_CMD = "streams-queries-stop";
    public static final String STREAM_QUERIES_LIST_CMD = "streams-queries-list";
    public static final String STREAM_QUERIES_DETAILS_CMD = "streams-queries-details";

    private final SharedShellState state;
    private final SparqlPrompt sparqlPrompt;

    /**
     * Constructs an instance of {@link RyaStreamsCommands}.
     *
     * @param state - Holds shared state between all of the command classes. (not null)
     * @param sparqlPrompt - Prompts a user for a SPARQL query. (not null)
     */
    @Autowired
    public RyaStreamsCommands(
            final SharedShellState state,
            final SparqlPrompt sparqlPrompt) {
        this.state = requireNonNull(state);
        this.sparqlPrompt = requireNonNull(sparqlPrompt);
    }

    /**
     * Enables commands that become available when connected to a Rya instance.
     */
    @CliAvailabilityIndicator({
        STREAMS_CONFIGURE_CMD,
        STREAMS_DETAILS_CMD})
    public boolean areConfigCommandsAvailable() {
        return state.getShellState().getConnectionState() == ConnectionState.CONNECTED_TO_INSTANCE;
    }

    /**
     * Enables commands that become available when a Rya instance has a configured Rya Streams subsystem to use.
     */
    @CliAvailabilityIndicator({
        STREAM_QUERIES_ADD_CMD,
        STREAM_QUERIES_DELETE_CMD,
        STREAM_QUERIES_START_CMD,
        STREAM_QUERIES_STOP_CMD,
        STREAM_QUERIES_LIST_CMD,
        STREAM_QUERIES_DETAILS_CMD})
    public boolean areQueriesCommandsAvailable() {
        return state.getShellState().getRyaStreamsCommands().isPresent();
    }

    @CliCommand(value = STREAMS_CONFIGURE_CMD, help = "Connect a Rya Streams subsystem to a Rya Instance.")
    public String configureRyaStreams(
            @CliOption(key = {"kafkaHostname"}, mandatory = true, help = "The hostname of the Kafka Broker.")
            final String kafkaHostname,
            @CliOption(key = {"kafkaPort"}, mandatory = true, help = "The port of the Kafka Broker.")
            final int kafkaPort) {

        // If this instance was connected to a different Rya Streams subsystem, then close that client.
        final Optional<RyaStreamsClient> oldClient = state.getShellState().getRyaStreamsCommands();
        if(oldClient.isPresent()) {
            try {
                oldClient.get().close();
            } catch (final Exception e) {
                System.err.print("Warning: Could not close the old Rya Streams Client.");
                e.printStackTrace();
            }
        }

        // Update the Rya Details for the connected Rya Instance.
        final String ryaInstance = state.getShellState().getRyaInstanceName().get();
        final RyaClient ryaClient = state.getShellState().getConnectedCommands().get();
        try {
            final RyaStreamsDetails streamsDetails = new RyaStreamsDetails(kafkaHostname, kafkaPort);
            ryaClient.getSetRyaStreamsConfiguration().setRyaStreamsConfiguration(ryaInstance, streamsDetails);
        } catch (final RyaClientException e) {
            throw new RuntimeException("Could not update the Rya instance's Rya Details to include the new " +
                    "information. This command failed to complete.", e);
        }

        // Connect a Rya Streams Client and set it in the shared state.
        final RyaStreamsClient newClient = KafkaRyaStreamsClientFactory.make(ryaInstance, kafkaHostname, kafkaPort);
        state.connectedToRyaStreams(newClient);

        // Return a message that indicates the operation was successful.
        if(oldClient.isPresent()) {
            return "The Rya Streams subsystem that this Rya instance uses has been changed. Any queries that were " +
                    "maintained by the previous subsystem will need to be migrated to the new one.";
        } else {
            return "The Rya Instance has been updated to use the provided Rya Streams subsystem. " +
                    "Rya Streams commands are now avaiable while connected to this instance.";
        }
    }

    @CliCommand(value = STREAMS_DETAILS_CMD, help = "Print information about which Rya Streams subsystem the Rya instance is connected to.")
    public String printRyaStreamsDetails() {
        final String ryaInstance = state.getShellState().getRyaInstanceName().get();
        final RyaClient client = state.getShellState().getConnectedCommands().get();
        try {
            // Handle the case where the instance does not have Rya Details.
            final Optional<RyaDetails> details = client.getGetInstanceDetails().getDetails(ryaInstance);
            if(!details.isPresent()) {
                return "This instance does not have any Rya Details, so it is unable to be connected to the Rya Streams subsystem.";
            }

            // Print a message based on if the instance is connected to Rya Streams.
            final Optional<RyaStreamsDetails> streamsDetails = details.get().getRyaStreamsDetails();
            if(!streamsDetails.isPresent()) {
                return "This instance of Rya has not been configured to use a Rya Streams subsystem.";
            }

            // Print the details about which Rya Streams subsystem is being used.
            return "Kafka Hostname: " + streamsDetails.get().getHostname() + ", Kafka Port: " + streamsDetails.get().getPort();

        } catch (final RyaClientException e) {
            throw new RuntimeException("Could not fetch the Rya Details for this Rya instance.", e);
        }
    }

    @CliCommand(value = STREAM_QUERIES_ADD_CMD, help = "Add a SPARQL query to the Rya Streams subsystem.")
    public String addQuery(
            @CliOption(key = {"inactive"}, mandatory = false, unspecifiedDefaultValue = "false", specifiedDefaultValue = "true",
                       help = "Setting this flag will add the query, but not run it. (default: false)")
            final boolean inactive) {
        final RyaStreamsClient streamsClient = state.getShellState().getRyaStreamsCommands().get();

        // Prompt the user for the SPARQL that defines the query.
        try {
            final Optional<String> sparql = sparqlPrompt.getSparql();

            // If the user aborted the prompt, return.
            if(!sparql.isPresent()) {
                return "";
            }

            final StreamsQuery streamsQuery = streamsClient.getAddQuery().addQuery(sparql.get(), !inactive);
            return "The added query's ID is " + streamsQuery.getQueryId();

        } catch (final IOException | RyaStreamsException e) {
            throw new RuntimeException("Unable to add the SPARQL query to the Rya Streams subsystem.", e);
        }
    }

    @CliCommand(value = STREAM_QUERIES_DELETE_CMD, help = "Delete a SPARQL query from the Rya Streams subsystem.")
    public String deleteQuery(
            @CliOption(key= {"queryId"}, mandatory = true, help = "The ID of the query to remove.")
            final String queryId) {

        final RyaStreamsClient streamsClient = state.getShellState().getRyaStreamsCommands().get();
        final UUID id = UUID.fromString(queryId);
        try {
            streamsClient.getDeleteQuery().delete(id);
        } catch (final RyaStreamsException e) {
            throw new RuntimeException("Could not delete the query from the Rya Streams subsystem.", e);
        }
        return "The query has been deleted.";
    }

    @CliCommand(value = STREAM_QUERIES_START_CMD, help = "Start processing a SPARQL query using the Rya Streams subsystem.")
    public String startQuery(
            @CliOption(key= {"queryId"}, mandatory = true, help = "The ID of the query to start processing.")
            final String queryId) {
        final RyaStreamsClient streamsClient = state.getShellState().getRyaStreamsCommands().get();

        try {
            // Ensure the query exists.
            final UUID id = UUID.fromString(queryId);
            final java.util.Optional<StreamsQuery> streamsQuery = streamsClient.getGetQuery().getQuery(id);
            if(!streamsQuery.isPresent()) {
                throw new RuntimeException("No Rya Streams query exists for ID " + queryId);
            }

            // Ensure it isn't already started.
            if(streamsQuery.get().isActive()) {
                return "That query is already running.";
            }

            // Start it.
            streamsClient.getStartQuery().start(id);
            return "The query will be processed by the Rya Streams subsystem.";

        } catch (final RyaStreamsException e) {
            throw new RuntimeException("Unable to start the Query.", e);
        }
    }

    @CliCommand(value = STREAM_QUERIES_STOP_CMD, help = "Stop processing a SPARQL query using the Rya Streams subsystem.")
    public String stopQuery(
            @CliOption(key= {"queryId"}, mandatory = true, help = "The ID of the query to stop processing.")
            final String queryId) {
        final RyaStreamsClient streamsClient = state.getShellState().getRyaStreamsCommands().get();

        try {
            // Ensure the query exists.
            final UUID id = UUID.fromString(queryId);
            final java.util.Optional<StreamsQuery> streamsQuery = streamsClient.getGetQuery().getQuery(id);
            if(!streamsQuery.isPresent()) {
                throw new RuntimeException("No Rya Streams query exists for ID " + queryId);
            }

            // Ensure it isn't already stopped.
            if(!streamsQuery.get().isActive()) {
                return "That query is already stopped.";
            }

            // Stop it.
            streamsClient.getStopQuery().stop(id);
            return "The query will no longer be processed by the Rya Streams subsystem.";

        } catch (final RyaStreamsException e) {
            throw new RuntimeException("Unable to start the Query.", e);
        }
    }

    @CliCommand(value = STREAM_QUERIES_LIST_CMD, help = "List the queries that are being managed by the configured Rya Streams subsystem.")
    public String listQueries() {
        final RyaStreamsClient streamsClient = state.getShellState().getRyaStreamsCommands().get();
        try {
            final Set<StreamsQuery> queries = streamsClient.getListQueries().all();
            return StreamsQueryFormatter.format(queries);
        } catch (final RyaStreamsException e) {
            throw new RuntimeException("Unable to fetch the queries from the Rya Streams subsystem.", e);
        } catch (final Exception e) {
            throw new RuntimeException("Unable to print the query to the console.", e);
        }
    }

    @CliCommand(value = STREAM_QUERIES_DETAILS_CMD, help = "Print detailed information about a specific query managed by the Rya Streams subsystem.")
    public String printQueryDetails(
            @CliOption(key= {"queryId"}, mandatory = true, help = "The ID of the query whose details will be printed.")
            final String queryId) {
        final RyaStreamsClient streamsClient = state.getShellState().getRyaStreamsCommands().get();
        final UUID id = UUID.fromString(queryId);
        try {
            final java.util.Optional<StreamsQuery> query = streamsClient.getGetQuery().getQuery(id);
            if(!query.isPresent()) {
                return "There is no query with the specified ID.";
            }
            return StreamsQueryFormatter.format(query.get());
        } catch (final RyaStreamsException e) {
            throw new RuntimeException("Unable to fetch the query from the Rya Streams subsystem.", e);
        } catch (final Exception e) {
            throw new RuntimeException("Unable to print the query to the console.", e);
        }
    }
}