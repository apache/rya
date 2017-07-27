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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.util.Objects;

import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.shell.SharedShellState.ShellState;
import org.apache.rya.shell.util.ConsolePrinter;
import org.apache.rya.shell.util.SparqlPrompt;
import org.openrdf.rio.RDFFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import com.google.common.base.Optional;

/**
 * Rya Shell commands that have to do with common tasks (loading and querying data)
 */
@Component
public class RyaCommands implements CommandMarker {

    private static final Logger log = LoggerFactory.getLogger(RyaCommands.class);

    public static final String LOAD_DATA_CMD = "load-data";
    public static final String SPARQL_QUERY_CMD = "sparql-query";

    private final SharedShellState state;
    private final SparqlPrompt sparqlPrompt;
    private final ConsolePrinter consolePrinter;

    /**
     * Constructs an instance of {@link RyaCommands}.
     *
     * @param state - Holds shared state between all of the command classes. (not null)
     * @param sparqlPrompt - Prompts a user for a SPARQL query. (not null)
     * @param consolePrinter - Allows the command to print feedback to the user. (not null)
     */
    @Autowired
    public RyaCommands(final SharedShellState state, final SparqlPrompt sparqlPrompt,
            final ConsolePrinter consolePrinter) {
        this.state = Objects.requireNonNull(state);
        this.sparqlPrompt = requireNonNull(sparqlPrompt);
        this.consolePrinter = Objects.requireNonNull(consolePrinter);
    }

    /**
     * Enables commands that are always available once the Shell is connected to a Rya Instance.
     */
    @CliAvailabilityIndicator({ LOAD_DATA_CMD, SPARQL_QUERY_CMD })
    public boolean areInstanceCommandsAvailable() {
        switch (state.getShellState().getConnectionState()) {
        case CONNECTED_TO_INSTANCE:
            return true;
        default:
            return false;
        }
    }

    @CliCommand(value = LOAD_DATA_CMD, help = "Loads RDF Statement data from a local file to the connected Rya instance.")
    public String loadData(
            @CliOption(key = { "file" }, mandatory = true, help = "A local file containing RDF Statements that is to be loaded.")
            final String file,
            @CliOption(key = { "format" }, mandatory = false, help = "The format of the supplied RDF Statements file. [RDF/XML, N-Triples, Turtle, N3, TriX, TriG, BinaryRDF, N-Quads, JSON-LD, RDF/JSON, RDFa]")
            final String format
            ) {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaClient commands = shellState.getConnectedCommands().get();
        final Optional<String> ryaInstanceName = shellState.getRyaInstanceName();
        try {
            final long start = System.currentTimeMillis();
            final File rdfInputFile = new File(file);

            RDFFormat rdfFormat = null;
            if (format != null) {
                rdfFormat = RDFFormat.valueOf(format);
                if (rdfFormat == null) {
                    throw new RuntimeException("Unsupported RDF Statement data input format: " + format);
                }
            }
            if (rdfFormat == null) {
                rdfFormat = RDFFormat.forFileName(rdfInputFile.getName());
                if (rdfFormat == null) {
                    throw new RuntimeException("Unable to detect RDF Statement data input format for file: " + rdfInputFile);
                } else {
                    consolePrinter.println("Detected RDF Format: " + rdfFormat);
                    consolePrinter.flush();
                }
            }
            commands.getLoadStatementsFile().loadStatements(ryaInstanceName.get(), rdfInputFile.toPath(), rdfFormat);

            final String seconds = new DecimalFormat("0.0##").format((System.currentTimeMillis() - start) / 1000.0);
            return "Loaded the file: '" + file + "' successfully in " + seconds + " seconds.";

        } catch (final RyaClientException | IOException e) {
            log.error("Error", e);
            throw new RuntimeException("Can not load the RDF Statement data. Reason: " + e.getMessage(), e);
        }
    }

    @CliCommand(value = SPARQL_QUERY_CMD, help = "Executes the provided SPARQL Query on the connected Rya instance.")
    public String sparqlQuery(
            @CliOption(key = { "file" }, mandatory = false, help = "A local file containing the SPARQL Query that is to be read and executed.")
            final String file) {
        // Fetch the command that is connected to the store.
        final ShellState shellState = state.getShellState();
        final RyaClient commands = shellState.getConnectedCommands().get();
        final Optional<String> ryaInstanceName = shellState.getRyaInstanceName();

        try {
            // file option specified
            String sparqlQuery;
            if (file != null) {
                sparqlQuery = new String(Files.readAllBytes(new File(file).toPath()), StandardCharsets.UTF_8);
                consolePrinter.println("Loaded Query:");
                consolePrinter.println(sparqlQuery);
            } else {
                // No Options specified. Show the user the SPARQL Prompt
                final Optional<String> sparqlQueryOpt = sparqlPrompt.getSparql();
                if (sparqlQueryOpt.isPresent()) {
                    sparqlQuery = sparqlQueryOpt.get();
                } else {
                    return ""; // user aborted the SPARQL prompt.
                }
            }

            consolePrinter.println("Executing Query...");
            consolePrinter.flush();
            return commands.getExecuteSparqlQuery().executeSparqlQuery(ryaInstanceName.get(), sparqlQuery);
        } catch (final RyaClientException | IOException e) {
            log.error("Error", e);
            throw new RuntimeException("Can not execute the SPARQL Query. Reason: " + e.getMessage(), e);
        }
    }
}