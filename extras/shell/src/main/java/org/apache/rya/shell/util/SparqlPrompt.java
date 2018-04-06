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
package org.apache.rya.shell.util;

import java.io.IOException;

import com.google.common.base.Optional;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import jline.console.ConsoleReader;

/**
 * A mechanism for prompting a user of the application for a SPARQL string.
 */
@DefaultAnnotation(NonNull.class)
public interface SparqlPrompt {

    /**
     * Prompt the user for a SPARQL query, wait for their input, and then get the value they entered.
     *
     * @return The user entered SPARQL query, or an empty string if the user aborts.
     * @throws IOException There was a problem reading the user's input.
     */
    public Optional<String> getSparql() throws IOException;

    /**
     * Prompt the user for a SPARQL query, wait for their input, and then get the value they entered.
     *
     * @return The user entered SPARQL query, or an empty string if the user aborts.
     * @throws IOException There was a problem reading the user's input.
     */
    public Optional<String> getSparqlWithResults() throws IOException;

    /**
     * Prompts a user for a SPARQL query using a JLine {@link ConsoleReader}.
     */
    @DefaultAnnotation(NonNull.class)
    public static class JLineSparqlPrompt extends JLinePrompt implements SparqlPrompt {

        private final String EXECUTE_COMMAND = "\\e";
        private final String CLEAR_COMMAND = "\\c";
        private final String NEXT_RESULTS_COMMAND = "ENTER";
        private final String STOP_COMMAND = "\\s";

        @Override
        public Optional<String> getSparql() throws IOException {
            final ConsoleReader reader = getReader();
            reader.setCopyPasteDetection(true); // disable tab completion from activating
            reader.setHistoryEnabled(false);    // don't store SPARQL fragments in the command history
            try {
                reader.println("Enter a SPARQL Query.");
                reader.println("Type '" + EXECUTE_COMMAND + "' to execute the current query.");
                reader.println("Type '" + CLEAR_COMMAND + "' to clear the current query.");
                reader.flush();

                final StringBuilder sb = new StringBuilder();
                String line = reader.readLine("SPARQL> ");
                while (!line.endsWith(CLEAR_COMMAND) && !line.endsWith(EXECUTE_COMMAND)) {
                    sb.append(line).append("\n");
                    line = reader.readLine("     -> ");
                }

                if (line.endsWith(EXECUTE_COMMAND)) {
                    sb.append(line.substring(0, line.length() - EXECUTE_COMMAND.length()));
                    return Optional.of(sb.toString());
                }
                return Optional.absent();
            } finally {
                reader.setHistoryEnabled(true);      // restore the ConsoleReader's settings
                reader.setCopyPasteDetection(false); // restore tab completion
            }
        }

        @Override
        public Optional<String> getSparqlWithResults() throws IOException {
            final ConsoleReader reader = getReader();
            reader.setCopyPasteDetection(true); // disable tab completion from activating
            reader.setHistoryEnabled(false);    // don't store SPARQL fragments in the command history
            try {
                reader.println("To see more results press [Enter].");
                reader.println("Type '" + STOP_COMMAND + "' to stop showing results.");
                reader.flush();

                final String line = reader.readLine("> ");
                if(line.endsWith(STOP_COMMAND)) {
                    return Optional.of(STOP_COMMAND);
                }
                return Optional.absent();
            } finally {
                reader.setHistoryEnabled(true);      // restore the ConsoleReader's settings
                reader.setCopyPasteDetection(false); // restore tab completion
            }
        }
    }

}