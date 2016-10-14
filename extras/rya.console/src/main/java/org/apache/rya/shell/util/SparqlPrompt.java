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

import javax.annotation.ParametersAreNonnullByDefault;

import jline.console.ConsoleReader;

/**
 * A mechanism for prompting a user of the application for a SPARQL string.
 */
@ParametersAreNonnullByDefault
public interface SparqlPrompt {

    /**
     * Prompt the user for a SPARQL query, wait for their input, and then get the
     * value they entered.
     *
     * @return The user entered SPARQL query.
     * @throws IOEXception There was a problem reading the user's input.
     */
    public String getSparql() throws IOException;

    /**
     * Prompts a user for a SPARQL query using a JLine {@link ConsoleReader}.
     */
    @ParametersAreNonnullByDefault
    public static class JLineSparqlPrompt extends JLinePrompt implements SparqlPrompt {

        @Override
        public String getSparql() throws IOException {
            final ConsoleReader reader = getReader();
            reader.setPrompt("SPARQL: ");
            return reader.readLine();
        }
    }
}