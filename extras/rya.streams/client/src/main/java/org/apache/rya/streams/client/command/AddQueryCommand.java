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

import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.AddQuery;
import org.apache.rya.streams.api.interactor.defaults.DefaultAddQuery;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.client.RyaStreamsCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;

/**
 * A command that adds a new query into Rya Streams.
 */
public class AddQueryCommand implements RyaStreamsCommand {
    private static final Logger log = LoggerFactory.getLogger(AddQueryCommand.class);

    /**
     * Command line parameters that are used by this command to configure
     * itself.
     */
    private static final class Parameters {
        @Parameter(names = { "--query",
        "-q" }, required = true, description = "The SPARQL query to add to Rya Streams.")
        private String query;
        @Parameter(names = { "--topic",
        "-t" }, required = true, description = "The kafka topic to load the statements into.")
        private String topicName;
        @Parameter(names = { "--kafkaPort",
        "-p" }, required = true, description = "The port to use to connect to Kafka.")
        private short kafkaPort;
        @Parameter(names = { "--kafkaHostname",
        "-i" }, required = true, description = "The IP or Hostname to use to connect to Kafka.")
        private String kafkaIP;

        @Override
        public String toString() {
            final StringBuilder parameters = new StringBuilder();
            parameters.append("Parameters");
            parameters.append("\n");

            if (Strings.isNullOrEmpty(query)) {
                parameters.append("\tQuery: " + query);
                parameters.append("\n");
            }

            if (Strings.isNullOrEmpty(kafkaIP)) {
                parameters.append("\tKafka Location: " + kafkaIP);
                if (kafkaPort > 0) {
                    parameters.append(":" + kafkaPort);
                }
                parameters.append("\n");
            }

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
        final JCommander parser = new JCommander(new Parameters());

        final StringBuilder usage = new StringBuilder();
        parser.usage(usage);
        return usage.toString();
    }

    @Override
    public void execute(final String[] args) throws ArgumentsException, ExecutionException {
        requireNonNull(args);

        // Parse the command line arguments.
        final Parameters params = new Parameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not add a new query because of invalid command line parameters.", e);
        }
        log.trace("Executing the Add Query Command\n" + params.toString());

        final QueryRepository repo = null;
        final AddQuery addQuery = new DefaultAddQuery(repo);
        try {
            addQuery.addQuery(params.query);
        } catch (final RyaStreamsException e) {
            log.error("Unable to parse query: " + params.query, e);
        }

        log.trace("Finished executing the Add Query Command.");
    }
}
