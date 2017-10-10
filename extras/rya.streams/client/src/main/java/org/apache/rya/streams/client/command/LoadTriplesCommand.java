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

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.rya.streams.api.interactor.LoadStatements;
import org.apache.rya.streams.client.RyaStreamsCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Strings;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A command that loads the contents of a statement file into the RYA Streams
 * application.
 */
@DefaultAnnotation(NonNull.class)
public class LoadTriplesCommand implements RyaStreamsCommand {
    private static final Logger log = LoggerFactory.getLogger(LoadTriplesCommand.class);

    /**
     * Command line parameters that are used by this command to configure itself.
     */
    private static final class Parameters {
        @Parameter(names = {"--triplesFile", "-f"}, required = true, description = "The RDF file of statements to load into RYA Streams.")
        private String triplesFile;
        @Parameter(names= {"--topic", "-t"}, required = true, description = "The kafka topic to load the statements into.")
        private String topicName;
        @Parameter(names= {"--kafkaPort", "-p"}, required = true, description = "The port to use to connect to Kafka.")
        private short kafkaPort;
        @Parameter(names= {"--kafkaHostname", "-i"}, required = true, description = "The IP or Hostname to use to connect to Kafka.")
        private String kafkaIP;
        @Parameter(names= {"--zookeeperPort", "-q"}, required = true, description = "The port to use to connect to Zookeeper.")
        private short zookeeperPort;
        @Parameter(names= {"--zookeeperHostname", "-z"}, required = true, description = "The IP or Hostname to use to connect to Zookeeper.")
        private String zookeeperIP;
        @Parameter(names= {"--visibilities", "-v"}, required = true, description = "The visibilities to assign to the statements being loaded in.")
        private String visibilities;

        @Override
        public String toString() {
            final StringBuilder parameters = new StringBuilder();
            parameters.append("Parameters");
            parameters.append("\n");

            if (Strings.isNullOrEmpty(triplesFile)) {
                parameters.append("\tTriples File: " + triplesFile);
                parameters.append("\n");
            }

            if (Strings.isNullOrEmpty(topicName)) {
                parameters.append("\tTopic: " + topicName);
                parameters.append("\n");
            }

            if (Strings.isNullOrEmpty(kafkaIP)) {
                parameters.append("\tKafka Location: " + kafkaIP);
                if (kafkaPort > 0) {
                    parameters.append(":" + kafkaPort);
                }
                parameters.append("\n");
            }

            if (Strings.isNullOrEmpty(zookeeperIP)) {
                parameters.append("\tZookeeper Location: " + zookeeperIP);
                if (zookeeperPort > 0) {
                    parameters.append(":" + zookeeperPort);
                }
                parameters.append("\n");
            }

            if (Strings.isNullOrEmpty(visibilities)) {
                parameters.append("\tVisibilities: " + visibilities);
                parameters.append("\n");
            }

            return parameters.toString();
        }
    }

    @Override
    public String getCommand() {
        return "load-triples";
    }

    @Override
    public String getDescription() {
        return "Load RDF Triples into the Fluo app";
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
            throw new ArgumentsException("Could not load the Triples file because of invalid command line parameters.", e);
        }
        log.trace("Executing the Load Triples Command\n" + params.toString());

        log.trace("Loading RDF Statements from the Triples file '" + params.triplesFile + "'.");
        final Path triplesPath = Paths.get( params.triplesFile );

        final LoadStatements statements = null;
        try {
            statements.load(triplesPath, params.visibilities);
        } catch (final Exception e) {
            log.error("Unable to parse statement file: " + triplesPath.toString(), e);
        }

        log.trace("Finished executing the Load Triples Command.");
    }
}