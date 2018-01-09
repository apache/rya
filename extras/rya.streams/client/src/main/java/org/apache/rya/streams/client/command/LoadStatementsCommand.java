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
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.api.interactor.LoadStatements;
import org.apache.rya.streams.client.RyaStreamsCommand;
import org.apache.rya.streams.kafka.KafkaTopics;
import org.apache.rya.streams.kafka.interactor.KafkaLoadStatements;
import org.apache.rya.streams.kafka.serialization.VisibilityStatementSerializer;

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
public class LoadStatementsCommand implements RyaStreamsCommand {

    /**
     * Command line parameters that are used by this command to configure itself.
     */
    private static final class LoadStatementsParameters extends RyaStreamsCommand.KafkaParameters {

        @Parameter(names = {"--statementsFile", "-f"}, required = true, description = "The file of RDF statements to load into Rya Streams.")
        private String statementsFile;

        @Parameter(names= {"--visibilities", "-v"}, required = true, description = "The visibilities to assign to the statements being loaded in.")
        private String visibilities;

        @Override
        public String toString() {
            final StringBuilder parameters = new StringBuilder();
            parameters.append(super.toString());

            if (!Strings.isNullOrEmpty(statementsFile)) {
                parameters.append("\tStatements File: " + statementsFile);
                parameters.append("\n");
            }

            if (!Strings.isNullOrEmpty(visibilities)) {
                parameters.append("\tVisibilities: " + visibilities);
                parameters.append("\n");
            }

            return parameters.toString();
        }
    }

    @Override
    public String getCommand() {
        return "load-statements";
    }

    @Override
    public String getDescription() {
        return "Load RDF Statements into Rya Streams.";
    }

    @Override
    public String getUsage() {
        final JCommander parser = new JCommander(new LoadStatementsParameters());

        final StringBuilder usage = new StringBuilder();
        parser.usage(usage);
        return usage.toString();
    }

    @Override
    public boolean validArguments(final String[] args) {
        boolean valid = true;
        try {
            new JCommander(new LoadStatementsParameters(), args);
        } catch(final ParameterException e) {
            valid = false;
        }
        return valid;
    }

    @Override
    public void execute(final String[] args) throws ArgumentsException, ExecutionException {
        requireNonNull(args);

        // Parse the command line arguments.
        final LoadStatementsParameters params = new LoadStatementsParameters();
        try {
            new JCommander(params, args);
        } catch(final ParameterException e) {
            throw new ArgumentsException("Could not load the Statements file because of invalid command line parameters.", e);
        }

        final Path statementsPath = Paths.get(params.statementsFile);

        final Properties producerProps = buildProperties(params);
        try (final Producer<Object, VisibilityStatement> producer = new KafkaProducer<>(producerProps)) {
            final LoadStatements statements = new KafkaLoadStatements(KafkaTopics.statementsTopic(params.ryaInstance), producer);
            System.out.printf("Loading statements from file `%s` using visibilities `%s`.\n", statementsPath, params.visibilities);
            statements.fromFile(statementsPath, params.visibilities);
        } catch (final Exception e) {
            System.err.println("Unable to parse statements file: " + statementsPath.toString());
            e.printStackTrace();
        }
    }

    private Properties buildProperties(final LoadStatementsParameters params) {
        requireNonNull(params);
        final Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, params.kafkaIP + ":" + params.kafkaPort);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VisibilityStatementSerializer.class.getName());
        return props;
    }
}