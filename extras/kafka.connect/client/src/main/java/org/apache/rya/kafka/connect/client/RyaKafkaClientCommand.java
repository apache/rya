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
package org.apache.rya.kafka.connect.client;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A command that may be executed by the Rya Kafka Connect Client {@link CLIDriver}.
 */
@DefaultAnnotation(NonNull.class)
public interface RyaKafkaClientCommand {

    /**
     * Command line parameters that are used by all commands that interact with Kafka.
     */
    class KafkaParameters {

        @Parameter(names = { "--bootstrapServers", "-b" }, description =
                "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.")
        public String bootstrapServers = "localhost:9092";

        @Parameter(names = { "--topic", "-t" }, required = true, description = "The Kafka topic that will be interacted with.")
        public String topic;
    }

    /**
     * @return What a user would type into the command line to indicate
     *   they want to execute this command.
     */
    public String getCommand();

    /**
     * @return Briefly describes what the command does.
     */
    public String getDescription();

    /**
     * @return Describes what arguments may be provided to the command.
     */
    default public String getUsage() {
        final JCommander parser = new JCommander(new KafkaParameters());

        final StringBuilder usage = new StringBuilder();
        parser.usage(usage);
        return usage.toString();
    }

    /**
     * Validates a set of arguments that may be passed into the command.
     *
     * @param args - The arguments that will be validated. (not null)
     * @return {@code true} if the arguments are valid, otherwise {@code false}.
     */
    public boolean validArguments(String[] args);

    /**
     * Execute the command using the command line arguments.
     *
     * @param args - Command line arguments that configure how the command will execute. (not null)
     * @throws ArgumentsException there was a problem with the provided arguments.
     * @throws ExecutionException There was a problem while executing the command.
     */
    public void execute(final String[] args) throws ArgumentsException, ExecutionException;

    /**
     * A {@link RyaKafkaClientCommand} could not be executed because of a problem with
     * the arguments that were provided to it.
     */
    public static final class ArgumentsException extends Exception {
        private static final long serialVersionUID = 1L;

        public ArgumentsException(final String message) {
            super(message);
        }

        public ArgumentsException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * A {@link RyaKafkaClientCommand} could not be executed.
     */
    public static final class ExecutionException extends Exception {
        private static final long serialVersionUID = 1L;

        public ExecutionException(final String message) {
            super(message);
        }

        public ExecutionException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}