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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rya.kafka.connect.client.RyaKafkaClientCommand.ArgumentsException;
import org.apache.rya.kafka.connect.client.RyaKafkaClientCommand.ExecutionException;
import org.apache.rya.kafka.connect.client.command.ReadStatementsCommand;
import org.apache.rya.kafka.connect.client.command.WriteStatementsCommand;
import org.eclipse.rdf4j.model.Statement;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A CLI tool used to read/write {@link Statement}s to/from a Kafka topic using the format
 * the Rya Kafka Connect Sinks expect.
 */
@DefaultAnnotation(NonNull.class)
public class CLIDriver {

    /**
     * Maps from command strings to the object that performs the command.
     */
    private static final ImmutableMap<String, RyaKafkaClientCommand> COMMANDS;
    static {
        final Set<Class<? extends RyaKafkaClientCommand>> commandClasses = new HashSet<>();
        commandClasses.add(ReadStatementsCommand.class);
        commandClasses.add(WriteStatementsCommand.class);
        final ImmutableMap.Builder<String, RyaKafkaClientCommand> builder = ImmutableMap.builder();
        for(final Class<? extends RyaKafkaClientCommand> commandClass : commandClasses) {
            try {
                final RyaKafkaClientCommand command = commandClass.newInstance();
                builder.put(command.getCommand(), command);
            } catch (InstantiationException | IllegalAccessException e) {
                System.err.println("Could not run the application because a RyaKafkaClientCommand is missing its empty constructor.");
                e.printStackTrace();
            }
        }
        COMMANDS = builder.build();
    }

    private static final String USAGE = makeUsage(COMMANDS);

    public static void main(final String[] args) {
        // If no command provided or the command isn't recognized, then print the usage.
        if (args.length == 0 || !COMMANDS.containsKey(args[0])) {
            System.out.println(USAGE);
            System.exit(1);
        }

        // Fetch the command that will be executed.
        final String command = args[0];
        final String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);
        final RyaKafkaClientCommand clientCommand = COMMANDS.get(command);

        // Print usage if the arguments are invalid for the command.
        if(!clientCommand.validArguments(commandArgs)) {
            System.out.println(clientCommand.getUsage());
            System.exit(1);
        }

        // Execute the command.
        try {
            clientCommand.execute(commandArgs);
        } catch (ArgumentsException | ExecutionException e) {
            System.err.println("The command: " + command + " failed to execute properly.");
            e.printStackTrace();
            System.exit(2);
        }
    }

    private static String makeUsage(final ImmutableMap<String, RyaKafkaClientCommand> commands) {
        final StringBuilder usage = new StringBuilder();
        usage.append("Usage: ").append(CLIDriver.class.getSimpleName()).append(" <command> (<argument> ... )\n");
        usage.append("\n");
        usage.append("Possible Commands:\n");

        // Sort and find the max width of the commands.
        final List<String> sortedCommandNames = Lists.newArrayList(commands.keySet());
        Collections.sort(sortedCommandNames);

        int maxCommandLength = 0;
        for (final String commandName : sortedCommandNames) {
            maxCommandLength = commandName.length() > maxCommandLength ? commandName.length() : maxCommandLength;
        }

        // Add each command to the usage.
        final String commandFormat = "    %-" + maxCommandLength + "s - %s\n";
        for (final String commandName : sortedCommandNames) {
            final String commandDescription = commands.get(commandName).getDescription();
            usage.append(String.format(commandFormat, commandName, commandDescription));
        }

        return usage.toString();
    }
}