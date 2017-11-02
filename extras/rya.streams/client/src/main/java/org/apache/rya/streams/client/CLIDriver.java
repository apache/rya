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
package org.apache.rya.streams.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rya.streams.client.RyaStreamsCommand.ArgumentsException;
import org.apache.rya.streams.client.RyaStreamsCommand.ExecutionException;
import org.apache.rya.streams.client.command.AddQueryCommand;
import org.apache.rya.streams.client.command.DeleteQueryCommand;
import org.apache.rya.streams.client.command.ListQueriesCommand;
import org.apache.rya.streams.client.command.LoadStatementsCommand;
import org.apache.rya.streams.client.command.StreamResultsCommand;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * CLI tool for interacting with Rya Streams.
 * </p>
 * This tool can be used to:
 * <ul>
 *   <li>Add a Query to Rya Streams</li>
 *   <li>Delete a Query from Rya Streams</li>
 *   <li>List the Queries that are being managed by Rya Streams</li>
 *   <li>Load a file of RDF Statements into Rya Streams</li>
 *   <li>Stream the results of a Query to the console</li>
 * </ul>
 */
@DefaultAnnotation(NonNull.class)
public class CLIDriver {

    /**
     * Maps from command strings to the object that performs the command.
     */
    private static final ImmutableMap<String, RyaStreamsCommand> COMMANDS;
    static {
        final Set<Class<? extends RyaStreamsCommand>> commandClasses = new HashSet<>();
        commandClasses.add(AddQueryCommand.class);
        commandClasses.add(DeleteQueryCommand.class);
        commandClasses.add(ListQueriesCommand.class);
        commandClasses.add(LoadStatementsCommand.class);
        commandClasses.add(StreamResultsCommand.class);
        final ImmutableMap.Builder<String, RyaStreamsCommand> builder = ImmutableMap.builder();
        for(final Class<? extends RyaStreamsCommand> commandClass : commandClasses) {
            try {
                final RyaStreamsCommand command = commandClass.newInstance();
                builder.put(command.getCommand(), command);
            } catch (InstantiationException | IllegalAccessException e) {
                System.err.println("Could not run the application because a RyaStreamsCommand is missing its empty constructor.");
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
        final RyaStreamsCommand streamsCommand = COMMANDS.get(command);

        // Print usage if the arguments are invalid for the command.
        if(!streamsCommand.validArguments(commandArgs)) {
            System.out.println(streamsCommand.getUsage());
            System.exit(1);
        }

        // Execute the command.
        try {
            streamsCommand.execute(commandArgs);
        } catch (ArgumentsException | ExecutionException e) {
            System.err.println("The command: " + command + " failed to execute properly.");
            e.printStackTrace();
            System.exit(2);
        }
    }

    private static String makeUsage(final ImmutableMap<String, RyaStreamsCommand> commands) {
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