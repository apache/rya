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
import java.nio.CharBuffer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import com.google.common.base.Optional;

import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.shell.SharedShellState.ConnectionState;
import org.apache.rya.shell.util.ConnectorFactory;
import org.apache.rya.shell.util.PasswordPrompt;

/**
 * Spring Shell commands that manage the connection that is used by the shell.
 */
@Component
public class RyaConnectionCommands implements CommandMarker {

    // Command line commands.
    public static final String PRINT_CONNECTION_DETAILS_CMD = "print-connection-details";
    public static final String CONNECT_ACCUMULO_CMD = "connect-accumulo";
    public static final String CONNECT_INSTANCE_CMD = "connect-to-instance";
    public static final String DISCONNECT_COMMAND_NAME_CMD = "disconnect";

    private final SharedShellState sharedState;
    private final PasswordPrompt passwordPrompt;

    /**
     * Constructs an instance of {@link RyaConnectionCommands}.
     *
     * @param state - Holds shared state between all of the command classes. (not null)
     * @param passwordPrompt - Prompts the user for their password when connecting to a Rya store. (not null)
     */
    @Autowired
    public RyaConnectionCommands(final SharedShellState state, final PasswordPrompt passwordPrompt) {
        this.sharedState = requireNonNull( state );
        this.passwordPrompt = requireNonNull(passwordPrompt);
    }

    @CliAvailabilityIndicator({PRINT_CONNECTION_DETAILS_CMD})
    public boolean isPrintConnectionDetailsAvailable() {
        return true;
    }

    @CliAvailabilityIndicator({CONNECT_ACCUMULO_CMD})
    public boolean areConnectCommandsAvailable() {
        return sharedState.getShellState().getConnectionState() == ConnectionState.DISCONNECTED;
    }

    @CliAvailabilityIndicator({CONNECT_INSTANCE_CMD})
    public boolean isConnectToInstanceAvailable() {
        switch(sharedState.getShellState().getConnectionState()) {
            case CONNECTED_TO_STORAGE:
            case CONNECTED_TO_INSTANCE:
                return true;
            default:
                return false;
        }
    }

    @CliAvailabilityIndicator({DISCONNECT_COMMAND_NAME_CMD})
    public boolean isDisconnectAvailable() {
        return sharedState.getShellState().getConnectionState() != ConnectionState.DISCONNECTED;
    }

    @CliCommand(value = PRINT_CONNECTION_DETAILS_CMD, help = "Print information about the Shell's Rya storage connection.")
    public String printConnectionDetails() {
        final Optional<AccumuloConnectionDetails> detailsHolder = sharedState.getShellState().getConnectionDetails();

        if(detailsHolder.isPresent()) {
            final AccumuloConnectionDetails details = detailsHolder.get();
            return "The shell is connected to an instance of Accumulo using the following parameters:\n" +
                    "    Username: " + details.getUsername() + "\n" +
                    "    Instance Name: " + details.getInstanceName() + "\n" +
                    "    Zookeepers: " + details.getZookeepers();
        } else {
            return "The shell is not connected to anything.";
        }
    }

    @CliCommand(value = CONNECT_ACCUMULO_CMD, help = "Connect the shell to an instance of Accumulo.")
    public String connectToAccumulo(
            @CliOption(key = {"username"}, mandatory = true, help = "The username that will be used to connect to Accummulo.")
            final String username,
            @CliOption(key = {"instanceName"}, mandatory = true, help = "The name of the Accumulo instance that will be connected to.")
            final String instanceName,
            @CliOption(key = {"zookeepers"}, mandatory = true, help = "A comma delimited list of zookeeper server hostnames.")
            final String zookeepers
            ) {

        try {
            // Prompt the user for their password.
            final char[] password = passwordPrompt.getPassword();
            final Connector connector= new ConnectorFactory().connect(username, CharBuffer.wrap(password), instanceName, zookeepers);

            // Initialize the connected to Accumulo shared state.
            final AccumuloConnectionDetails accumuloDetails = new AccumuloConnectionDetails(username, password, instanceName, zookeepers);
            final RyaClient commands = AccumuloRyaClientFactory.build(accumuloDetails, connector);
            sharedState.connectedToAccumulo(accumuloDetails, commands);

        } catch(IOException | AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException("Could not connect to Accumulo. Reason: " + e.getMessage(), e);
        }

        return "Connected. You must select a Rya instance to interact with next.";
    }

    @CliCommand(value = CONNECT_INSTANCE_CMD, help = "Connect to a specific ")
    public void connectToInstance(
            @CliOption(key = {"instance"}, mandatory = true, help = "The name of the Rya Instance the shell will interact with.")
            final String instance) {
        try {
            final InstanceExists instanceExists = sharedState.getShellState().getConnectedCommands().get().getInstanceExists();

            // TODO gracefully fail if that version doen't support it. maybe the list command should go ahead

            // Make sure the requested instance exists.
            if(!instanceExists.exists(instance)) {
                throw new RuntimeException(String.format("'%s' does not match an existing Rya instance.", instance));
            }
        } catch(final RyaClientException e) {
            throw new RuntimeException("Could not connect to Rya instance. Reason: " + e.getMessage(), e);
        }

        // Store the instance name in the shared state.
        sharedState.connectedToInstance(instance);
    }

    @CliCommand(value = DISCONNECT_COMMAND_NAME_CMD, help = "Disconnect the shell from the Rya storage it is connect to.")
    public void disconnect() {
        sharedState.disconnected();
    }
}