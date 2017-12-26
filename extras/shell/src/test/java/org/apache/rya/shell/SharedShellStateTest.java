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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.shell.SharedShellState.ConnectionState;
import org.apache.rya.shell.SharedShellState.ShellState;
import org.junit.Test;

/**
 * Tests the methods of {@link SharedShellState}.
 */
public class SharedShellStateTest {

    @Test
    public void initialStateIsDisconnected() {
        final SharedShellState state = new SharedShellState();

        // Verify disconnected and no values are set.
        final ShellState expected = ShellState.builder()
                .setConnectionState(ConnectionState.DISCONNECTED)
                .build();

        assertEquals(expected, state.getShellState());
    }

    @Test
    public void disconnectedToConnectedToStorage() {
        final SharedShellState state = new SharedShellState();

        // Connect to Accumulo.
        final AccumuloConnectionDetails connectionDetails = mock(AccumuloConnectionDetails.class);
        final RyaClient connectedCommands = mock(RyaClient.class);
        state.connectedToAccumulo(connectionDetails, connectedCommands);

        // Verify the state.
        final ShellState expected = ShellState.builder()
                .setConnectionState(ConnectionState.CONNECTED_TO_STORAGE)
                .setAccumuloDetails(connectionDetails)
                .setConnectedCommands(connectedCommands)
                .build();

        assertEquals(expected, state.getShellState());
    }

    @Test(expected = IllegalStateException.class)
    public void connectToStorageAgain() {
        final SharedShellState state = new SharedShellState();

        // Connect to Accumulo.
        final AccumuloConnectionDetails connectionDetails = mock(AccumuloConnectionDetails.class);
        final RyaClient connectedCommands = mock(RyaClient.class);
        state.connectedToAccumulo(connectionDetails, connectedCommands);

        // Try to set the information again.
        state.connectedToAccumulo(connectionDetails, connectedCommands);
    }

    @Test
    public void connectedToInstance() {
        final SharedShellState state = new SharedShellState();

        // Connect to Accumulo.
        final AccumuloConnectionDetails connectionDetails = mock(AccumuloConnectionDetails.class);
        final RyaClient connectedCommands = mock(RyaClient.class);
        state.connectedToAccumulo(connectionDetails, connectedCommands);

        // Connect to an Instance.
        state.connectedToInstance("instance");

        // Verify the state.
        final ShellState expected = ShellState.builder()
                .setConnectionState(ConnectionState.CONNECTED_TO_INSTANCE)
                .setAccumuloDetails(connectionDetails)
                .setConnectedCommands(connectedCommands)
                .setRyaInstanceName("instance")
                .build();

        assertEquals(expected, state.getShellState());
    }

    @Test
    public void ConnectedToInstanceAgain() {
        final SharedShellState state = new SharedShellState();

        // Connect to Accumulo.
        final AccumuloConnectionDetails connectionDetails = mock(AccumuloConnectionDetails.class);
        final RyaClient connectedCommands = mock(RyaClient.class);
        state.connectedToAccumulo(connectionDetails, connectedCommands);

        // Connect to an Instance.
        state.connectedToInstance("instance");

        // Connect to another instance.
        state.connectedToInstance("secondInstance");

        // Verify the state.
        final ShellState expected = ShellState.builder()
                .setConnectionState(ConnectionState.CONNECTED_TO_INSTANCE)
                .setAccumuloDetails(connectionDetails)
                .setConnectedCommands(connectedCommands)
                .setRyaInstanceName("secondInstance")
                .build();
        assertEquals(expected, state.getShellState());
    }

    @Test(expected = IllegalStateException.class)
    public void connectedToInstanceWhileDisconnectedFromStorage() {
        final SharedShellState state = new SharedShellState();

        state.connectedToInstance("instance");
    }

    @Test
    public void disconnected() {
        final SharedShellState state = new SharedShellState();

        // Connect to Accumulo and an instance.
        final AccumuloConnectionDetails connectionDetails = mock(AccumuloConnectionDetails.class);
        final RyaClient connectedCommands = mock(RyaClient.class);
        state.connectedToAccumulo(connectionDetails, connectedCommands);
        state.connectedToInstance("instance");

        // Disconnect.
        state.disconnected();

        // Verify the state.
        final ShellState expected = ShellState.builder()
                .setConnectionState(ConnectionState.DISCONNECTED)
                .build();
        assertEquals(expected, state.getShellState());
    }

    @Test
    public void disconnectedAgain() {
        // Indicate we have diconnected while already in the disconnected state.
        final SharedShellState state = new SharedShellState();
        state.disconnected();

        // Verify the state.
        final ShellState expected = ShellState.builder()
                .setConnectionState(ConnectionState.DISCONNECTED)
                .build();
        assertEquals(expected, state.getShellState());
    }
}