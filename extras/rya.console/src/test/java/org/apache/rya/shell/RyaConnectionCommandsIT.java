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
///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
package org.apache.rya.shell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.shell.Bootstrap;
import org.springframework.shell.core.CommandResult;
import org.springframework.shell.core.JLineShellComponent;

import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.shell.SharedShellState.ConnectionState;
import org.apache.rya.shell.SharedShellState.ShellState;
import org.apache.rya.shell.util.InstallPrompt;
import org.apache.rya.shell.util.PasswordPrompt;

/**
 * Integration tests the methods of {@link RyaConnectionCommands}.
 */
public class RyaConnectionCommandsIT extends RyaShellITBase {

    @Test
    public void connectAccumulo() throws IOException {
        final MiniAccumuloCluster cluster = getCluster();
        final Bootstrap bootstrap = getTestBootstrap();
        final JLineShellComponent shell = getTestShell();

        // Mock the user entering the correct password.
        final ApplicationContext context = bootstrap.getApplicationContext();
        final PasswordPrompt mockPrompt = context.getBean( PasswordPrompt.class );
        when(mockPrompt.getPassword()).thenReturn("password".toCharArray());

        // Execute the connect command.
        final String cmd =
                RyaConnectionCommands.CONNECT_ACCUMULO_CMD + " " +
                        "--username root " +
                        "--instanceName " + cluster.getInstanceName() + " "+
                        "--zookeepers " + cluster.getZooKeepers();

        final CommandResult connectResult = shell.executeCommand(cmd);

        // Ensure the connection was successful.
        assertTrue( connectResult.isSuccess() );
    }

    @Test
    public void connectAccumulo_noAuths() throws IOException {
        final MiniAccumuloCluster cluster = getCluster();
        final Bootstrap bootstrap = getTestBootstrap();
        final JLineShellComponent shell = getTestShell();

        // Mock the user entering the correct password.
        final ApplicationContext context = bootstrap.getApplicationContext();
        final PasswordPrompt mockPrompt = context.getBean( PasswordPrompt.class );
        when(mockPrompt.getPassword()).thenReturn("password".toCharArray());

        // Execute the command
        final String cmd =
                RyaConnectionCommands.CONNECT_ACCUMULO_CMD + " " +
                        "--username root " +
                        "--instanceName " + cluster.getInstanceName() + " "+
                        "--zookeepers " + cluster.getZooKeepers();

        final CommandResult connectResult = shell.executeCommand(cmd);

        // Ensure the connection was successful.
        assertTrue( connectResult.isSuccess() );
    }

    @Test
    public void connectAccumulo_wrongCredentials() throws IOException {
        final MiniAccumuloCluster cluster = getCluster();
        final Bootstrap bootstrap = getTestBootstrap();
        final JLineShellComponent shell = getTestShell();

        // Mock the user entering the wrong password.
        final ApplicationContext context = bootstrap.getApplicationContext();
        final PasswordPrompt mockPrompt = context.getBean( PasswordPrompt.class );
        when(mockPrompt.getPassword()).thenReturn("asjifo[ijwa".toCharArray());

        // Execute the command
        final String cmd =
                RyaConnectionCommands.CONNECT_ACCUMULO_CMD + " " +
                        "--username root " +
                        "--instanceName " + cluster.getInstanceName() + " "+
                        "--zookeepers " + cluster.getZooKeepers();

        final CommandResult connectResult = shell.executeCommand(cmd);

        // Ensure the command failed.
        assertFalse( connectResult.isSuccess() );
    }

    @Test
    public void printConnectionDetails_notConnected() {
        final JLineShellComponent shell = getTestShell();

        // Run the print connection details command.
        final CommandResult printResult = shell.executeCommand( RyaConnectionCommands.PRINT_CONNECTION_DETAILS_CMD );
        final String msg = (String) printResult.getResult();

        final String expected = "The shell is not connected to anything.";
        assertEquals(expected, msg);
    }

    @Test
    public void printConnectionDetails_connectedToAccumulo() throws IOException {
        final MiniAccumuloCluster cluster = getCluster();
        final Bootstrap bootstrap = getTestBootstrap();
        final JLineShellComponent shell = getTestShell();

        // Mock the user entering the correct password.
        final ApplicationContext context = bootstrap.getApplicationContext();
        final PasswordPrompt mockPrompt = context.getBean( PasswordPrompt.class );
        when(mockPrompt.getPassword()).thenReturn("password".toCharArray());

        // Connect to the mini accumulo instance.
        final String cmd =
                RyaConnectionCommands.CONNECT_ACCUMULO_CMD + " " +
                        "--username root " +
                        "--instanceName " + cluster.getInstanceName() + " "+
                        "--zookeepers " + cluster.getZooKeepers();
        shell.executeCommand(cmd);

        // Run the print connection details command.
        final CommandResult printResult = shell.executeCommand( RyaConnectionCommands.PRINT_CONNECTION_DETAILS_CMD );
        final String msg = (String) printResult.getResult();

        final String expected =
                "The shell is connected to an instance of Accumulo using the following parameters:\n" +
                "    Username: root\n" +
                "    Instance Name: " + cluster.getInstanceName() + "\n" +
                "    Zookeepers: " + cluster.getZooKeepers();
        assertEquals(expected, msg);
    }

    @Test
    public void connectToInstance() throws IOException {
        final MiniAccumuloCluster cluster = getCluster();
        final Bootstrap bootstrap = getTestBootstrap();
        final JLineShellComponent shell = getTestShell();

        // Mock the user entering the correct password.
        final ApplicationContext context = bootstrap.getApplicationContext();
        final PasswordPrompt mockPrompt = context.getBean( PasswordPrompt.class );
        when(mockPrompt.getPassword()).thenReturn("password".toCharArray());

        // Connect to the mini accumulo instance.
        String cmd =
                RyaConnectionCommands.CONNECT_ACCUMULO_CMD + " " +
                        "--username root " +
                        "--instanceName " + cluster.getInstanceName() + " "+
                        "--zookeepers " + cluster.getZooKeepers();
        CommandResult result = shell.executeCommand(cmd);

        // Install an instance of rya.
        final String instanceName = "testInstance";
        final InstallConfiguration installConf = InstallConfiguration.builder().build();

        final InstallPrompt installPrompt = context.getBean( InstallPrompt.class );
        when(installPrompt.promptInstanceName()).thenReturn("testInstance");
        when(installPrompt.promptInstallConfiguration()).thenReturn( installConf );
        when(installPrompt.promptVerified(instanceName, installConf)).thenReturn(true);

        result = shell.executeCommand( RyaAdminCommands.INSTALL_CMD );
        assertTrue( result.isSuccess() );

        // Connect to the instance that was just installed.
        cmd = RyaConnectionCommands.CONNECT_INSTANCE_CMD + " --instance " + instanceName;
        result = shell.executeCommand(cmd);
        assertTrue( result.isSuccess() );

        // Verify the shell state indicates it is connected to an instance.
        final SharedShellState sharedState = context.getBean( SharedShellState.class );
        final ShellState state = sharedState.getShellState();
        assertEquals(ConnectionState.CONNECTED_TO_INSTANCE, state.getConnectionState());
    }

    @Test
    public void connectToInstance_instanceDoesNotExist() throws IOException {
        final MiniAccumuloCluster cluster = getCluster();
        final Bootstrap bootstrap = getTestBootstrap();
        final JLineShellComponent shell = getTestShell();

        // Mock the user entering the correct password.
        final ApplicationContext context = bootstrap.getApplicationContext();
        final PasswordPrompt mockPrompt = context.getBean( PasswordPrompt.class );
        when(mockPrompt.getPassword()).thenReturn("password".toCharArray());

        // Connect to the mini accumulo instance.
        String cmd =
                RyaConnectionCommands.CONNECT_ACCUMULO_CMD + " " +
                        "--username root " +
                        "--instanceName " + cluster.getInstanceName() + " "+
                        "--zookeepers " + cluster.getZooKeepers();
        shell.executeCommand(cmd);

        // Try to connect to a non-existing instance.
        cmd = RyaConnectionCommands.CONNECT_INSTANCE_CMD + " --instance doesNotExist";
        final CommandResult result = shell.executeCommand(cmd);
        assertFalse( result.isSuccess() );
    }

    @Test
    public void disconnect() throws IOException {
        final MiniAccumuloCluster cluster = getCluster();
        final Bootstrap bootstrap = getTestBootstrap();
        final JLineShellComponent shell = getTestShell();

        // Mock the user entering the correct password.
        final ApplicationContext context = bootstrap.getApplicationContext();
        final PasswordPrompt mockPrompt = context.getBean( PasswordPrompt.class );
        when(mockPrompt.getPassword()).thenReturn("password".toCharArray());

        // Connect to the mini accumulo instance.
        final String cmd =
                RyaConnectionCommands.CONNECT_ACCUMULO_CMD + " " +
                        "--username root " +
                        "--instanceName " + cluster.getInstanceName() + " "+
                        "--zookeepers " + cluster.getZooKeepers();
        shell.executeCommand(cmd);

        // Disconnect from it.
        final CommandResult disconnectResult = shell.executeCommand( RyaConnectionCommands.DISCONNECT_COMMAND_NAME_CMD );
        assertTrue( disconnectResult.isSuccess() );
    }
}