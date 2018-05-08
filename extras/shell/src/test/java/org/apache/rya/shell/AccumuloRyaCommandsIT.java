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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.shell.util.InstallPrompt;
import org.apache.rya.shell.util.PasswordPrompt;
import org.apache.rya.shell.util.SparqlPrompt;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.shell.Bootstrap;
import org.springframework.shell.core.CommandResult;
import org.springframework.shell.core.JLineShellComponent;

import com.google.common.base.Optional;

/**
 * Integration tests for the methods of {@link RyaCommands}.
 */
public class AccumuloRyaCommandsIT extends RyaShellAccumuloITBase {

    @Test
    public void loadsAndQueryData() throws Exception {
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
        when(installPrompt.promptInstallConfiguration("testInstance")).thenReturn( installConf );
        when(installPrompt.promptVerified(instanceName, installConf)).thenReturn(true);

        result = shell.executeCommand( RyaAdminCommands.INSTALL_CMD );
        assertTrue( result.isSuccess() );

        // Connect to the instance that was just installed.
        cmd = RyaConnectionCommands.CONNECT_INSTANCE_CMD + " --instance " + instanceName;
        result = shell.executeCommand(cmd);
        assertTrue( result.isSuccess() );

        // Load a statements file into the instance.
        cmd = RyaCommands.LOAD_DATA_CMD + " --file src/test/resources/test-statements.nt";
        result = shell.executeCommand(cmd);
        assertTrue( result.isSuccess() );

        // Query for all of the statements that were loaded.
        final SparqlPrompt sparqlPrompt = context.getBean(SparqlPrompt.class);
        when(sparqlPrompt.getSparql()).thenReturn(Optional.of("select * where { ?s ?p ?o .}"));

        cmd = RyaCommands.SPARQL_QUERY_CMD;
        result = shell.executeCommand(cmd);
        assertTrue( result.isSuccess() );
    }
}