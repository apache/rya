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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import com.google.common.base.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.rya.api.client.ExecuteSparqlQuery;
import org.apache.rya.api.client.LoadStatementsFile;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.shell.util.ConsolePrinter;
import org.apache.rya.shell.util.SparqlPrompt;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Unit tests the methods of {@link RyaAdminCommands}.
 */
public class RyaCommandsTest {

    @Test
    public void testLoadData() throws RyaClientException, IOException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";
        final String statementsFile = "/path/to/statements.nt";
        final String format = null;

        final LoadStatementsFile mockLoadStatementsFile = mock(LoadStatementsFile.class);
        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getLoadStatementsFile()).thenReturn(mockLoadStatementsFile);

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        final SparqlPrompt mockSparqlPrompt = mock(SparqlPrompt.class);

        final ConsolePrinter mockConsolePrinter = mock(ConsolePrinter.class);

        // Execute the command.
        final RyaCommands commands = new RyaCommands(state, mockSparqlPrompt, mockConsolePrinter);
        final String message = commands.loadData(statementsFile, format);

        // Verify the values that were provided to the command were passed through to LoadStatementsFile.
        verify(mockLoadStatementsFile).loadStatements(instanceName, Paths.get(statementsFile), RDFFormat.NTRIPLES);

        // Verify a message is returned that explains what was created.
        assertTrue(message.startsWith("Loaded the file: '" + statementsFile +"' successfully in "));
        assertTrue(message.endsWith(" seconds."));
    }

    @Test
    public void testLoadData_specifyFormat() throws RyaClientException, IOException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";
        final String statementsFile = "/path/to/statements.nt";
        final String format = "N-TRIPLES";

        final LoadStatementsFile mockLoadStatementsFile = mock(LoadStatementsFile.class);
        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getLoadStatementsFile()).thenReturn(mockLoadStatementsFile);

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        final SparqlPrompt mockSparqlPrompt = mock(SparqlPrompt.class);

        final ConsolePrinter mockConsolePrinter = mock(ConsolePrinter.class);

        // Execute the command.
        final RyaCommands commands = new RyaCommands(state, mockSparqlPrompt, mockConsolePrinter);
        final String message = commands.loadData(statementsFile, format);

        // Verify the values that were provided to the command were passed through to LoadStatementsFile.
        verify(mockLoadStatementsFile).loadStatements(instanceName, Paths.get(statementsFile), RDFFormat.NTRIPLES);

        // Verify a message is returned that explains what was created.
        assertTrue(message.startsWith("Loaded the file: '" + statementsFile +"' successfully in "));
        assertTrue(message.endsWith(" seconds."));
    }

    @Test(expected = RuntimeException.class)
    public void testLoadData_specifyInvalidFormat() throws RyaClientException, IOException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";
        final String statementsFile = "/path/to/statements.nt";
        final String format = "INVALID_FORMAT_NAME";

        final LoadStatementsFile mockLoadStatementsFile = mock(LoadStatementsFile.class);
        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getLoadStatementsFile()).thenReturn(mockLoadStatementsFile);

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        final SparqlPrompt mockSparqlPrompt = mock(SparqlPrompt.class);

        final ConsolePrinter mockConsolePrinter = mock(ConsolePrinter.class);

        // Execute the command.
        final RyaCommands commands = new RyaCommands(state, mockSparqlPrompt, mockConsolePrinter);

        commands.loadData(statementsFile, format);
    }

    @Test(expected = RuntimeException.class)
    public void testLoadData_specifyInvalidFilenameFormat() throws RyaClientException, IOException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";
        final String statementsFile = "/path/to/statements.invalidFormat";
        final String format = null;

        final LoadStatementsFile mockLoadStatementsFile = mock(LoadStatementsFile.class);
        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getLoadStatementsFile()).thenReturn(mockLoadStatementsFile);

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        final SparqlPrompt mockSparqlPrompt = mock(SparqlPrompt.class);

        final ConsolePrinter mockConsolePrinter = mock(ConsolePrinter.class);

        // Execute the command.
        final RyaCommands commands = new RyaCommands(state, mockSparqlPrompt, mockConsolePrinter);

        commands.loadData(statementsFile, format);
    }

    @Test
    public void testSparqlQuery() throws RyaClientException, IOException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";
        final String queryFile = "src/test/resources/Query1.sparql";
        final String queryContent = FileUtils.readFileToString(new File(queryFile), StandardCharsets.UTF_8);
        final String expectedMessage = "MockAnswer";

        final ExecuteSparqlQuery mockExecuteSparqlQuery = mock(ExecuteSparqlQuery.class);
        when(mockExecuteSparqlQuery.executeSparqlQuery(instanceName, queryContent)).thenReturn(expectedMessage);

        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getExecuteSparqlQuery()).thenReturn(mockExecuteSparqlQuery);


        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        final SparqlPrompt mockSparqlPrompt = mock(SparqlPrompt.class);

        final ConsolePrinter mockConsolePrinter = mock(ConsolePrinter.class);

        // Execute the command.
        final RyaCommands commands = new RyaCommands(state, mockSparqlPrompt, mockConsolePrinter);
        final String message = commands.sparqlQuery(queryFile);

        // Verify the values that were provided to the command were passed through to LoadStatementsFile.
        verify(mockExecuteSparqlQuery).executeSparqlQuery(instanceName, queryContent);

        assertEquals(expectedMessage, message);
        // Verify a message is returned that explains what was created.
    }

    @Test(expected = RuntimeException.class)
    public void testSparqlQuery_nonexistentFile() throws RyaClientException, IOException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";
        final String queryFile = "src/test/resources/Nonexistent.sparql";

        final RyaClient mockCommands = mock(RyaClient.class);

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        final SparqlPrompt mockSparqlPrompt = mock(SparqlPrompt.class);

        final ConsolePrinter mockConsolePrinter = mock(ConsolePrinter.class);

        // Execute the command.
        final RyaCommands commands = new RyaCommands(state, mockSparqlPrompt, mockConsolePrinter);
        commands.sparqlQuery(queryFile);
    }

    @Test
    public void testSparqlQuery_fromPrompt() throws RyaClientException, IOException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";
        final String queryContent = "SELECT * WHERE { ?person <http://isA> ?noun }";
        final String queryFile = null;
        final String expectedMessage = "MockAnswer";

        final ExecuteSparqlQuery mockExecuteSparqlQuery = mock(ExecuteSparqlQuery.class);
        when(mockExecuteSparqlQuery.executeSparqlQuery(instanceName, queryContent)).thenReturn(expectedMessage);

        final RyaClient mockCommands = mock(RyaClient.class);
        when(mockCommands.getExecuteSparqlQuery()).thenReturn(mockExecuteSparqlQuery);


        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        final SparqlPrompt mockSparqlPrompt = mock(SparqlPrompt.class);
        when(mockSparqlPrompt.getSparql()).thenReturn(Optional.of(queryContent));

        final ConsolePrinter mockConsolePrinter = mock(ConsolePrinter.class);

        // Execute the command.
        final RyaCommands commands = new RyaCommands(state, mockSparqlPrompt, mockConsolePrinter);
        final String message = commands.sparqlQuery(queryFile);

        // Verify the values that were provided to the command were passed through to LoadStatementsFile.
        verify(mockExecuteSparqlQuery).executeSparqlQuery(instanceName, queryContent);

        assertEquals(expectedMessage, message);
        // Verify a message is returned that explains what was created.
    }

    @Test
    public void testSparqlQuery_fromPrompt_cancelled() throws RyaClientException, IOException {
        // Mock the object that performs the create operation.
        final String instanceName = "unitTest";
        final String queryFile = null;
        final String expectedMessage = "";

        final RyaClient mockCommands = mock(RyaClient.class);

        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance(instanceName);

        final SparqlPrompt mockSparqlPrompt = mock(SparqlPrompt.class);
        when(mockSparqlPrompt.getSparql()).thenReturn(Optional.absent());

        final ConsolePrinter mockConsolePrinter = mock(ConsolePrinter.class);

        // Execute the command.
        final RyaCommands commands = new RyaCommands(state, mockSparqlPrompt, mockConsolePrinter);
        final String message = commands.sparqlQuery(queryFile);

        assertEquals(expectedMessage, message);
        // Verify a message is returned that explains what was created.
    }

}
