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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.UUID;

import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.SetRyaStreamsConfiguration;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.RyaStreamsDetails;
import org.apache.rya.shell.util.ConsolePrinter;
import org.apache.rya.shell.util.SparqlPrompt;
import org.apache.rya.streams.api.RyaStreamsClient;
import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.interactor.AddQuery;
import org.apache.rya.streams.api.interactor.DeleteQuery;
import org.apache.rya.streams.api.interactor.GetQuery;
import org.apache.rya.streams.api.interactor.ListQueries;
import org.apache.rya.streams.api.interactor.StartQuery;
import org.apache.rya.streams.api.interactor.StopQuery;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

/**
 * Unit tests the methods of {@link RyaStreamsCommands}.
 */
public class RyaStreamsCommandsTest {

    @Test
    public void configureRyaStreams() throws Exception {
        // Mock the object that performs the configure operation.
        final RyaClient mockCommands = mock(RyaClient.class);
        final SetRyaStreamsConfiguration setStreams = mock(SetRyaStreamsConfiguration.class);
        when(mockCommands.getSetRyaStreamsConfiguration()).thenReturn(setStreams);

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance("unitTest");

        // Verify that no Rya Streams Client is set to the state.
        assertFalse(state.getShellState().getRyaStreamsCommands().isPresent());

        try {
            // Execute the command.
            final RyaStreamsCommands commands = new RyaStreamsCommands(state, mock(SparqlPrompt.class), mock(ConsolePrinter.class));
            final String message = commands.configureRyaStreams("localhost", 6);

            // Verify the request was forwarded to the mocked interactor.
            final RyaStreamsDetails expectedDetails = new RyaStreamsDetails("localhost", 6);
            verify(setStreams).setRyaStreamsConfiguration(eq("unitTest"), eq(expectedDetails));

            // Verify a RyaStreamsClient was created and added to the state.
            assertTrue(state.getShellState().getRyaStreamsCommands().isPresent());

            // Verify the correct message is reported.
            final String expected = "The Rya Instance has been updated to use the provided Rya Streams subsystem. " +
                    "Rya Streams commands are now avaiable while connected to this instance.";
            assertEquals(expected, message);
        } finally {
            state.getShellState().getRyaStreamsCommands().get().close();
        }
    }

    @Test
    public void printRyaStreamsDetails_noRyaDetails() throws Exception {
        // Mock the object that performs the configure operation.
        final RyaClient mockCommands = mock(RyaClient.class);
        final GetInstanceDetails getDetails = mock(GetInstanceDetails.class);
        when(mockCommands.getGetInstanceDetails()).thenReturn(getDetails);

        // When getting the instance details, ensure they are not found.
        when(getDetails.getDetails(eq("unitTest"))).thenReturn(Optional.absent());

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance("unitTest");

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, mock(SparqlPrompt.class), mock(ConsolePrinter.class));
        final String message = commands.printRyaStreamsDetails();
        final String expected = "This instance does not have any Rya Details, so it is unable to be connected to the Rya Streams subsystem.";
        assertEquals(expected, message);
    }

    @Test
    public void printRyaStreamsDetails_notConfigured() throws Exception {
        // Mock the object that performs the configure operation.
        final RyaClient mockCommands = mock(RyaClient.class);
        final GetInstanceDetails getDetails = mock(GetInstanceDetails.class);
        when(mockCommands.getGetInstanceDetails()).thenReturn(getDetails);

        // When getting the instance details, ensure they do not have RyaStreamsDetails to print.
        final RyaDetails details = mock(RyaDetails.class);
        when(details.getRyaStreamsDetails()).thenReturn(Optional.absent());
        when(getDetails.getDetails(eq("unitTest"))).thenReturn(Optional.of(details));

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance("unitTest");

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, mock(SparqlPrompt.class), mock(ConsolePrinter.class));
        final String message = commands.printRyaStreamsDetails();
        final String expected = "This instance of Rya has not been configured to use a Rya Streams subsystem.";
        assertEquals(expected, message);
    }

    @Test
    public void printRyaStreamsDetails_configured() throws Exception {
        // Mock the object that performs the configure operation.
        final RyaClient mockCommands = mock(RyaClient.class);
        final GetInstanceDetails getDetails = mock(GetInstanceDetails.class);
        when(mockCommands.getGetInstanceDetails()).thenReturn(getDetails);

        // When getting the instance details, ensure they do have RyaStreamsDetails to print.
        final RyaDetails details = mock(RyaDetails.class);
        when(details.getRyaStreamsDetails()).thenReturn(Optional.of(new RyaStreamsDetails("localhost", 6)));
        when(getDetails.getDetails(eq("unitTest"))).thenReturn(Optional.of(details));

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mockCommands);
        state.connectedToInstance("unitTest");

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, mock(SparqlPrompt.class), mock(ConsolePrinter.class));
        final String message = commands.printRyaStreamsDetails();
        final String expected = "Kafka Hostname: localhost, Kafka Port: 6";
        assertEquals(expected, message);
    }

    @Test
    public void addQuery_userAbortsSparqlPrompt() throws Exception {
        // Mock the object that performs the rya streams operation.
        final RyaStreamsClient mockClient = mock(RyaStreamsClient.class);
        final AddQuery addQuery = mock(AddQuery.class);
        when(mockClient.getAddQuery()).thenReturn(addQuery);

        // Mock a SPARQL prompt that a user aborts.
        final SparqlPrompt prompt = mock(SparqlPrompt.class);
        when(prompt.getSparql()).thenReturn(Optional.absent());

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mock(RyaClient.class));
        state.connectedToInstance("unitTest");
        state.connectedToRyaStreams(mockClient);

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, prompt, mock(ConsolePrinter.class));
        final String message = commands.addQuery(false, false);

        // Verify a message is printed to the user.
        assertEquals("", message);
    }

    @Test
    public void addQuery_doNotInsertQuery() throws Exception {
        // Mock the object that performs the rya streams operation.
        final RyaStreamsClient mockClient = mock(RyaStreamsClient.class);
        final AddQuery addQuery = mock(AddQuery.class);
        when(mockClient.getAddQuery()).thenReturn(addQuery);

        final String sparql = "SELECT * WHERE { ?a ?b ?c }";

        final StreamsQuery addedQuery = new StreamsQuery(UUID.randomUUID(), sparql, true, false);
        when(addQuery.addQuery(eq(sparql), eq(true), eq(false))).thenReturn(addedQuery);

        // Mock a SPARQL prompt that a user entered a query through.
        final SparqlPrompt prompt = mock(SparqlPrompt.class);
        when(prompt.getSparql()).thenReturn(Optional.of(sparql));

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mock(RyaClient.class));
        state.connectedToInstance("unitTest");
        state.connectedToRyaStreams(mockClient);

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, prompt, mock(ConsolePrinter.class));
        final String message = commands.addQuery(false, false);

        // Verify the interactor was invoked with the provided input.
        verify(addQuery).addQuery(sparql, true, false);

        // Verify a message is printed to the user.
        final String expected = "The added query's ID is " + addedQuery.getQueryId();
        assertEquals(expected, message);
    }

    @Test
    public void addQuery_insertConstructQuery() throws Exception {
        // Mock the object that performs the rya streams operation.
        final RyaStreamsClient mockClient = mock(RyaStreamsClient.class);
        final AddQuery addQuery = mock(AddQuery.class);
        when(mockClient.getAddQuery()).thenReturn(addQuery);

        final String sparql =
                "PREFIX vCard: <http://www.w3.org/2001/vcard-rdf/3.0#> " +
                "PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
                "CONSTRUCT { " +
                    "?X vCard:FN ?name . " +
                    "?X vCard:URL ?url . " +
                    "?X vCard:TITLE ?title . " +
                "} " +
                "FROM <http://www.w3.org/People/Berners-Lee/card> " +
                "WHERE { " +
                    "OPTIONAL { ?X foaf:name ?name . FILTER isLiteral(?name) . } " +
                    "OPTIONAL { ?X foaf:homepage ?url . FILTER isURI(?url) . } " +
                    "OPTIONAL { ?X foaf:title ?title . FILTER isLiteral(?title) . } " +
                "}";

        final StreamsQuery addedQuery = new StreamsQuery(UUID.randomUUID(), sparql, true, true);
        when(addQuery.addQuery(eq(sparql), eq(false), eq(true))).thenReturn(addedQuery);

        // Mock a SPARQL prompt that a user entered a query through.
        final SparqlPrompt prompt = mock(SparqlPrompt.class);
        when(prompt.getSparql()).thenReturn(Optional.of(sparql));

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mock(RyaClient.class));
        state.connectedToInstance("unitTest");
        state.connectedToRyaStreams(mockClient);

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, prompt, mock(ConsolePrinter.class));
        final String message = commands.addQuery(true, true);

        // Verify the interactor was invoked with the provided input.
        verify(addQuery).addQuery(sparql, false, true);

        // Verify a message is printed to the user.
        final String expected = "The added query's ID is " + addedQuery.getQueryId();
        assertEquals(expected, message);
    }

    @Test
    public void addQuery_doNotInsertInsertUpdate() throws Exception {
        // Mock the object that performs the rya streams operation.
        final RyaStreamsClient mockClient = mock(RyaStreamsClient.class);
        final AddQuery addQuery = mock(AddQuery.class);
        when(mockClient.getAddQuery()).thenReturn(addQuery);

        final String sparql =
                "PREFIX Sensor: <http://example.com/Equipment.owl#> " +
                "INSERT { " +
                    "?subject Sensor:test2 ?newValue " +
                "} WHERE {" +
                    "values (?oldValue ?newValue) {" +
                        "('testValue1' 'newValue1')" +
                        "('testValue2' 'newValue2')" +
                    "}" +
                    "?subject Sensor:test1 ?oldValue" +
                "}";

        final StreamsQuery addedQuery = new StreamsQuery(UUID.randomUUID(), sparql, true, false);
        when(addQuery.addQuery(eq(sparql), eq(false), eq(false))).thenReturn(addedQuery);

        // Mock a SPARQL prompt that a user entered a query through.
        final SparqlPrompt prompt = mock(SparqlPrompt.class);
        when(prompt.getSparql()).thenReturn(Optional.of(sparql));

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mock(RyaClient.class));
        state.connectedToInstance("unitTest");
        state.connectedToRyaStreams(mockClient);

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, prompt, mock(ConsolePrinter.class));
        final String message = commands.addQuery(true, false);

        // Verify the interactor was invoked with the provided input.
        verify(addQuery).addQuery(sparql, false, false);

        // Verify a message is printed to the user.
        final String expected = "The added query's ID is " + addedQuery.getQueryId();
        assertEquals(expected, message);
    }

    @Test(expected = RuntimeException.class)
    public void addQuery_insertQueryNotCorrectType() throws Exception {
        // Mock the object that performs the rya streams operation.
        final RyaStreamsClient mockClient = mock(RyaStreamsClient.class);
        final AddQuery addQuery = mock(AddQuery.class);
        when(mockClient.getAddQuery()).thenReturn(addQuery);

        final String sparql = "SELECT * WHERE { ?a ?b ?c }";

        final StreamsQuery addedQuery = new StreamsQuery(UUID.randomUUID(), sparql, true, true);
        when(addQuery.addQuery(eq(sparql), eq(false), eq(true))).thenReturn(addedQuery);

        // Mock a SPARQL prompt that a user entered a query through.
        final SparqlPrompt prompt = mock(SparqlPrompt.class);
        when(prompt.getSparql()).thenReturn(Optional.of(sparql));

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mock(RyaClient.class));
        state.connectedToInstance("unitTest");
        state.connectedToRyaStreams(mockClient);

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, prompt, mock(ConsolePrinter.class));
        commands.addQuery(true, true);
    }

    @Test
    public void deleteQuery() throws Exception {
        // Mock the object that performs the rya streams operation.
        final RyaStreamsClient mockClient = mock(RyaStreamsClient.class);
        final DeleteQuery deleteQuery = mock(DeleteQuery.class);
        when(mockClient.getDeleteQuery()).thenReturn(deleteQuery);

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mock(RyaClient.class));
        state.connectedToInstance("unitTest");
        state.connectedToRyaStreams(mockClient);

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, mock(SparqlPrompt.class), mock(ConsolePrinter.class));
        final UUID queryId = UUID.randomUUID();
        final String message = commands.deleteQuery(queryId.toString());

        // Verify the interactor was invoked with the provided parameters.
        verify(deleteQuery).delete(eq(queryId));

        // Verify a message is printed to the user.
        final String expected = "The query has been deleted.";
        assertEquals(expected, message);
    }

    @Test
    public void startQuery() throws Exception {
        // Mock the object that performs the rya streams operation.
        final RyaStreamsClient mockClient = mock(RyaStreamsClient.class);
        final StartQuery startQuery = mock(StartQuery.class);
        when(mockClient.getStartQuery()).thenReturn(startQuery);
        final GetQuery getQuery = mock(GetQuery.class);
        when(mockClient.getGetQuery()).thenReturn(getQuery);

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mock(RyaClient.class));
        state.connectedToInstance("unitTest");
        state.connectedToRyaStreams(mockClient);

        // Report the query as not running.
        final UUID queryId = UUID.randomUUID();
        when(getQuery.getQuery(eq(queryId))).thenReturn(java.util.Optional.of(new StreamsQuery(queryId, "sparql", false, false)));

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, mock(SparqlPrompt.class), mock(ConsolePrinter.class));
        final String message = commands.startQuery(queryId.toString());

        // Verify the interactor was invoked with the provided parameters.
        verify(startQuery).start(queryId);

        // Verify a message is printed to the user.
        final String expected = "The query will be processed by the Rya Streams subsystem.";
        assertEquals(expected, message);
    }

    @Test
    public void startQuery_alreadyRunning() throws Exception{
        // Mock the object that performs the rya streams operation.
        final RyaStreamsClient mockClient = mock(RyaStreamsClient.class);
        final StartQuery startQuery = mock(StartQuery.class);
        when(mockClient.getStartQuery()).thenReturn(startQuery);
        final GetQuery getQuery = mock(GetQuery.class);
        when(mockClient.getGetQuery()).thenReturn(getQuery);

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mock(RyaClient.class));
        state.connectedToInstance("unitTest");
        state.connectedToRyaStreams(mockClient);

        // Report the query as running.
        final UUID queryId = UUID.randomUUID();
        when(getQuery.getQuery(eq(queryId))).thenReturn(java.util.Optional.of(new StreamsQuery(queryId, "sparql", true, false)));

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, mock(SparqlPrompt.class), mock(ConsolePrinter.class));
        final String message = commands.startQuery(queryId.toString());

        // Verify the interactor was not invoked.
        verify(startQuery, never()).start(queryId);

        // Verify a message is printed to the user.
        final String expected = "That query is already running.";
        assertEquals(expected, message);
    }

    @Test
    public void stopQuery() throws Exception {
        // Mock the object that performs the rya streams operation.
        final RyaStreamsClient mockClient = mock(RyaStreamsClient.class);
        final StopQuery stopQuery = mock(StopQuery.class);
        when(mockClient.getStopQuery()).thenReturn(stopQuery);
        final GetQuery getQuery = mock(GetQuery.class);
        when(mockClient.getGetQuery()).thenReturn(getQuery);

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mock(RyaClient.class));
        state.connectedToInstance("unitTest");
        state.connectedToRyaStreams(mockClient);

        // Report the query as running.
        final UUID queryId = UUID.randomUUID();
        when(getQuery.getQuery(eq(queryId))).thenReturn(java.util.Optional.of(new StreamsQuery(queryId, "sparql", true, false)));

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, mock(SparqlPrompt.class), mock(ConsolePrinter.class));
        final String message = commands.stopQuery(queryId.toString());

        // Verify the interactor was invoked with the provided parameters.
        verify(stopQuery).stop(queryId);

        // Verify a message is printed to the user.
        final String expected = "The query will no longer be processed by the Rya Streams subsystem.";
        assertEquals(expected, message);
    }

    @Test
    public void stopQuery_alreadyStopped() throws Exception {
        // Mock the object that performs the rya streams operation.
        final RyaStreamsClient mockClient = mock(RyaStreamsClient.class);
        final StopQuery stopQuery = mock(StopQuery.class);
        when(mockClient.getStopQuery()).thenReturn(stopQuery);
        final GetQuery getQuery = mock(GetQuery.class);
        when(mockClient.getGetQuery()).thenReturn(getQuery);

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mock(RyaClient.class));
        state.connectedToInstance("unitTest");
        state.connectedToRyaStreams(mockClient);

        // Report the query as not running.
        final UUID queryId = UUID.randomUUID();
        when(getQuery.getQuery(eq(queryId))).thenReturn(java.util.Optional.of(new StreamsQuery(queryId, "sparql", false, false)));

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, mock(SparqlPrompt.class), mock(ConsolePrinter.class));
        final String message = commands.stopQuery(queryId.toString());

        // Verify the interactor was not invoked with the provided parameters.
        verify(stopQuery, never()).stop(queryId);

        // Verify a message is printed to the user.
        final String expected = "That query is already stopped.";
        assertEquals(expected, message);
    }

    @Test
    public void listQueries() throws Exception {
        // Mock the object that performs the rya streams operation.
        final RyaStreamsClient mockClient = mock(RyaStreamsClient.class);
        final ListQueries listQueries = mock(ListQueries.class);
        when(mockClient.getListQueries()).thenReturn(listQueries);

        final Set<StreamsQuery> queries = Sets.newHashSet(
                new StreamsQuery(
                        UUID.fromString("33333333-3333-3333-3333-333333333333"),
                        "SELECT * WHERE { ?person <urn:worksAt> ?business . }",
                        true, false),
                new StreamsQuery(
                        UUID.fromString("11111111-1111-1111-1111-111111111111"),
                        "SELECT * WHERE { ?a ?b ?c . }",
                        true, false),
                new StreamsQuery(
                        UUID.fromString("22222222-2222-2222-2222-222222222222"),
                        "SELECT * WHERE { ?d ?e ?f . }",
                        false, false));
        when(listQueries.all()).thenReturn(queries);

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mock(RyaClient.class));
        state.connectedToInstance("unitTest");
        state.connectedToRyaStreams(mockClient);

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, mock(SparqlPrompt.class), mock(ConsolePrinter.class));
        final String message = commands.listQueries();

        // Verify the correct report is returned.
        final String expected =
                "-----------------------------------------------\n" +
                " Query ID: 11111111-1111-1111-1111-111111111111\n" +
                "Is Active: true\n" +
                "Is Insert: false\n" +
                "   SPARQL: select ?a ?b ?c\n" +
                "           where {\n" +
                "             ?a ?b ?c.\n" +
                "           }\n" +
                "-----------------------------------------------\n" +
                " Query ID: 22222222-2222-2222-2222-222222222222\n" +
                "Is Active: false\n" +
                "Is Insert: false\n" +
                "   SPARQL: select ?d ?e ?f\n" +
                "           where {\n" +
                "             ?d ?e ?f.\n" +
                "           }\n" +
                "-----------------------------------------------\n" +
                " Query ID: 33333333-3333-3333-3333-333333333333\n" +
                "Is Active: true\n" +
                "Is Insert: false\n" +
                "   SPARQL: select ?person ?business\n" +
                "           where {\n" +
                "             ?person <urn:worksAt> ?business.\n" +
                "           }\n" +
                "-----------------------------------------------\n";
        assertEquals(expected, message);
    }

    @Test
    public void printQueryDetails() throws Exception {
        // Mock the object that performs the rya streams operation.
        final RyaStreamsClient mockClient = mock(RyaStreamsClient.class);
        final GetQuery getQuery = mock(GetQuery.class);
        when(mockClient.getGetQuery()).thenReturn(getQuery);

        final UUID queryId = UUID.fromString("da55cea5-c21c-46a5-ab79-5433eef4efaa");
        final StreamsQuery query = new StreamsQuery(queryId, "SELECT * WHERE { ?a ?b ?c . }", true, false);
        when(getQuery.getQuery(queryId)).thenReturn(java.util.Optional.of(query));

        // Mock a shell state and connect it to a Rya instance.
        final SharedShellState state = new SharedShellState();
        state.connectedToAccumulo(mock(AccumuloConnectionDetails.class), mock(RyaClient.class));
        state.connectedToInstance("unitTest");
        state.connectedToRyaStreams(mockClient);

        // Execute the command.
        final RyaStreamsCommands commands = new RyaStreamsCommands(state, mock(SparqlPrompt.class), mock(ConsolePrinter.class));
        final String message = commands.printQueryDetails(queryId.toString());

        // Verify the correct report is returned.
        final String expected =
                " Query ID: da55cea5-c21c-46a5-ab79-5433eef4efaa\n" +
                "Is Active: true\n" +
                "Is Insert: false\n" +
                "   SPARQL: select ?a ?b ?c\n" +
                "           where {\n" +
                "             ?a ?b ?c.\n" +
                "           }\n";
        assertEquals(expected, message);
    }
}