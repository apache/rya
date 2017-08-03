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

import org.junit.Test;

import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;

/**
 * Tests the methods of {@link RyaPromptProvider}.
 */
public class RyaPromptProviderTest {

    @Test
    public void notConnected() {
        // Create a shared state that is disconnected.
        final SharedShellState sharedState = new SharedShellState();
        sharedState.disconnected();

        // Create the prompt.
        final String prompt = new RyaPromptProvider(sharedState).getPrompt();

        // Verify the prompt is formatted correctly.
        final String expected = "rya> ";
        assertEquals(expected, prompt);
    }

    @Test
    public void isConnected_noInstanceName() {
        // Create a shared state that is connected to a storage, but not a rya instance.
        final SharedShellState sharedState = new SharedShellState();

        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails("", new char[]{}, "testInstance", "");
        sharedState.connectedToAccumulo(connectionDetails, mock(RyaClient.class));

        // Create a prompt.
        final String prompt = new RyaPromptProvider(sharedState).getPrompt();

        // Verify the prompt is formatted correctly.
        final String expected = "rya/testInstance> ";
        assertEquals(expected, prompt);
    }

    @Test
    public void isConnected_hasInstanceName() {
        // Create a shared state that is connected to a specific instance.
        final SharedShellState sharedState = new SharedShellState();

        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails("", new char[]{}, "testInstance", "");
        sharedState.connectedToAccumulo(connectionDetails, mock(RyaClient.class));
        sharedState.connectedToInstance("testRya");

        // Create a prompt.
        final String prompt = new RyaPromptProvider(sharedState).getPrompt();

        // Verify the prompt is formatted correctly.
        final String expected = "rya/testInstance:testRya> ";
        assertEquals(expected, prompt);
    }
}