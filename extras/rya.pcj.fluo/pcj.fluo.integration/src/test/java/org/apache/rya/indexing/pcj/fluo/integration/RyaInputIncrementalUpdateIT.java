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
package org.apache.rya.indexing.pcj.fluo.integration;

import static org.junit.Assert.assertEquals;
import io.fluo.api.client.FluoClient;

import java.util.HashSet;
import java.util.Set;

import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.indexing.external.PrecomputedJoinIndexer;

import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.update.PrecomputedJoinUpdater;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.repository.RepositoryConnection;

import com.google.common.collect.Sets;


/**
 * This test ensures that the correct updates are pushed by Fluo
 * to the external PCJ table as triples are added to Rya through
 * the {@link RepositoryConnection}.  The key difference between these
 * tests and those in {@link InputIT} is that streaming triples are added through
 * the RepositoryConnection and not through the {@link FluoClient}.  These tests are
 * designed to verify that the {@link AccumuloRyaDAO} has been integrated
 * with the {@link PrecomputedJoinIndexer} and that the associated {@link PrecomputedJoinUpdater} updates
 * Fluo accordingly.
 *
 */

public class RyaInputIncrementalUpdateIT extends ITBase {

    /**
     * Ensure historic matches are included in the result.
     */
    @Test
    public void streamResultsThroughRya() throws Exception {

        // A query that finds people who talk to Eve and work at Chipotle.
        final String sparql = "SELECT ?x " + "WHERE { " + "?x <http://talksTo> <http://Eve>. "
                + "?x <http://worksAt> <http://Chipotle>." + "}";

        // Triples that are loaded into Rya before the PCJ is created.
        final Set<Statement> historicTriples = Sets.newHashSet(
                makeStatement("http://Alice", "http://talksTo", "http://Eve"),
                makeStatement("http://Bob", "http://talksTo", "http://Eve"),
                makeStatement("http://Charlie", "http://talksTo", "http://Eve"),

                makeStatement("http://Eve", "http://helps", "http://Kevin"),

                makeStatement("http://Bob", "http://worksAt", "http://Chipotle"),
                makeStatement("http://Charlie", "http://worksAt", "http://Chipotle"),
                makeStatement("http://Eve", "http://worksAt", "http://Chipotle"),
                makeStatement("http://David", "http://worksAt", "http://Chipotle"));

        // The expected results of the SPARQL query once the PCJ has been
        // computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(new BindingImpl("x", new URIImpl("http://Bob"))));
        expected.add(makeBindingSet(new BindingImpl("x", new URIImpl("http://Charlie"))));

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn,
                new HashSet<VariableOrder>(), sparql);

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();

        // Load the historic data into Rya.
        for (final Statement triple : historicTriples) {
            ryaConn.add(triple);
        }

        fluo.waitForObservers();

        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected, results);
    }

    /**
     * Simulates the case where a Triple is added to Rya, a new query that
     * includes that triple as a historic match is inserted into Fluo, and then
     * some new triple that matches the query is streamed into Fluo. The query's
     * results must include both the historic result and the newly streamed
     * result.
     */
    @Test
    public void historicThenStreamedResults() throws Exception {
        // A query that finds people who talk to Eve and work at Chipotle.
        final String sparql = "SELECT ?x " + "WHERE { " + "?x <http://talksTo> <http://Eve>. "
                + "?x <http://worksAt> <http://Chipotle>." + "}";

        // Triples that are loaded into Rya before the PCJ is created.
        final Set<Statement> historicTriples = Sets.newHashSet(
                makeStatement("http://Alice", "http://talksTo", "http://Eve"),
                makeStatement("http://Alice", "http://worksAt", "http://Chipotle"),
                makeStatement("http://Joe", "http://worksAt", "http://Chipotle"));

        // Triples that will be streamed into Fluo after the PCJ has been
        final Set<Statement> streamedTriples = Sets.newHashSet(
                makeStatement("http://Frank", "http://talksTo", "http://Eve"),
                makeStatement("http://Joe", "http://talksTo", "http://Eve"),
                makeStatement("http://Frank", "http://worksAt", "http://Chipotle"));

        // Load the historic data into Rya.
        for (final Statement triple : historicTriples) {
            ryaConn.add(triple);
        }

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn,
                new HashSet<VariableOrder>(), sparql);
        fluo.waitForObservers();

        // Load the streaming data into Rya.
        for (final Statement triple : streamedTriples) {
            ryaConn.add(triple);
        }

        // Ensure Alice is a match.
        fluo.waitForObservers();
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(new BindingImpl("x", new URIImpl("http://Alice"))));
        expected.add(makeBindingSet(new BindingImpl("x", new URIImpl("http://Frank"))));
        expected.add(makeBindingSet(new BindingImpl("x", new URIImpl("http://Joe"))));

        Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected, results);
    }

    @Test
    public void historicAndStreamMultiVariables() throws Exception {
        // A query that finds people who talk to Eve and work at Chipotle.
        // A query that finds people who talk to Eve and work at Chipotle.
        final String sparql = "SELECT ?x ?y " + "WHERE { " + "?x <http://talksTo> ?y. "
                + "?x <http://worksAt> <http://Chipotle>." + "}";

        // Triples that are loaded into Rya before the PCJ is created.
        final Set<Statement> historicTriples = Sets.newHashSet(
                makeStatement("http://Alice", "http://talksTo", "http://Eve"),
                makeStatement("http://Alice", "http://worksAt", "http://Chipotle"),
                makeStatement("http://Joe", "http://worksAt", "http://Chipotle"));

        // Triples that will be streamed into Fluo after the PCJ has been
        final Set<Statement> streamedTriples = Sets.newHashSet(
                makeStatement("http://Frank", "http://talksTo", "http://Betty"),
                makeStatement("http://Joe", "http://talksTo", "http://Alice"),
                makeStatement("http://Frank", "http://worksAt", "http://Chipotle"));

        // Load the historic data into Rya.
        for (final Statement triple : historicTriples) {
            ryaConn.add(triple);
        }

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn,
                new HashSet<VariableOrder>(), sparql);
        fluo.waitForObservers();

        // Load the streaming data into Rya.
        for (final Statement triple : streamedTriples) {
            ryaConn.add(triple);
        }

        // Ensure Alice is a match.
        fluo.waitForObservers();
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(new BindingImpl("x", new URIImpl("http://Alice")), new BindingImpl("y", new URIImpl("http://Eve"))));
        expected.add(makeBindingSet(new BindingImpl("x", new URIImpl("http://Frank")), new BindingImpl("y", new URIImpl("http://Betty"))));
        expected.add(makeBindingSet(new BindingImpl("x", new URIImpl("http://Joe")), new BindingImpl("y", new URIImpl("http://Alice"))));

        Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected, results);
    }
}