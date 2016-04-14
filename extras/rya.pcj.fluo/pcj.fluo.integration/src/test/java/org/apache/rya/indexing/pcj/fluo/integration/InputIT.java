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
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.BindingImpl;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import mvm.rya.api.domain.RyaStatement;

/**
 * Performs integration tests over the Fluo application geared towards various types of input.
 * <p>
 * These tests are being ignore so that they will not run as unit tests while building the application.
 */
public class InputIT extends ITBase {

    /**
     * Ensure historic matches are included in the result.
     */
    @Test
    public void historicResults() throws Exception {
        // A query that finds people who talk to Eve and work at Chipotle.
        final String sparql =
              "SELECT ?x " +
                "WHERE { " +
                "?x <http://talksTo> <http://Eve>. " +
                "?x <http://worksAt> <http://Chipotle>." +
              "}";

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

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(
                new BindingImpl("x", new URIImpl("http://Bob"))));
        expected.add(makeBindingSet(
                new BindingImpl("x", new URIImpl("http://Charlie"))));

        // Load the historic data into Rya.
        for(final Statement triple : historicTriples) {
            ryaConn.add(triple);
        }

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, new HashSet<VariableOrder>(), sparql);

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();

        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected, results);
    }

    /**
     * Ensure streamed matches are included in the result.
     */
    @Test
    public void streamedResults() throws Exception {
        // A query that finds people who talk to Eve and work at Chipotle.
        final String sparql =
              "SELECT ?x " +
                "WHERE { " +
                "?x <http://talksTo> <http://Eve>. " +
                "?x <http://worksAt> <http://Chipotle>." +
              "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                makeRyaStatement("http://Alice", "http://talksTo", "http://Eve"),
                makeRyaStatement("http://Bob", "http://talksTo", "http://Eve"),
                makeRyaStatement("http://Charlie", "http://talksTo", "http://Eve"),

                makeRyaStatement("http://Eve", "http://helps", "http://Kevin"),

                makeRyaStatement("http://Bob", "http://worksAt", "http://Chipotle"),
                makeRyaStatement("http://Charlie", "http://worksAt", "http://Chipotle"),
                makeRyaStatement("http://Eve", "http://worksAt", "http://Chipotle"),
                makeRyaStatement("http://David", "http://worksAt", "http://Chipotle"));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(
                new BindingImpl("x", new URIImpl("http://Bob"))));
        expected.add(makeBindingSet(
                new BindingImpl("x", new URIImpl("http://Charlie"))));

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, new HashSet<VariableOrder>(), sparql);

        // Ensure the query has no results yet.
        fluo.waitForObservers();
        Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertTrue( results.isEmpty() );

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected, results);
    }

    /**
     * Simulates the case where a Triple is added to Rya, a new query that includes
     * that triple as a historic match is inserted into Fluo, and then some new
     * triple that matches the query is streamed into Fluo. The query's results
     * must include both the historic result and the newly streamed result.
     */
    @Test
    public void historicThenStreamedResults() throws Exception {
        // A query that finds people who talk to Eve and work at Chipotle.
        final String sparql =
              "SELECT ?x " +
                "WHERE { " +
                "?x <http://talksTo> <http://Eve>. " +
                "?x <http://worksAt> <http://Chipotle>." +
              "}";

        // Triples that are loaded into Rya before the PCJ is created.
        final Set<Statement> historicTriples = Sets.newHashSet(
                makeStatement("http://Alice", "http://talksTo", "http://Eve"),
                makeStatement("http://Alice", "http://worksAt", "http://Chipotle"));

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                makeRyaStatement("http://Frank", "http://talksTo", "http://Eve"),
                makeRyaStatement("http://Frank", "http://worksAt", "http://Chipotle"));

        // Load the historic data into Rya.
        for(final Statement triple : historicTriples) {
            ryaConn.add(triple);
        }

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, new HashSet<VariableOrder>(), sparql);

        // Ensure Alice is a match.
        fluo.waitForObservers();
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(
                new BindingImpl("x", new URIImpl("http://Alice"))));

        Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected, results);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Verify the end results of the query also include Frank.
        fluo.waitForObservers();
        expected.add(makeBindingSet(
                new BindingImpl("x", new URIImpl("http://Frank"))));

        results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected, results);
    }

    /**
     * Simulates the case where a Triple is added to Rya, a new query that
     * includes the triple as a historic match is inserted into Fluo, and then
     * the same triple is streamed into Fluo. The query's results will already
     * include the Triple because they were added while the query was being
     * created. This case should not fail or effect the end results in any way.
     */
    @Test
    public void historicAndStreamConflict() throws Exception {
        // A query that finds people who talk to Eve and work at Chipotle.
        final String sparql =
              "SELECT ?x " +
                "WHERE { " +
                "?x <http://talksTo> <http://Eve>. " +
                "?x <http://worksAt> <http://Chipotle>." +
              "}";

        // Triples that are loaded into Rya before the PCJ is created.
        final Set<Statement> historicTriples = Sets.newHashSet(
                makeStatement("http://Alice", "http://talksTo", "http://Eve"),
                makeStatement("http://Alice", "http://worksAt", "http://Chipotle"));

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                makeRyaStatement("http://Alice", "http://talksTo", "http://Eve"),
                makeRyaStatement("http://Alice", "http://worksAt", "http://Chipotle"));

        // The expected final result.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(
                new BindingImpl("x", new URIImpl("http://Alice"))));

        // Load the historic data into Rya.
        for(final Statement triple : historicTriples) {
            ryaConn.add(triple);
        }

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, new HashSet<VariableOrder>(), sparql);

        // Ensure Alice is a match.
        fluo.waitForObservers();
        Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected, results);

        // Stream the same Alice triple into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Verify the end results of the query is stiill only Alice.
        fluo.waitForObservers();
        results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected, results);
    }
}