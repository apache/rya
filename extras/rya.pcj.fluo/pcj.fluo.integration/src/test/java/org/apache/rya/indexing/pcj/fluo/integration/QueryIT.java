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

import java.util.HashSet;
import java.util.Set;

import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;
import org.openrdf.model.impl.NumericLiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.BindingImpl;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import mvm.rya.api.domain.RyaStatement;

/**
 * Performs integration tests over the Fluo application geared towards various query structures.
 * <p>
 * These tests are being ignore so that they will not run as unit tests while building the application.
 */
public class QueryIT extends ITBase {

    @Test
    public void optionalStatements() throws Exception {
        // A query that has optional statement patterns. This query is looking for all
        // people who have Law degrees and any BAR exams they have passed (though they
        // do not have to have passed any).
        final String sparql =
                "SELECT ?person ?exam " +
                "WHERE {" +
                    "?person <http://hasDegreeIn> <http://Law> . " +
                    "OPTIONAL {?person <http://passedExam> ?exam } . " +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                makeRyaStatement("http://Alice", "http://hasDegreeIn", "http://Computer Science"),
                makeRyaStatement("http://Alice", "http://passedExam", "http://Certified Ethical Hacker"),
                makeRyaStatement("http://Bob", "http://hasDegreeIn", "http://Law"),
                makeRyaStatement("http://Bob", "http://passedExam", "http://MBE"),
                makeRyaStatement("http://Bob", "http://passedExam", "http://BAR-Kansas"),
                makeRyaStatement("http://Charlie", "http://hasDegreeIn", "http://Law"));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add( makeBindingSet(
                new BindingImpl("person", new URIImpl("http://Bob")),
                new BindingImpl("exam", new URIImpl("http://MBE"))));
        expected.add( makeBindingSet(
                new BindingImpl("person", new URIImpl("http://Bob")),
                new BindingImpl("exam", new URIImpl("http://BAR-Kansas"))));
        expected.add( makeBindingSet(
                new BindingImpl("person", new URIImpl("http://Charlie"))));

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, new HashSet<VariableOrder>(), sparql);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected,  results);
    }

    /**
     * Tests when there are a bunch of variables across a bunch of joins.
     */
    @Test
    public void complexQuery() throws Exception {
        // A query that find people who live in the USA, have been recruited by Geek Squad,
        // and are skilled with computers. The resulting binding set includes everybody who
        // was involved in the recruitment process.
        final String sparql =
                "SELECT ?recruiter ?candidate ?leader " +
                "{ " +
                  "?recruiter <http://recruiterFor> <http://GeekSquad>. " +
                  "?candidate <http://skilledWith> <http://Computers>. " +
                  "?candidate <http://livesIn> \"USA\". " +
                  "?leader <http://leaderOf> <http://GeekSquad>. " +
                  "?recruiter <http://talksTo> ?candidate. " +
                  "?candidate <http://talksTo> ?leader. " +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                // Leaders
                makeRyaStatement("http://Alice", "http://leaderOf", "http://GeekSquad"),
                makeRyaStatement("http://Bob", "http://leaderOf", "http://GeekSquad"),

                // Recruiters
                makeRyaStatement("http://Charlie", "http://recruiterFor", "http://GeekSquad"),
                makeRyaStatement("http://David", "http://recruiterFor", "http://GeekSquad"),

                // Candidates
                makeRyaStatement("http://Eve", "http://skilledWith", "http://Computers"),
                makeRyaStatement("http://Eve", "http://livesIn", "USA"),
                makeRyaStatement("http://Frank", "http://skilledWith", "http://Computers"),
                makeRyaStatement("http://Frank", "http://livesIn", "USA"),
                makeRyaStatement("http://George", "http://skilledWith", "http://Computers"),
                makeRyaStatement("http://George", "http://livesIn", "Germany"),
                makeRyaStatement("http://Harry", "http://skilledWith", "http://Negotiating"),
                makeRyaStatement("http://Harry", "http://livesIn", "USA"),
                makeRyaStatement("http://Ivan", "http://skilledWith", "http://Computers"),
                makeRyaStatement("http://Ivan", "http://livesIn", "USA"),

                // Candidates the recruiters talk to.
                makeRyaStatement("http://Charlie", "http://talksTo", "http://Eve"),
                makeRyaStatement("http://Charlie", "http://talksTo", "http://George"),
                makeRyaStatement("http://Charlie", "http://talksTo", "http://Harry"),
                makeRyaStatement("http://David", "http://talksTo", "http://Eve"),
                makeRyaStatement("http://David", "http://talksTo", "http://Frank"),
                makeRyaStatement("http://David", "http://talksTo", "http://Ivan"),

                // Recruits that talk to leaders.
                makeRyaStatement("http://Eve", "http://talksTo", "http://Alice"),
                makeRyaStatement("http://George", "http://talksTo", "http://Alice"),
                makeRyaStatement("http://Harry", "http://talksTo", "http://Bob"),
                makeRyaStatement("http://Ivan", "http://talksTo", "http://Bob"));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add( makeBindingSet(
                new BindingImpl("recruiter", new URIImpl("http://Charlie")),
                new BindingImpl("candidate", new URIImpl("http://Eve")),
                new BindingImpl("leader", new URIImpl("http://Alice"))));
        expected.add( makeBindingSet(
                new BindingImpl("recruiter", new URIImpl("http://David")),
                new BindingImpl("candidate", new URIImpl("http://Eve")),
                new BindingImpl("leader", new URIImpl("http://Alice"))));
        expected.add( makeBindingSet(
                new BindingImpl("recruiter", new URIImpl("http://David")),
                new BindingImpl("candidate", new URIImpl("http://Ivan")),
                new BindingImpl("leader", new URIImpl("http://Bob"))));

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, new HashSet<VariableOrder>(), sparql);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected,  results);
    }

    @Test
    public void withURIFilters() throws Exception {
        final String sparql =
                "SELECT ?customer ?worker ?city " +
                "{ " +
                  "FILTER(?customer = <http://Alice>) " +
                  "FILTER(?city = <http://London>) " +
                  "?customer <http://talksTo> ?worker. " +
                  "?worker <http://livesIn> ?city. " +
                  "?worker <http://worksAt> <http://Chipotle>. " +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                makeRyaStatement("http://Alice", "http://talksTo", "http://Bob"),
                makeRyaStatement("http://Bob", "http://livesIn", "http://London"),
                makeRyaStatement("http://Bob", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Alice", "http://talksTo", "http://Charlie"),
                makeRyaStatement("http://Charlie", "http://livesIn", "http://London"),
                makeRyaStatement("http://Charlie", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Alice", "http://talksTo", "http://David"),
                makeRyaStatement("http://David", "http://livesIn", "http://London"),
                makeRyaStatement("http://David", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Alice", "http://talksTo", "http://Eve"),
                makeRyaStatement("http://Eve", "http://livesIn", "http://Leeds"),
                makeRyaStatement("http://Eve", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Frank", "http://talksTo", "http://Alice"),
                makeRyaStatement("http://Frank", "http://livesIn", "http://London"),
                makeRyaStatement("http://Frank", "http://worksAt", "http://Chipotle"));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Bob")),
                new BindingImpl("city", new URIImpl("http://London"))));
        expected.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Charlie")),
                new BindingImpl("city", new URIImpl("http://London"))));
        expected.add( makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://David")),
                new BindingImpl("city", new URIImpl("http://London"))));

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, new HashSet<VariableOrder>(), sparql);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected,  results);
    }

    @Test
    public void withNumericFilters() throws Exception {
        final String sparql =
                "SELECT ?name ?age " +
                "{" +
                  "FILTER(?age < 30) ." +
                  "?name <http://hasAge> ?age." +
                  "?name <http://playsSport> \"Soccer\" " +
                "}";

        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                makeRyaStatement("http://Alice", "http://hasAge", 18),
                makeRyaStatement("http://Bob", "http://hasAge", 30),
                makeRyaStatement("http://Charlie", "http://hasAge", 14),
                makeRyaStatement("http://David", "http://hasAge", 16),
                makeRyaStatement("http://Eve", "http://hasAge", 35),

                makeRyaStatement("http://Alice", "http://playsSport", "Soccer"),
                makeRyaStatement("http://Bob", "http://playsSport", "Soccer"),
                makeRyaStatement("http://Charlie", "http://playsSport", "Basketball"),
                makeRyaStatement("http://Charlie", "http://playsSport", "Soccer"),
                makeRyaStatement("http://David", "http://playsSport", "Basketball"));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add( makeBindingSet(
                new BindingImpl("name", new URIImpl("http://Alice")),
                new BindingImpl("age", new NumericLiteralImpl(18, XMLSchema.INTEGER))));
        expected.add( makeBindingSet(
                new BindingImpl("name", new URIImpl("http://Charlie")),
                new BindingImpl("age", new NumericLiteralImpl(14, XMLSchema.INTEGER))));

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, new HashSet<VariableOrder>(), sparql);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected,  results);
    }
}