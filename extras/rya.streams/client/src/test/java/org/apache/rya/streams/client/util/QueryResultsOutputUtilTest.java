/**
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
package org.apache.rya.streams.client.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.api.entity.QueryResultStream;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Test;

import com.google.common.base.Charsets;

/**
 * Unit tests the methods of {@link QueryResultsOutputUtil}.
 */
public class QueryResultsOutputUtilTest {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Test
    public void toNtriplesFile() throws Exception {
        // Mock a result stream that signals shutdown when it returns a set of results.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);
        final QueryResultStream<VisibilityStatement> resultsStream = mock(QueryResultStream.class);
        when(resultsStream.poll(anyLong())).thenAnswer(invocation -> {
            shutdownSignal.set(true);

            final List<VisibilityStatement> results = new ArrayList<>();

            Statement stmt = VF.createStatement(VF.createIRI("urn:alice"), VF.createIRI("urn:age"), VF.createLiteral(23));
            results.add( new VisibilityStatement(stmt) );

            stmt = VF.createStatement(VF.createIRI("urn:bob"), VF.createIRI("urn:worksAt"), VF.createLiteral("Taco Shop"));
            results.add( new VisibilityStatement(stmt) );
            return results;
        });

        // The stream the JSON will be written to.
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        // Invoke the test method. This will write the NTriples.
        QueryResultsOutputUtil.toNtriplesFile(out, resultsStream, shutdownSignal);

        // Show the produced NTriples matches the expected NTriples.
        final String expected =
                "<urn:alice> <urn:age> \"23\"^^<http://www.w3.org/2001/XMLSchema#int> .\n" +
                "<urn:bob> <urn:worksAt> \"Taco Shop\" .\n";

        final String nTriples = new String(out.toByteArray(), Charsets.UTF_8);
        assertEquals(expected, nTriples);
    }

    @Test
    public void toBindingSetJSONFile() throws Exception {
        // A SPARQL query that uses OPTIONAL values.
        final String sparql =
                "SELECT * WHERE { " +
                    "?name <urn:worksAt> ?company . " +
                    "OPTIONAL{ ?name <urn:ssn> ?ssn} " +
                "}";
        final TupleExpr query = new SPARQLParser().parseQuery(sparql, null).getTupleExpr();

        // Mock a results stream that signals shutdown when it returns a set of results.
        final AtomicBoolean shutdownSignal = new AtomicBoolean(false);
        final QueryResultStream<VisibilityBindingSet> resultsStream = mock(QueryResultStream.class);
        when(resultsStream.poll(anyLong())).thenAnswer(invocation -> {
            shutdownSignal.set(true);

            final List<VisibilityBindingSet> results = new ArrayList<>();

            // A result with the optional value.
            MapBindingSet bs = new MapBindingSet();
            bs.addBinding("name", VF.createLiteral("alice"));
            bs.addBinding("company", VF.createLiteral("Taco Shop"));
            bs.addBinding("ssn", VF.createIRI("urn:111-11-1111"));
            results.add(new VisibilityBindingSet(bs, ""));


            // A result without the optional value.
            bs = new MapBindingSet();
            bs.addBinding("name", VF.createLiteral("bob"));
            bs.addBinding("company", VF.createLiteral("Cafe"));
            results.add(new VisibilityBindingSet(bs, ""));

            return results;
        });

        // The stream the JSON will be written to.
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        // Invoke the test method. This will write the json.
        QueryResultsOutputUtil.toBindingSetJSONFile(out, query, resultsStream, shutdownSignal);

        // Show the produced JSON matches the expected JSON.
        final String expected =  "{\"head\":{\"vars\":[\"name\",\"company\",\"ssn\"]},\"results\":{" +
                "\"bindings\":[{\"name\":{\"type\":\"literal\",\"value\":\"alice\"},\"company\":{" +
                "\"type\":\"literal\",\"value\":\"Taco Shop\"},\"ssn\":{\"type\":\"uri\",\"value\":" +
                "\"urn:111-11-1111\"}},{\"name\":{\"type\":\"literal\",\"value\":\"bob\"},\"company\"" +
                ":{\"type\":\"literal\",\"value\":\"Cafe\"}}]}}";
        final String json = new String(out.toByteArray(), Charsets.UTF_8);
        System.out.println(json);
        assertEquals(expected, json);
    }
}