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
package org.apache.rya.api.function.sp;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Test;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Unit tests the methods of {@link StatementPatternMatcher}.
 */
public class StatementPatternMatcherTest {

    @Test
    public void matchesSubject() throws Exception {
        // Create the matcher against a pattern that matches a specific subject.
        final StatementPatternMatcher matcher = new StatementPatternMatcher(getSp(
                "SELECT * WHERE {" +
                    "<urn:Alice> ?p ?o ." +
                "}"));

        // Create a statement that matches the pattern.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Statement statement = vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"), vf.createIRI("urn:testGraph"));

        // Create the expected resulting Binding Set.
        final QueryBindingSet expected = new QueryBindingSet();
        expected.addBinding("p", vf.createIRI("urn:talksTo"));
        expected.addBinding("o", vf.createIRI("urn:Bob"));

        // Show the expected Binding Set matches the resulting Binding Set.
        final Optional<BindingSet> bs = matcher.match(statement);
        assertEquals(expected, bs.get());
    }

    @Test
    public void doesNotMatchSubject() throws Exception {
        // Create the matcher against a pattern that matches a specific subject.
        final StatementPatternMatcher matcher = new StatementPatternMatcher(getSp(
                "SELECT * WHERE {" +
                    "<urn:Alice> ?p ?o ." +
                "}"));

        // Create a statement that does not match the pattern.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Statement statement = vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"), vf.createIRI("urn:testGraph"));

        // Show the statement did not match.
        final Optional<BindingSet> bs = matcher.match(statement);
        assertFalse(bs.isPresent());
    }

    @Test
    public void matchesPredicate() throws Exception {
        // Create the matcher against a pattern that matches a specific predicate.
        final StatementPatternMatcher matcher = new StatementPatternMatcher(getSp(
                "SELECT * WHERE {" +
                    "?s <urn:talksTo> ?o ." +
                "}"));

        // Create a statement that matches the pattern.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Statement statement = vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"), vf.createIRI("urn:testGraph"));

        // Create the expected resulting Binding Set.
        final QueryBindingSet expected = new QueryBindingSet();
        expected.addBinding("s", vf.createIRI("urn:Alice"));
        expected.addBinding("o", vf.createIRI("urn:Bob"));

        // Show the expected Binding Set matches the resulting Binding Set.
        final Optional<BindingSet> bs = matcher.match(statement);
        assertEquals(expected, bs.get());
    }

    @Test
    public void doesNotMatchPredicate() throws Exception {
        // Create the matcher against a pattern that matches a specific predicate.
        final StatementPatternMatcher matcher = new StatementPatternMatcher(getSp(
                "SELECT * WHERE {" +
                    "?s <urn:talksTo> ?o ." +
                "}"));

        // Create a statement that does not match the pattern.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Statement statement = vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:knows"), vf.createIRI("urn:Bob"), vf.createIRI("urn:testGraph"));

        // Show the statement did not match.
        final Optional<BindingSet> bs = matcher.match(statement);
        assertFalse(bs.isPresent());
    }

    @Test
    public void matchesObject() throws Exception {
        // Create the matcher against a pattern that matches a specific object.
        final StatementPatternMatcher matcher = new StatementPatternMatcher(getSp(
                "SELECT * WHERE {" +
                    "?s ?p <urn:Bob> ." +
                "}"));

        // Create a statement that matches the pattern.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Statement statement = vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"), vf.createIRI("urn:testGraph"));

        // Create the expected resulting Binding Set.
        final QueryBindingSet expected = new QueryBindingSet();
        expected.addBinding("s", vf.createIRI("urn:Alice"));
        expected.addBinding("p", vf.createIRI("urn:talksTo"));

        // Show the expected Binding Set matches the resulting Binding Set.
        final Optional<BindingSet> bs = matcher.match(statement);
        assertEquals(expected, bs.get());
    }

    @Test
    public void doesNotMatchObject() throws Exception {
        // Create the matcher against a pattern that matches a specific object.
        final StatementPatternMatcher matcher = new StatementPatternMatcher(getSp(
                "SELECT * WHERE {" +
                    "?s ?p <urn:Bob> ." +
                "}"));

        // Create a statement that does not match the pattern.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Statement statement = vf.createStatement(vf.createIRI("urn:Charlie"), vf.createIRI("urn:knows"), vf.createIRI("urn:Alice"), vf.createIRI("urn:testGraph"));

        // Show the statement did not match.
        final Optional<BindingSet> bs = matcher.match(statement);
        assertFalse(bs.isPresent());
    }

    @Test
    public void matchesContext() throws Exception {
        // Create a matcher against a pattern that matches a specific context.
        final StatementPatternMatcher matcher = new StatementPatternMatcher(getSp(
                "SELECT * WHERE {" +
                    "GRAPH <urn:testGraph> {" +
                        "?s ?p ?o ." +
                    "}" +
                "}"));

        // Create a statement that matches the pattern.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Statement statement = vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"), vf.createIRI("urn:testGraph"));

        // Create the expected resulting Binding Set.
        final QueryBindingSet expected = new QueryBindingSet();
        expected.addBinding("s", vf.createIRI("urn:Alice"));
        expected.addBinding("p", vf.createIRI("urn:talksTo"));
        expected.addBinding("o", vf.createIRI("urn:Bob"));

        // Show the expected Binding Set matches the resulting Binding Set.
        final Optional<BindingSet> bs = matcher.match(statement);
        assertEquals(expected, bs.get());
    }

    @Test
    public void doesNotMatchContext() throws Exception {
        // Create a matcher against a pattern that matches a specific context.
        final StatementPatternMatcher matcher = new StatementPatternMatcher(getSp(
                "SELECT * WHERE {" +
                    "GRAPH <urn:testGraph> {" +
                        "?s ?p ?o ." +
                    "}" +
                "}"));

        // Create a statement that does not match the pattern.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Statement statement = vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"), vf.createIRI("urn:wrong"));

        // Show the statement did not match.
        final Optional<BindingSet> bs = matcher.match(statement);
        assertFalse(bs.isPresent());
    }

    @Test
    public void variableContext() throws Exception {
        // Create a matcher against a pattern that matches a variable context.
        final StatementPatternMatcher matcher = new StatementPatternMatcher(getSp(
                "SELECT * WHERE {" +
                    "GRAPH ?c {" +
                        "?s ?p ?o ." +
                    "}" +
                "}"));

        // Create a statement that matches the pattern.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Statement statement = vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"), vf.createIRI("urn:testGraph"));

        // Create the expected resulting Binding Set.
        final QueryBindingSet expected = new QueryBindingSet();
        expected.addBinding("s", vf.createIRI("urn:Alice"));
        expected.addBinding("p", vf.createIRI("urn:talksTo"));
        expected.addBinding("o", vf.createIRI("urn:Bob"));
        expected.addBinding("c", vf.createIRI("urn:testGraph"));

        // Show the expected Binding Set matches the resulting Binding Set.
        final Optional<BindingSet> bs = matcher.match(statement);
        assertEquals(expected, bs.get());
    }

    @Test
    public void variableContext_contextFreeStatement() throws Exception {
        // Create a matcher against a pattern that matches a variable context.
        final StatementPatternMatcher matcher = new StatementPatternMatcher(getSp(
                "SELECT * WHERE {" +
                    "GRAPH ?c {" +
                        "?s ?p ?o ." +
                    "}" +
                "}"));

        // Create a statement that does not have a context value.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Statement statement = vf.createStatement(vf.createIRI("urn:Alice"), vf.createIRI("urn:talksTo"), vf.createIRI("urn:Bob"));

        // Show the statement did not match.
        final Optional<BindingSet> bs = matcher.match(statement);
        assertFalse(bs.isPresent());
    }

    /**
     * Fetch the {@link StatementPattern} from a SPARQL string.
     *
     * @param sparql - A SPARQL query that contains only a single Statement Patern. (not nul)
     * @return The {@link StatementPattern} that was in the query, if it could be found. Otherwise {@code null}
     * @throws Exception The statement pattern could not be found in the parsed SPARQL query.
     */
    public static @Nullable StatementPattern getSp(final String sparql) throws Exception {
        requireNonNull(sparql);

        final AtomicReference<StatementPattern> statementPattern = new AtomicReference<>();
        final ParsedQuery parsed = new SPARQLParser().parseQuery(sparql, null);
        parsed.getTupleExpr().visitChildren(new AbstractQueryModelVisitor<Exception>() {
            @Override
            public void meet(final StatementPattern node) throws Exception {
                statementPattern.set(node);
            }
        });
        return statementPattern.get();
    }
}