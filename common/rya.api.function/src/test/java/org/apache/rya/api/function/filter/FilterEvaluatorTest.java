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
package org.apache.rya.api.function.filter;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.Filter;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Test;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Unit tests the methods of {@link FilterEvaluator}.
 */
public class FilterEvaluatorTest {

    @Test
    public void matches() throws Exception {
        // Read the filter object from a SPARQL query.
        final Filter filter = getFilter(
                "SELECT * " +
                "WHERE { " +
                    "FILTER(?age < 10)" +
                    "?person <urn:age> ?age " +
                "}");

        // Create the input binding set.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("age", vf.createLiteral(9));
        final VisibilityBindingSet visBs = new VisibilityBindingSet(bs);

        // Test the evaluator.
        assertTrue( FilterEvaluator.make(filter).filter(visBs) );
    }

    @Test
    public void doesNotMatch() throws Exception {
        // Read the filter object from a SPARQL query.
        final Filter filter = getFilter(
                "SELECT * " +
                "WHERE { " +
                    "FILTER(?age < 10)" +
                    "?person <urn:age> ?age " +
                "}");

        // Create the input binding set.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createIRI("urn:Alice"));
        bs.addBinding("age", vf.createLiteral(11));
        final VisibilityBindingSet visBs = new VisibilityBindingSet(bs);

        // Test the evaluator.
        assertFalse( FilterEvaluator.make(filter).filter(visBs) );
    }

    /**
     * Get the first {@link Filter} node from a SPARQL query.
     *
     * @param sparql - The query that contains a single Projection node.
     * @return The first {@link Filter} that is encountered.
     * @throws Exception The query could not be parsed.
     */
    public static @Nullable Filter getFilter(final String sparql) throws Exception {
        requireNonNull(sparql);

        final AtomicReference<Filter> filter = new AtomicReference<>();
        final ParsedQuery parsed = new SPARQLParser().parseQuery(sparql, null);
        parsed.getTupleExpr().visit(new AbstractQueryModelVisitor<Exception>() {
            @Override
            public void meet(final Filter node) throws Exception {
                filter.set(node);
            }
        });

        return filter.get();
    }
}