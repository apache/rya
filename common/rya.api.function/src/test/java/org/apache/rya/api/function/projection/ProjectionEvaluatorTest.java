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
package org.apache.rya.api.function.projection;

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Unit tests the methods of {@link ProjectionEvaluator}.
 */
public class ProjectionEvaluatorTest {

    /**
     * This Projection enumerates all of the variables that were in the query, none of them are anonymous, and
     * none of them insert constants.
     */
    @Test
    public void changesNothing() throws Exception {
        // Read the projection object from a SPARQL query.
        final Projection projection = getProjection(
                "SELECT ?person ?employee ?business " +
                "WHERE { " +
                    "?person <urn:talksTo> ?employee . " +
                    "?employee <urn:worksAt> ?business . " +
                "}");

        // Create a Binding Set that contains the result of the WHERE clause.
        final ValueFactory vf = new ValueFactoryImpl();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:TacoJoint"));
        final VisibilityBindingSet original = new VisibilityBindingSet(bs, "a|b");

        // Execute the projection.
        final VisibilityBindingSet result = ProjectionEvaluator.make(projection).project(original);
        assertEquals(original, result);
    }

    /**
     * This Projection replaces some of the variables names with different names.
     */
    @Test
    public void renameBindings() throws Exception {
        // Read the projection object from a SPARQL query.
        final Projection projection = getProjection(
                "SELECT (?person AS ?p) (?employee AS ?e) ?business " +
                "WHERE { " +
                    "?person <urn:talksTo> ?employee . " +
                    "?employee <urn:worksAt> ?business . " +
                "}");

        // Create a Binding Set that contains the result of the WHERE clause.
        final ValueFactory vf = new ValueFactoryImpl();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:TacoJoint"));
        final VisibilityBindingSet original = new VisibilityBindingSet(bs, "a|b");

        // The expected binding set changes the "person" binding name to "p" and "employee" to "e".
        bs = new MapBindingSet();
        bs.addBinding("p", vf.createURI("urn:Alice"));
        bs.addBinding("e", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:TacoJoint"));
        final VisibilityBindingSet expected = new VisibilityBindingSet(bs, "a|b");

        // Execute the projection.
        final VisibilityBindingSet result = ProjectionEvaluator.make(projection).project(original);
        assertEquals(expected, result);
    }

    /**
     * This projection drops a binding from the original Binding Set.
     */
    @Test
    public void dropsBinding() throws Exception {
        // Read the projection object from a SPARQL query.
        final Projection projection = getProjection(
                "SELECT ?person " +
                "WHERE { " +
                    "?person <urn:talksTo> ?employee . " +
                    "?employee <urn:worksAt> ?business . " +
                "}");

        // Create a Binding Set that contains the result of the WHERE clause.
        final ValueFactory vf = new ValueFactoryImpl();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("employee", vf.createURI("urn:Bob"));
        bs.addBinding("business", vf.createURI("urn:TacoJoint"));
        final VisibilityBindingSet original = new VisibilityBindingSet(bs, "a|b");

        // The expected binding set only has the "person" binding.
        bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        final VisibilityBindingSet expected = new VisibilityBindingSet(bs, "a|b");

        // Execute the projection.
        final VisibilityBindingSet result = ProjectionEvaluator.make(projection).project(original);
        assertEquals(expected, result);
    }

    /**
     * This projection creates a Binding Set that represents a Statement and add a constant value to it.
     */
    @Test
    public void addsConstantBinding() throws Exception {
        // Read the projection object from a SPARQL query.
        final Projection projection = getProjection(
                "CONSTRUCT { ?person <urn:hasGrandchild> ?grandchild } " +
                "WHERE {" +
                    "?person <urn:hasChild> ?child ." +
                    "?child <urn:hasChild> ?grandchild . " +
                 "}");

        // Create a Binding Set that contains the result of the WHERE clause.
        final ValueFactory vf = new ValueFactoryImpl();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("child", vf.createURI("urn:Bob"));
        bs.addBinding("grandchild", vf.createURI("urn:Charlie"));
        final VisibilityBindingSet original = new VisibilityBindingSet(bs, "a|b");

        // The expected binding set represents a statement.
        bs = new MapBindingSet();
        bs.addBinding("subject", vf.createURI("urn:Alice"));
        bs.addBinding("predicate", vf.createURI("urn:hasGrandchild"));
        bs.addBinding("object", vf.createURI("urn:Charlie"));
        final VisibilityBindingSet expected = new VisibilityBindingSet(bs, "a|b");

        // Execute the projection.
        final VisibilityBindingSet result = ProjectionEvaluator.make(projection).project(original);
        assertEquals(expected, result);
    }

    /**
     * This projection creates a Binding Set that represents a Statement that has a blank node added to it.
     */
    @Test
    public void addsBlankNodeBinding() throws Exception {
        // Read the projection object from a SPARQL query.
        final Projection projection = getProjection(
                "CONSTRUCT { ?person <urn:hasChild> _:b } " +
                "WHERE {" +
                    "?person <urn:hasGrandchild> ?grandchild ." +
                 "}");

        // Create a Binding Set that contains the result of the WHERE clause.
        final ValueFactory vf = new ValueFactoryImpl();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("person", vf.createURI("urn:Alice"));
        bs.addBinding("hasGrandchild", vf.createURI("urn:Bob"));
        final VisibilityBindingSet original = new VisibilityBindingSet(bs, "a|b");

        // Execute the projection.
        final VisibilityBindingSet result = ProjectionEvaluator.make(projection).project(original);

        // The expected binding set represents a statement. We need to get the blank node's id from the
        // result since that is different every time.
        bs = new MapBindingSet();
        bs.addBinding("subject", vf.createURI("urn:Alice"));
        bs.addBinding("predicate", vf.createURI("urn:hasChild"));
        bs.addBinding("object", result.getValue("object"));
        final VisibilityBindingSet expected = new VisibilityBindingSet(bs, "a|b");

        assertEquals(expected, result);
    }

    /**
     * Get the first {@link Projection} node from a SPARQL query.
     *
     * @param sparql - The query that contains a single Projection node.
     * @return The first {@link Projection} that is encountered.
     * @throws Exception The query could not be parsed.
     */
    public static @Nullable Projection getProjection(final String sparql) throws Exception {
        requireNonNull(sparql);

        final AtomicReference<Projection> projection = new AtomicReference<>();
        final ParsedQuery parsed = new SPARQLParser().parseQuery(sparql, null);
        parsed.getTupleExpr().visit(new QueryModelVisitorBase<Exception>() {
            @Override
            public void meet(final Projection node) throws Exception {
                projection.set(node);
            }
        });

        return projection.get();
    }
}