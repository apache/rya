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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.junit.Test;
import org.openrdf.model.BNode;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Unit tests the methods of {@link MultiProjectionEvaluator}.
 */
public class MultiProjectionEvaluatorTest {

    @Test
    public void singleBlankNode() throws Exception {
        // Read the multi projection object from a SPARQL query.
        final MultiProjection multiProjection = getMultiProjection(
                "CONSTRUCT {" +
                    "_:b a <urn:movementObservation> ; " +
                    "<urn:location> ?location ; " +
                    "<urn:direction> ?direction ; " +
                "}" +
                "WHERE {" +
                    "?thing <urn:corner> ?location ." +
                    "?thing <urn:compass> ?direction." +
                "}");

        // Create a Binding Set that contains the result of the WHERE clause.
        final ValueFactory vf = new ValueFactoryImpl();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("location", vf.createLiteral("South St and 5th St"));
        bs.addBinding("direction", vf.createLiteral("NW"));
        final VisibilityBindingSet original = new VisibilityBindingSet(bs, "a|b");

        // Create the expected results.
        final Set<VisibilityBindingSet> expected = new HashSet<>();
        final String blankNodeId = UUID.randomUUID().toString();
        final BNode blankNode = vf.createBNode(blankNodeId);

        bs = new MapBindingSet();
        bs.addBinding("subject", blankNode);
        bs.addBinding("predicate", RDF.TYPE);
        bs.addBinding("object", vf.createURI("urn:movementObservation"));
        expected.add( new VisibilityBindingSet(bs, "a|b") );

        bs = new MapBindingSet();
        bs.addBinding("subject", blankNode);
        bs.addBinding("predicate", vf.createURI("urn:location"));
        bs.addBinding("object", vf.createLiteral("South St and 5th St"));
        expected.add( new VisibilityBindingSet(bs, "a|b") );

        bs = new MapBindingSet();
        bs.addBinding("subject", blankNode);
        bs.addBinding("predicate", vf.createURI("urn:direction"));
        bs.addBinding("object", vf.createLiteral("NW"));
        expected.add( new VisibilityBindingSet(bs, "a|b") );

        // Run the projection evaluator.
        final Set<VisibilityBindingSet> results = MultiProjectionEvaluator.make(multiProjection, () -> blankNodeId).project(original);

        // The expected binding sets.
        assertEquals(expected, results);
    }

    @Test
    public void multipleBlanknodes() throws Exception {
        // Read the multi projection object from a SPARQL query.
        final MultiProjection multiProjection = getMultiProjection(
                "CONSTRUCT {" +
                    "_:b a <urn:vehicle> . " +
                    "_:b <urn:tiresCount> 4 ." +
                    "_:c a <urn:pet> . " +
                    "_:c <urn:isDead> false . " +
                "}" +
                "WHERE {" +
                    "?vehicle <urn:owner> ?owner . " +
                    "?vehicle <urn:plates> ?plates . " +
                    "?pet <urn:owner> ?owner . " +
                    "?pet <urn:isLiving> true . " +
                "}");

        // Create a Binding Set that contains the result of the WHERE clause.
        final ValueFactory vf = new ValueFactoryImpl();
        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("vehicle", vf.createLiteral("Alice's car"));
        bs.addBinding("owner", vf.createURI("urn:Alice"));
        bs.addBinding("plates", vf.createLiteral("XXXXXXX"));
        bs.addBinding("pet", vf.createURI("urn:Kitty"));
        final VisibilityBindingSet original = new VisibilityBindingSet(bs, "a|b");

        // Run the projection evaluator.
        final Set<VisibilityBindingSet> results = MultiProjectionEvaluator.make(multiProjection, new RandomUUIDFactory()).project(original);

        // Figure out the blank nodes.
        Value vehicalBNode = null;
        Value petBNode = null;
        for(final VisibilityBindingSet result : results) {
            final Value object = result.getValue("object");
            if(object.equals(vf.createURI("urn:vehicle"))) {
                vehicalBNode = result.getValue("subject");
            } else if(object.equals(vf.createURI("urn:pet"))) {
                petBNode = result.getValue("subject");
            }
        }

        // The expected binding sets.
        final Set<VisibilityBindingSet> expected = new HashSet<>();

        bs = new MapBindingSet();
        bs.addBinding("subject", vehicalBNode);
        bs.addBinding("predicate", RDF.TYPE);
        bs.addBinding("object", vf.createURI("urn:vehicle"));
        expected.add( new VisibilityBindingSet(bs, "a|b") );

        bs = new MapBindingSet();
        bs.addBinding("subject", vehicalBNode);
        bs.addBinding("predicate", vf.createURI("urn:tiresCount"));
        bs.addBinding("object", vf.createLiteral("4", XMLSchema.INTEGER));
        expected.add( new VisibilityBindingSet(bs, "a|b") );

        bs = new MapBindingSet();
        bs.addBinding("subject", petBNode);
        bs.addBinding("predicate", RDF.TYPE);
        bs.addBinding("object", vf.createURI("urn:pet"));
        expected.add( new VisibilityBindingSet(bs, "a|b") );

        bs = new MapBindingSet();
        bs.addBinding("subject", petBNode);
        bs.addBinding("predicate", vf.createURI("urn:isDead"));
        bs.addBinding("object", vf.createLiteral(false));
        expected.add( new VisibilityBindingSet(bs, "a|b") );

        assertEquals(expected, results);
    }

    /**
     * Get the first {@link MultiProjection} node from a SPARQL query.
     *
     * @param sparql - The query that contains a single Projection node.
     * @return The first {@link MultiProjection} that is encountered.
     * @throws Exception The query could not be parsed.
     */
    public static @Nullable MultiProjection getMultiProjection(final String sparql) throws Exception {
        requireNonNull(sparql);

        final AtomicReference<MultiProjection> multiProjection = new AtomicReference<>();
        final ParsedQuery parsed = new SPARQLParser().parseQuery(sparql, null);
        parsed.getTupleExpr().visit(new QueryModelVisitorBase<Exception>() {
            @Override
            public void meet(final MultiProjection node) throws Exception {
                multiProjection.set(node);
            }
        });

        return multiProjection.get();
    }
}