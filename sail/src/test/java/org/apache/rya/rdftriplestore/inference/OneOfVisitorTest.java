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
package org.apache.rya.rdftriplestore.inference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Set;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Sets;

/**
 * Tests the methods of {@link OneOfVisitor}.
 */
public class OneOfVisitorTest {
    private final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
    private static final ValueFactory VF = new ValueFactoryImpl();

    private static final URI SUITS = VF.createURI("urn:Suits");
    private static final URI RANKS = VF.createURI("urn:Ranks");

    // Definition #1: :Suits owl:oneOf(:Clubs, :Diamonds, :Hearts, :Spades)
    private static final URI CLUBS = VF.createURI("urn:Clubs");
    private static final URI DIAMONDS = VF.createURI("urn:Diamonds");
    private static final URI HEARTS = VF.createURI("urn:Hearts");
    private static final URI SPADES = VF.createURI("urn:Spades");

    // Definition #2: :Ranks owl:oneOf(:Ace, :2, :3, :4, :5, :6, :7, :8, :9, :10, :Jack, :Queen, :King)
    private static final URI ACE = VF.createURI("urn:Ace");
    private static final URI TWO = VF.createURI("urn:2");
    private static final URI THREE = VF.createURI("urn:3");
    private static final URI FOUR = VF.createURI("urn:4");
    private static final URI FIVE = VF.createURI("urn:5");
    private static final URI SIX = VF.createURI("urn:6");
    private static final URI SEVEN = VF.createURI("urn:7");
    private static final URI EIGHT = VF.createURI("urn:8");
    private static final URI NINE = VF.createURI("urn:9");
    private static final URI TEN = VF.createURI("urn:10");
    private static final URI JACK = VF.createURI("urn:Jack");
    private static final URI QUEEN = VF.createURI("urn:Queen");
    private static final URI KING = VF.createURI("urn:King");

    private static final Set<Resource> CARD_SUIT_ENUMERATION =
        Sets.newLinkedHashSet(
            Lists.newArrayList(CLUBS, DIAMONDS, HEARTS, SPADES)
        );
    private static final Set<Resource> CARD_RANK_ENUMERATION =
        Sets.newLinkedHashSet(
            Lists.newArrayList(
                ACE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN,
                JACK, QUEEN, KING
            )
        );

    @Test
    public void testOneOf() throws Exception {
        // Configure a mock instance engine with an ontology:
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        when(inferenceEngine.isEnumeratedType(SUITS)).thenReturn(true);
        when(inferenceEngine.getEnumeration(SUITS)).thenReturn(CARD_SUIT_ENUMERATION);
        when(inferenceEngine.isEnumeratedType(RANKS)).thenReturn(true);
        when(inferenceEngine.getEnumeration(RANKS)).thenReturn(CARD_RANK_ENUMERATION);
        // Query for a  Suits and rewrite using the visitor:
        final Projection query = new Projection(
                new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", SUITS)),
                new ProjectionElemList(new ProjectionElem("s", "subject")));
        query.visit(new OneOfVisitor(conf, inferenceEngine));
        // Expected structure: BindingSetAssignment containing the enumeration:
        // BindingSetAssignment(CLUBS, DIAMONDS, HEARTS, SPADES)
        // Collect the arguments to the BindingSetAssignment:
        assertTrue(query.getArg() instanceof BindingSetAssignment);
        final BindingSetAssignment bsa = (BindingSetAssignment) query.getArg();
        final Iterable<BindingSet> iterable = bsa.getBindingSets();
        final Iterator<BindingSet> iter = iterable.iterator();

        assertBindingSet(iter, CARD_SUIT_ENUMERATION.iterator());

        // Query for a Ranks and rewrite using the visitor:
        final Projection query2 = new Projection(
                new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", RANKS)),
                new ProjectionElemList(new ProjectionElem("s", "subject")));
        query2.visit(new OneOfVisitor(conf, inferenceEngine));
        // Expected structure: BindingSetAssignment containing the enumeration:
        // BindingSetAssignment(ACE, 2, 3, 4, 5, 6, 7, 8, 9, 10, JACK, QUEEN, KING)
        // Collect the arguments to the BindingSetAssignment:
        assertTrue(query2.getArg() instanceof BindingSetAssignment);
        final BindingSetAssignment bsa2 = (BindingSetAssignment) query2.getArg();
        final Iterable<BindingSet> iterable2 = bsa2.getBindingSets();
        final Iterator<BindingSet> iter2 = iterable2.iterator();

        assertBindingSet(iter2, CARD_RANK_ENUMERATION.iterator());
    }

    @Test
    public void testOneOfDisabled() throws Exception {
        // Configure a mock instance engine with an ontology:
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        when(inferenceEngine.isEnumeratedType(SUITS)).thenReturn(true);
        when(inferenceEngine.getEnumeration(SUITS)).thenReturn(CARD_SUIT_ENUMERATION);
        when(inferenceEngine.isEnumeratedType(RANKS)).thenReturn(true);
        when(inferenceEngine.getEnumeration(RANKS)).thenReturn(CARD_RANK_ENUMERATION);

        // Query for a Suits and rewrite using the visitor:
        final Projection query = new Projection(
                new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", SUITS)),
                new ProjectionElemList(new ProjectionElem("s", "subject")));

        final AccumuloRdfConfiguration disabledConf = conf.clone();
        disabledConf.setInferOneOf(false);

        query.visit(new OneOfVisitor(disabledConf, inferenceEngine));

        // Expected structure: the original statement:
        assertTrue(query.getArg() instanceof StatementPattern);
        final StatementPattern actualCardSuitSp = (StatementPattern) query.getArg();
        final StatementPattern expectedCardSuitSp = new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", SUITS));
        assertEquals(expectedCardSuitSp, actualCardSuitSp);
    }

    private static void assertBindingSet(final Iterator<BindingSet> bindingSetIter, final Iterator<Resource> expectedValues) {
        while (expectedValues.hasNext()) {
            final Resource expectedValue = expectedValues.next();
            assertTrue(bindingSetIter.hasNext());
            final BindingSet bindingSet = bindingSetIter.next();
            assertTrue(bindingSet instanceof QueryBindingSet);
            assertEquals(1, bindingSet.getBindingNames().size());
            final Binding binding = bindingSet.getBinding("s");
            assertNotNull(binding);
            final Value actualValue = binding.getValue();
            assertEquals(expectedValue, actualValue);
        }
    }
}