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
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.Binding;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.BindingSetAssignment;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Tests the methods of {@link OneOfVisitor}.
 */
public class OneOfVisitorTest {
    private final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final IRI SUITS = VF.createIRI("urn:Suits");
    private static final IRI RANKS = VF.createIRI("urn:Ranks");

    // Definition #1: :Suits owl:oneOf(:Clubs, :Diamonds, :Hearts, :Spades)
    private static final IRI CLUBS = VF.createIRI("urn:Clubs");
    private static final IRI DIAMONDS = VF.createIRI("urn:Diamonds");
    private static final IRI HEARTS = VF.createIRI("urn:Hearts");
    private static final IRI SPADES = VF.createIRI("urn:Spades");

    // Definition #2: :Ranks owl:oneOf(:Ace, :2, :3, :4, :5, :6, :7, :8, :9, :10, :Jack, :Queen, :King)
    private static final IRI ACE = VF.createIRI("urn:Ace");
    private static final IRI TWO = VF.createIRI("urn:2");
    private static final IRI THREE = VF.createIRI("urn:3");
    private static final IRI FOUR = VF.createIRI("urn:4");
    private static final IRI FIVE = VF.createIRI("urn:5");
    private static final IRI SIX = VF.createIRI("urn:6");
    private static final IRI SEVEN = VF.createIRI("urn:7");
    private static final IRI EIGHT = VF.createIRI("urn:8");
    private static final IRI NINE = VF.createIRI("urn:9");
    private static final IRI TEN = VF.createIRI("urn:10");
    private static final IRI JACK = VF.createIRI("urn:Jack");
    private static final IRI QUEEN = VF.createIRI("urn:Queen");
    private static final IRI KING = VF.createIRI("urn:King");

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