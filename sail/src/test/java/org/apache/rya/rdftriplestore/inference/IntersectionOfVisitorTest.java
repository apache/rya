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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Tests the methods of {@link IntersectionOfVisitor}.
 */
public class IntersectionOfVisitorTest {
    private final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
    private static final ValueFactory vf = SimpleValueFactory.getInstance();

    private static final IRI MOTHER = vf.createIRI("urn:Mother");
    private static final IRI FATHER = vf.createIRI("urn:Father");

    // Definition #1: :Mother owl:intersectionOf(:Animal, :Female, :Parent)
    private static final IRI ANIMAL = vf.createIRI("urn:Animal");
    private static final IRI FEMALE = vf.createIRI("urn:Female");
    private static final IRI PARENT = vf.createIRI("urn:Parent");

    // Definition #2: :Mother owl:intersectionOf(:Female, :Leader, :Nun)
    private static final IRI NUN = vf.createIRI("urn:Nun");
    private static final IRI LEADER = vf.createIRI("urn:Leader");

    // Definition #3: :Father owl:intersectionOf(:Man, :Parent)
    private static final IRI MAN = vf.createIRI("urn:Man");

    @Test
    public void testIntersectionOf() throws Exception {
        // Configure a mock instance engine with an ontology:
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        final Map<Resource, List<Set<Resource>>> intersections = new HashMap<>();
        final List<Set<Resource>> motherIntersections = Arrays.asList(
                Sets.newHashSet(ANIMAL, FEMALE, PARENT),
                Sets.newHashSet(FEMALE, LEADER, NUN)
            );
        final List<Set<Resource>> fatherIntersections = Arrays.asList(
                Sets.newHashSet(MAN, PARENT)
            );
        intersections.put(MOTHER, motherIntersections);
        when(inferenceEngine.getIntersectionsImplying(MOTHER)).thenReturn(motherIntersections);
        when(inferenceEngine.getIntersectionsImplying(FATHER)).thenReturn(fatherIntersections);
        // Query for a specific type and rewrite using the visitor:
        final Projection query = new Projection(
                new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", MOTHER)),
                new ProjectionElemList(new ProjectionElem("s", "subject")));
        query.visit(new IntersectionOfVisitor(conf, inferenceEngine));
        // Expected structure: a union whose members are the original
        // statement pattern and a nested union of statement patterns and joins.
        // This will join both intersections of Mother together. The nested
        // union tree is unioned with the original statement pattern.
        // The nested union of Mother should be:
        // Union(
        //     Join(
        //         SP(Animal),
        //         Join(
        //             SP(Female),
        //             SP(Parent)
        //         )
        //     ),
        //     Join(
        //         SP(Female),
        //         Join(
        //             SP(Leader),
        //             SP(Nun)
        //         )
        //     )
        // )
        //
        // Collect the arguments to the union:
        assertTrue(query.getArg() instanceof Union);
        final Union union1 = (Union) query.getArg();
        assertTrue(union1.getLeftArg() instanceof Union);
        final Union union2 = (Union) union1.getLeftArg();

        assertTrue(union2.getLeftArg() instanceof Join);
        final Join join1 = (Join) union2.getLeftArg();
        assertTrue(join1.getLeftArg() instanceof StatementPattern);
        final StatementPattern animalSp = (StatementPattern) join1.getLeftArg();
        assertStatementPattern(animalSp, ANIMAL);
        assertTrue(join1.getRightArg() instanceof Join);
        final Join join2 = (Join) join1.getRightArg();
        assertTrue(join2.getLeftArg() instanceof StatementPattern);
        final StatementPattern femaleSp = (StatementPattern) join2.getLeftArg();
        assertStatementPattern(femaleSp, FEMALE);
        assertTrue(join2.getRightArg() instanceof StatementPattern);
        final StatementPattern parentSp = (StatementPattern) join2.getRightArg();
        assertStatementPattern(parentSp, PARENT);

        assertTrue(union2.getRightArg() instanceof Join);
        final Join join3 = (Join) union2.getRightArg();
        assertTrue(join3.getLeftArg() instanceof StatementPattern);
        final StatementPattern femaleSp2 = (StatementPattern) join3.getLeftArg();
        assertStatementPattern(femaleSp2, FEMALE);
        assertTrue(join3.getRightArg() instanceof Join);
        final Join join4 = (Join) join3.getRightArg();
        assertTrue(join4.getLeftArg() instanceof StatementPattern);
        final StatementPattern leaderSp = (StatementPattern) join4.getLeftArg();
        assertStatementPattern(leaderSp, LEADER);
        assertTrue(join4.getRightArg() instanceof StatementPattern);
        final StatementPattern nunSp = (StatementPattern) join4.getRightArg();
        assertStatementPattern(nunSp, NUN);

        assertTrue(union1.getRightArg() instanceof StatementPattern);
        final StatementPattern actualMotherSp = (StatementPattern) union1.getRightArg();
        final StatementPattern expectedMotherSp = new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", MOTHER));
        assertEquals(expectedMotherSp, actualMotherSp);


        // Query for a specific type and rewrite using the visitor:
        final Projection query2 = new Projection(
                new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", FATHER)),
                new ProjectionElemList(new ProjectionElem("s", "subject")));
        query2.visit(new IntersectionOfVisitor(conf, inferenceEngine));
        // Since Father produces only one intersection it creates a nested join
        // that gets union with the original statement pattern.
        // The nested join of Father should be:
        // Join(
        //     SP(Man),
        //     SP(Parent)
        // )
        // Collect the arguments to the union:
        assertTrue(query2.getArg() instanceof Union);
        final Union union3 = (Union) query2.getArg();
        assertTrue(union3.getLeftArg() instanceof Join);
        final Join join5 = (Join) union3.getLeftArg();
        assertTrue(join5.getLeftArg() instanceof StatementPattern);
        final StatementPattern manSp = (StatementPattern) join5.getLeftArg();
        assertStatementPattern(manSp, MAN);
        assertTrue(join5.getRightArg() instanceof StatementPattern);
        final StatementPattern parentSp2 = (StatementPattern) join5.getRightArg();
        assertStatementPattern(parentSp2, PARENT);

        assertTrue(union3.getRightArg() instanceof StatementPattern);
        final StatementPattern actualFatherSp = (StatementPattern) union3.getRightArg();
        final StatementPattern expectedFatherSp = new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", FATHER));
        assertEquals(expectedFatherSp, actualFatherSp);
    }

    @Test
    public void testIntersectionOfDisabled() throws Exception {
        // Configure a mock instance engine with an ontology:
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        final Map<Resource, List<Set<Resource>>> intersections = new HashMap<>();
        final List<Set<Resource>> motherIntersections = Arrays.asList(
                Sets.newHashSet(ANIMAL, FEMALE, PARENT),
                Sets.newHashSet(FEMALE, LEADER, NUN)
            );
        final List<Set<Resource>> fatherIntersections = Arrays.asList(
                Sets.newHashSet(MAN, PARENT)
            );
        intersections.put(MOTHER, motherIntersections);
        when(inferenceEngine.getIntersectionsImplying(MOTHER)).thenReturn(motherIntersections);
        when(inferenceEngine.getIntersectionsImplying(FATHER)).thenReturn(fatherIntersections);
        // Query for a specific type and rewrite using the visitor:
        final Projection query = new Projection(
                new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", MOTHER)),
                new ProjectionElemList(new ProjectionElem("s", "subject")));

        final AccumuloRdfConfiguration disabledConf = conf.clone();
        disabledConf.setInferIntersectionOf(false);

        query.visit(new IntersectionOfVisitor(disabledConf, inferenceEngine));

        // Expected structure: the original statement:
        assertTrue(query.getArg() instanceof StatementPattern);
        final StatementPattern actualMotherSp = (StatementPattern) query.getArg();
        final StatementPattern expectedMotherSp = new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", MOTHER));
        assertEquals(expectedMotherSp, actualMotherSp);


        // Query for a specific type and rewrite using the visitor:
        final Projection query2 = new Projection(
                new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", FATHER)),
                new ProjectionElemList(new ProjectionElem("s", "subject")));
        query2.visit(new IntersectionOfVisitor(disabledConf, inferenceEngine));

        // Expected structure: the original statement:
        assertTrue(query2.getArg() instanceof StatementPattern);
        final StatementPattern actualFatherSp = (StatementPattern) query2.getArg();
        final StatementPattern expectedFatherSp = new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", FATHER));
        assertEquals(expectedFatherSp, actualFatherSp);
    }

    private static void assertStatementPattern(final StatementPattern statementPattern, final IRI uri) {
        assertNotNull(statementPattern.getPredicateVar());
        assertEquals(RDF.TYPE, statementPattern.getPredicateVar().getValue());
        assertNotNull(statementPattern.getObjectVar());
        assertEquals(uri, statementPattern.getObjectVar().getValue());
    }
}