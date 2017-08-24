package org.apache.rya.rdftriplestore.inference;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.utils.NullableStatementImpl;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.Var;

import com.google.common.collect.Sets;

public class SomeValuesFromVisitorTest {
    private static final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
    private static final ValueFactory vf = new ValueFactoryImpl();

    // Value types
    private final URI course = vf.createURI("lubm:Course");
    private final URI gradCourse = vf.createURI("lubm:GraduateCourse");
    private final URI department = vf.createURI("lubm:Department");
    private final URI organization = vf.createURI("lubm:Organization");
    // Predicates
    private final URI takesCourse = vf.createURI("lubm:takesCourse");
    private final URI headOf = vf.createURI("lubm:headOf");
    private final URI worksFor = vf.createURI("lubm:worksFor");
    // Supertype of restriction types
    private final URI person = vf.createURI("lubm:Person");

    @Test
    public void testSomeValuesFrom() throws Exception {
        // Configure a mock instance engine with an ontology:
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        Map<Resource, Set<URI>> personSVF = new HashMap<>();
        personSVF.put(gradCourse, Sets.newHashSet(takesCourse));
        personSVF.put(course, Sets.newHashSet(takesCourse));
        personSVF.put(department, Sets.newHashSet(headOf));
        personSVF.put(organization, Sets.newHashSet(worksFor, headOf));
        when(inferenceEngine.getSomeValuesFromByRestrictionType(person)).thenReturn(personSVF);
        // Query for a specific type and rewrite using the visitor:
        StatementPattern originalSP = new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", person));
        final Projection query = new Projection(originalSP, new ProjectionElemList(new ProjectionElem("s", "subject")));
        query.visit(new SomeValuesFromVisitor(conf, inferenceEngine));
        // Expected structure: a union of two elements: one is equal to the original statement
        // pattern, and the other one joins a list of predicate/value type combinations
        // with another join querying for any nodes who are the subject of a triple with that
        // predicate and with an object of that type.
        //
        // Union(
        //     SP(?node a :impliedType),
        //     Join(
        //         FSP(<?property someValuesFrom ?valueType> {
        //             takesCourse/Course;
        //             takesCourse/GraduateCourse;
        //             headOf/Department;
        //             headOf/Organization;
        //             worksFor/Organization;
        //         }),
        //         Join(
        //             SP(_:object a ?valueType),
        //             SP(?node ?property _:object)
        //         )
        //     )
        Assert.assertTrue(query.getArg() instanceof Union);
        TupleExpr left = ((Union) query.getArg()).getLeftArg();
        TupleExpr right = ((Union) query.getArg()).getRightArg();
        Assert.assertEquals(originalSP, left);
        Assert.assertTrue(right instanceof Join);
        final Join join = (Join) right;
        Assert.assertTrue(join.getLeftArg() instanceof FixedStatementPattern);
        Assert.assertTrue(join.getRightArg() instanceof Join);
        FixedStatementPattern fsp = (FixedStatementPattern) join.getLeftArg();
        left = ((Join) join.getRightArg()).getLeftArg();
        right = ((Join) join.getRightArg()).getRightArg();
        Assert.assertTrue(left instanceof StatementPattern);
        Assert.assertTrue(right instanceof StatementPattern);
        // Verify expected predicate/type pairs
        Assert.assertTrue(fsp.statements.contains(new NullableStatementImpl(takesCourse, OWL.SOMEVALUESFROM, course)));
        Assert.assertTrue(fsp.statements.contains(new NullableStatementImpl(takesCourse, OWL.SOMEVALUESFROM, gradCourse)));
        Assert.assertTrue(fsp.statements.contains(new NullableStatementImpl(headOf, OWL.SOMEVALUESFROM, department)));
        Assert.assertTrue(fsp.statements.contains(new NullableStatementImpl(headOf, OWL.SOMEVALUESFROM, organization)));
        Assert.assertTrue(fsp.statements.contains(new NullableStatementImpl(worksFor, OWL.SOMEVALUESFROM, organization)));
        Assert.assertEquals(5, fsp.statements.size());
        // Verify pattern for matching instances of each pair: a Join of <_:x rdf:type ?t> and
        // <?s ?p _:x> where p and t are the predicate/type pair and s is the original subject
        // variable.
        StatementPattern leftSP = (StatementPattern) left;
        StatementPattern rightSP = (StatementPattern) right;
        Assert.assertEquals(rightSP.getObjectVar(), leftSP.getSubjectVar());
        Assert.assertEquals(RDF.TYPE, leftSP.getPredicateVar().getValue());
        Assert.assertEquals(fsp.getObjectVar(), leftSP.getObjectVar());
        Assert.assertEquals(originalSP.getSubjectVar(), rightSP.getSubjectVar());
        Assert.assertEquals(fsp.getSubjectVar(), rightSP.getPredicateVar());
    }

    @Test
    public void testSomeValuesFromDisabled() throws Exception {
        // Disable someValuesOf inference
        final AccumuloRdfConfiguration disabledConf = conf.clone();
        disabledConf.setInferSomeValuesFrom(false);
        // Configure a mock instance engine with an ontology:
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        Map<Resource, Set<URI>> personSVF = new HashMap<>();
        personSVF.put(gradCourse, Sets.newHashSet(takesCourse));
        personSVF.put(course, Sets.newHashSet(takesCourse));
        personSVF.put(department, Sets.newHashSet(headOf));
        personSVF.put(organization, Sets.newHashSet(worksFor, headOf));
        when(inferenceEngine.getSomeValuesFromByRestrictionType(person)).thenReturn(personSVF);
        // Query for a specific type visit -- should not change
        StatementPattern originalSP = new StatementPattern(new Var("s"), new Var("p", RDF.TYPE), new Var("o", person));
        final Projection originalQuery = new Projection(originalSP, new ProjectionElemList(new ProjectionElem("s", "subject")));
        final Projection modifiedQuery = originalQuery.clone();
        modifiedQuery.visit(new SomeValuesFromVisitor(disabledConf, inferenceEngine));
        Assert.assertEquals(originalQuery, modifiedQuery);
    }
}
