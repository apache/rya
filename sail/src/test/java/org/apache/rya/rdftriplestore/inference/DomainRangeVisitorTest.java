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

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.algebra.Join;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.Assert;
import org.junit.Test;

public class DomainRangeVisitorTest {
    private static final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    private static final IRI person = VF.createIRI("lubm:Person");
    private static final IRI advisor = VF.createIRI("lubm:advisor");
    private static final IRI takesCourse = VF.createIRI("lubm:takesCourse");

    @Test
    public void testRewriteTypePattern() throws Exception {
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        final Set<IRI> domainPredicates = new HashSet<>();
        final Set<IRI> rangePredicates = new HashSet<>();
        domainPredicates.add(advisor);
        domainPredicates.add(takesCourse);
        rangePredicates.add(advisor);
        when(inferenceEngine.getPropertiesWithDomain(person)).thenReturn(domainPredicates);
        when(inferenceEngine.getPropertiesWithRange(person)).thenReturn(rangePredicates);
        final Var subjVar = new Var("s");
        final StatementPattern originalSP = new StatementPattern(subjVar, new Var("p", RDF.TYPE), new Var("o", person));
        final Projection query = new Projection(originalSP, new ProjectionElemList(new ProjectionElem("s", "subject")));
        query.visit(new DomainRangeVisitor(conf, inferenceEngine));
        // Resulting tree should consist of Unions of:
        // 1. The original StatementPattern
        // 2. A join checking for domain inference
        // 3. A join checking for range inference
        boolean containsOriginal = false;
        boolean containsDomain = false;
        boolean containsRange = false;
        final Stack<TupleExpr> nodes = new Stack<>();
        nodes.push(query.getArg());
        while (!nodes.isEmpty()) {
            final TupleExpr currentNode = nodes.pop();
            if (currentNode instanceof Union) {
                nodes.push(((Union) currentNode).getLeftArg());
                nodes.push(((Union) currentNode).getRightArg());
            }
            else if (currentNode instanceof StatementPattern) {
                Assert.assertFalse(containsOriginal);
                Assert.assertEquals(originalSP, currentNode);
                containsOriginal = true;
            }
            else if (currentNode instanceof Join) {
                final TupleExpr left = ((Join) currentNode).getLeftArg();
                final TupleExpr right = ((Join) currentNode).getRightArg();
                Assert.assertTrue("Left-hand side should enumerate domain/range predicates",
                        left instanceof FixedStatementPattern);
                Assert.assertTrue("Right-hand side should be a non-expandable SP matching triples satisfying domain/range",
                        right instanceof DoNotExpandSP);
                final FixedStatementPattern fsp = (FixedStatementPattern) left;
                final StatementPattern sp = (StatementPattern) right;
                // fsp should be <predicate var, domain/range, original type var>
                boolean isDomain = RDFS.DOMAIN.equals(fsp.getPredicateVar().getValue());
                boolean isRange = RDFS.RANGE.equals(fsp.getPredicateVar().getValue());
                Assert.assertTrue(isDomain || isRange);
                Assert.assertEquals(originalSP.getObjectVar(), fsp.getObjectVar());
                // sp should have same predicate var
                Assert.assertEquals(fsp.getSubjectVar(), sp.getPredicateVar());
                // collect predicates that are enumerated in the FixedStatementPattern
                final Set<Resource> queryPredicates = new HashSet<>();
                for (Statement statement : fsp.statements) {
                    queryPredicates.add(statement.getSubject());
                }
                if (isDomain) {
                    Assert.assertFalse(containsDomain);
                    // sp should be <original subject var, predicate var, unbound object var> for domain
                    Assert.assertEquals(originalSP.getSubjectVar(), sp.getSubjectVar());
                    Assert.assertFalse(sp.getObjectVar().hasValue());
                    // verify predicates
                    Assert.assertEquals(2, fsp.statements.size());
                    Assert.assertEquals(domainPredicates, queryPredicates);
                    Assert.assertTrue(queryPredicates.contains(advisor));
                    Assert.assertTrue(queryPredicates.contains(takesCourse));
                    containsDomain = true;
                }
                else {
                    Assert.assertFalse(containsRange);
                    // sp should be <unbound subject var, predicate var, original subject var> for range
                    Assert.assertFalse(sp.getSubjectVar().hasValue());
                    Assert.assertEquals(originalSP.getSubjectVar(), sp.getObjectVar());
                    // verify predicates
                    Assert.assertEquals(1, fsp.statements.size());
                    Assert.assertEquals(rangePredicates, queryPredicates);
                    Assert.assertTrue(queryPredicates.contains(advisor));
                    containsRange = true;
                }
            }
            else {
                Assert.fail("Expected nested Unions of Joins and StatementPatterns, found: " + currentNode.toString());
            }
        }
        Assert.assertTrue(containsOriginal && containsDomain && containsRange);
    }
}
