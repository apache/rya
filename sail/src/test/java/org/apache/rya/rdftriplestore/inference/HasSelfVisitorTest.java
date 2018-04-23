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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.Extension;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Union;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.Assert;
import org.junit.Test;

public class HasSelfVisitorTest {
    private final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final IRI narcissist = VF.createIRI("urn:Narcissist");
    private static final IRI love = VF.createIRI("urn:love");
    private static final IRI self = VF.createIRI("urn:self");

    @Test
    public void testTypePattern() throws Exception {
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        final Set<IRI> narcissistProps = new HashSet<>();
        narcissistProps.add(love);
        when(inferenceEngine.getHasSelfImplyingType(narcissist)).thenReturn(narcissistProps);
        final Var subj = new Var("s");
        final Var obj = new Var("o", narcissist);
        obj.setConstant(true);
        final Var pred = new Var("p", RDF.TYPE);
        pred.setConstant(true);

        final Projection query = new Projection(new StatementPattern(subj, pred, obj),
                new ProjectionElemList(new ProjectionElem("s", "subject")));
        query.visit(new HasSelfVisitor(conf, inferenceEngine));

        Assert.assertTrue(query.getArg() instanceof Union);
        final Union union = (Union) query.getArg();
        Assert.assertTrue(union.getRightArg() instanceof StatementPattern);
        Assert.assertTrue(union.getLeftArg() instanceof StatementPattern);
        final StatementPattern expectedLeft = new StatementPattern(subj, pred, obj);
        final StatementPattern expectedRight = new StatementPattern(subj, new Var("urn:love", love), subj);
        Assert.assertEquals(expectedLeft, union.getLeftArg());
        Assert.assertEquals(expectedRight, union.getRightArg());
    }

    @Test
    public void testPropertyPattern_constantSubj() throws Exception {
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        final Set<Resource> loveTypes = new HashSet<>();
        loveTypes.add(narcissist);
        when(inferenceEngine.getHasSelfImplyingProperty(love)).thenReturn(loveTypes);
        final Var subj = new Var("s", self);
        subj.setConstant(true);
        final Var obj = new Var("o");
        final Var pred = new Var("p", love);
        pred.setConstant(true);

        final Projection query = new Projection(new StatementPattern(subj, pred, obj),
                new ProjectionElemList(new ProjectionElem("s", "subject")));
        query.visit(new HasSelfVisitor(conf, inferenceEngine));

        Assert.assertTrue(query.getArg() instanceof Union);
        final Union union = (Union) query.getArg();
        Assert.assertTrue(union.getRightArg() instanceof StatementPattern);
        Assert.assertTrue(union.getLeftArg() instanceof Extension);
        final StatementPattern expectedRight = new StatementPattern(subj, pred, obj);
        final Extension expectedLeft = new Extension(
                new StatementPattern(subj, new Var(RDF.TYPE.stringValue(), RDF.TYPE), new Var("urn:Narcissist", narcissist)),
                new ExtensionElem(subj, "o"));
        Assert.assertEquals(expectedLeft, union.getLeftArg());
        Assert.assertEquals(expectedRight, union.getRightArg());
    }

    @Test
    public void testPropertyPattern_constantObj() throws Exception {
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        final Set<Resource> loveTypes = new HashSet<>();
        loveTypes.add(narcissist);
        when(inferenceEngine.getHasSelfImplyingProperty(love)).thenReturn(loveTypes);
        final Var subj = new Var("s");
        final Var obj = new Var("o", self);
        obj.setConstant(true);
        final Var pred = new Var("p", love);
        pred.setConstant(true);

        final Projection query = new Projection(new StatementPattern(subj, pred, obj),
                new ProjectionElemList(new ProjectionElem("s", "subject")));
        query.visit(new HasSelfVisitor(conf, inferenceEngine));

        Assert.assertTrue(query.getArg() instanceof Union);
        final Union union = (Union) query.getArg();
        Assert.assertTrue(union.getRightArg() instanceof StatementPattern);
        Assert.assertTrue(union.getLeftArg() instanceof Extension);
        final StatementPattern expectedRight = new StatementPattern(subj, pred, obj);
        final Extension expectedLeft = new Extension(
                new StatementPattern(obj, new Var(RDF.TYPE.stringValue(), RDF.TYPE), new Var("urn:Narcissist", narcissist)),
                new ExtensionElem(obj, "s"));
        Assert.assertEquals(expectedLeft, union.getLeftArg());
        Assert.assertEquals(expectedRight, union.getRightArg());
    }
}