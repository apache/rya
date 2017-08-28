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

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.ZeroLengthPath;

/**
 * Tests the methods of {@link ReflexivePropertyVisitor}.
 */
public class ReflexivePropertyVisitorTest {
    private final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
    private static final ValueFactory VF = new ValueFactoryImpl();

    private static final URI ALICE = VF.createURI("urn:Alice");
    private static final URI HAS_FAMILY = VF.createURI("urn:hasFamilyMember");

    @Test
    public void testReflexiveProperty() throws Exception {
        // Define a reflexive property
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        when(inferenceEngine.isReflexiveProperty(HAS_FAMILY)).thenReturn(true);
        // Construct a query, then visit it
        final StatementPattern sp = new StatementPattern(new Var("s", ALICE), new Var("p", HAS_FAMILY), new Var("o"));
        final Projection query = new Projection(sp, new ProjectionElemList(new ProjectionElem("o", "member")));
        query.visit(new ReflexivePropertyVisitor(conf, inferenceEngine));
        // Expected structure after rewriting SP(:Alice :hasFamilyMember ?member):
        //
        // Union(
        //     originalSP(:Alice :hasFamilyMember ?member),
        //     ZeroLengthPath(:Alice, ?member)
        // )
        Assert.assertTrue(query.getArg() instanceof Union);
        final TupleExpr left = ((Union) query.getArg()).getLeftArg();
        final TupleExpr right = ((Union) query.getArg()).getRightArg();
        Assert.assertEquals(sp, left);
        Assert.assertTrue(right instanceof ZeroLengthPath);
        Assert.assertEquals(sp.getSubjectVar(), ((ZeroLengthPath) right).getSubjectVar());
        Assert.assertEquals(sp.getObjectVar(), ((ZeroLengthPath) right).getObjectVar());
    }

    @Test
    public void testReflexivePropertyDisabled() throws Exception {
        // Disable inference
        final RdfCloudTripleStoreConfiguration disabledConf = conf.clone();
        disabledConf.setInferReflexiveProperty(false);
        // Define a reflexive property
        final InferenceEngine inferenceEngine = mock(InferenceEngine.class);
        when(inferenceEngine.isReflexiveProperty(HAS_FAMILY)).thenReturn(true);
        // Construct a query, then make a copy and visit the copy
        final Projection query = new Projection(
                new StatementPattern(new Var("s", ALICE), new Var("p", HAS_FAMILY), new Var("o")),
                new ProjectionElemList(new ProjectionElem("s", "subject")));
        final Projection modifiedQuery = query.clone();
        modifiedQuery.visit(new ReflexivePropertyVisitor(disabledConf, inferenceEngine));
        // There should be no difference
        Assert.assertEquals(query, modifiedQuery);
    }
}