package org.apache.rya.reasoning.mr;

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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.rya.accumulo.mr.RyaStatementWritable;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolver;
import org.apache.rya.api.resolver.triple.impl.WholeRowTripleResolver;
import org.apache.rya.reasoning.Derivation;
import org.apache.rya.reasoning.OwlRule;
import org.apache.rya.reasoning.Fact;
import org.apache.rya.reasoning.TestUtils;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DuplicateElimination.DuplicateEliminationReducer.class})
public class DuplicateEliminationTest {
    // inputs
    static Fact X_SUB_Y = new Fact(
        TestUtils.uri("x"), TestUtils.uri("subOrganizationOf"), TestUtils.uri("y"));
    static Fact X_TYPE_ORG = new Fact(
        TestUtils.uri("x"), RDF.TYPE, TestUtils.uri("Organization"));
    static Fact X_TYPE_PERSON = new Fact(
        TestUtils.uri("x"), RDF.TYPE, TestUtils.uri("Person"));
    // inferences level 1
    static Fact Y_SUPER_X = new Fact(
        TestUtils.uri("y"), TestUtils.uri("hasSubOrganization"), TestUtils.uri("x"),
        1, OwlRule.PRP_INV, TestUtils.uri("y"));
    static Derivation X_DISJOINT = new Derivation(1, OwlRule.CAX_DW,
        TestUtils.uri("x"));
    // redundant inferences
    static Fact X_SUB_Y_INV = new Fact(
        TestUtils.uri("x"), TestUtils.uri("subOrganizationOf"), TestUtils.uri("y"),
        2, OwlRule.PRP_INV, TestUtils.uri("y"));
    static Fact Y_SUPER_X_INV = new Fact(
        TestUtils.uri("y"), TestUtils.uri("hasSubOrganization"), TestUtils.uri("x"),
        3, OwlRule.PRP_INV, TestUtils.uri("y"));
    // schema
    static Fact TRP = new Fact(
        TestUtils.uri("subOrganizationOf"), RDF.TYPE, OWL.TRANSITIVEPROPERTY);
    static Fact SPO = new Fact(
        TestUtils.uri("subOrganizationOf"), RDFS.SUBPROPERTYOF,
        TestUtils.uri("affiliatedWith"));
    static Fact INV = new Fact(
        TestUtils.uri("hasSubOrganization"), OWL.INVERSEOF,
        TestUtils.uri("subOrganizationOf"));
    static Fact DISJOINT = new Fact(TestUtils.uri("Person"),
        OWL.DISJOINTWITH, TestUtils.uri("Organization"));

    @Before
    public void setUp() {
        Y_SUPER_X.addSource(X_SUB_Y);
        X_SUB_Y_INV.addSource(Y_SUPER_X);
        Y_SUPER_X_INV.addSource(X_SUB_Y_INV);
        X_DISJOINT.addSource(X_TYPE_ORG);
        X_DISJOINT.addSource(X_TYPE_PERSON);
    }

    @Test
    public void testTableMapperOutput() throws Exception {
        RyaStatement rya = TestUtils.ryaStatement("x", "subOrganizationOf", "y");
        TripleRowResolver trr = new WholeRowTripleResolver();
        Map<TABLE_LAYOUT,TripleRow> map = trr.serialize(rya);
        TripleRow tr = map.get(TABLE_LAYOUT.SPO);
        byte[] b = new byte[0];
        Key key = new Key(tr.getRow(), tr.getColumnFamily(),
            tr.getColumnQualifier(), b, 1);
        Value val = new Value(b);
        new MapDriver<Key, Value, Fact, Derivation>()
            .withMapper(new DuplicateElimination.DuplicateTableMapper())
            .withInput(key, val)
            .withOutput(X_SUB_Y, X_SUB_Y.getDerivation())
            .runTest();
    }

    @Test
    public void testFileMapperOutput() throws Exception {
        new MapDriver<Fact, NullWritable, Fact, Derivation>()
            .withMapper(new DuplicateElimination.DuplicateFileMapper())
            .withInput(X_SUB_Y, NullWritable.get())
            .withOutput(X_SUB_Y, X_SUB_Y.getDerivation())
            .runTest();
    }

    @Test
    public void testRdfMapperOutput() throws Exception {
        RyaStatement rya = TestUtils.ryaStatement("x", "subOrganizationOf", "y");
        RyaStatementWritable rsw = new RyaStatementWritable();
        rsw.setRyaStatement(rya);
        LongWritable l = new LongWritable();
        new MapDriver<LongWritable, RyaStatementWritable, Fact,
            Derivation>()
            .withMapper(new DuplicateElimination.DuplicateRdfMapper())
            .withInput(l, rsw)
            .withOutput(X_SUB_Y, X_SUB_Y.getDerivation())
            .runTest();
    }

    @Test
    public void testInconsistencyMapperOutput() throws Exception {
        Fact empty = new Fact();
        empty.setDerivation(X_DISJOINT);
        new MapDriver<Derivation, NullWritable, Fact, Derivation>()
            .withMapper(new DuplicateElimination.InconsistencyMapper())
            .withInput(X_DISJOINT, NullWritable.get())
            .withOutput(empty, X_DISJOINT)
            .runTest();
    }

    @Test
    public void testEliminateOld() throws Exception {
        List<Derivation> facts = new LinkedList<>();
        facts.add(X_SUB_Y_INV.getDerivation());
        facts.add(X_SUB_Y.getDerivation());
        X_SUB_Y.unsetDerivation();
        ReduceDriver<Fact, Derivation, Fact, NullWritable> driver = new ReduceDriver<>();
        driver.getConfiguration().setInt(MRReasoningUtils.STEP_PROP, 1);
        driver.withReducer(new DuplicateElimination.DuplicateEliminationReducer())
            .withInput(X_SUB_Y, facts)
            .runTest();
    }

    @Test
    public void testRetainSimplest() throws Exception {
        List<Derivation> facts = new LinkedList<>();
        facts.add(Y_SUPER_X_INV.getDerivation());
        facts.add(Y_SUPER_X.getDerivation());
        Fact unset = Y_SUPER_X.clone();
        unset.unsetDerivation();
        ReduceDriver<Fact, Derivation, Fact, NullWritable> driver = new ReduceDriver<>();
        driver.getConfiguration().setInt(MRReasoningUtils.STEP_PROP, 1);
        driver.withReducer(new DuplicateElimination.DuplicateEliminationReducer())
            .withInput(unset, facts)
            .withMultiOutput(MRReasoningUtils.INTERMEDIATE_OUT,
                Y_SUPER_X, NullWritable.get())
            .runTest();
    }

    @Test
    public void testInconsistencyReduce() throws Exception {
        List<Derivation> facts = new LinkedList<>();
        facts.add(X_DISJOINT.clone());
        facts.add(X_DISJOINT.clone());
        ReduceDriver<Fact, Derivation, Fact, NullWritable> driver = new ReduceDriver<>();
        driver.getConfiguration().setInt(MRReasoningUtils.STEP_PROP, 1);
        driver.withReducer(new DuplicateElimination.DuplicateEliminationReducer())
            .withInput(Fact.NONE, facts)
            .withMultiOutput(MRReasoningUtils.INCONSISTENT_OUT,
                X_DISJOINT, NullWritable.get())
            .runTest();
    }

    @Test
    public void testInconsistencyOld() throws Exception {
        List<Derivation> facts = new LinkedList<>();
        facts.add(X_DISJOINT.clone());
        ReduceDriver<Fact, Derivation, Fact, NullWritable> driver = new ReduceDriver<>();
        driver.getConfiguration().setInt(MRReasoningUtils.STEP_PROP, 2);
        driver.withReducer(new DuplicateElimination.DuplicateEliminationReducer())
            .withInput(Fact.NONE, facts)
            .runTest();
    }
}
