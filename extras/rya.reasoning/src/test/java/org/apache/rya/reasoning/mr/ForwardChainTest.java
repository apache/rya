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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.rya.accumulo.mr.RyaStatementWritable;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolver;
import org.apache.rya.api.resolver.triple.impl.WholeRowTripleResolver;
import org.apache.rya.reasoning.OwlRule;
import org.apache.rya.reasoning.Fact;
import org.apache.rya.reasoning.Schema;
import org.apache.rya.reasoning.TestUtils;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceFeeder;
import org.apache.hadoop.mrunit.types.KeyValueReuseList;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ForwardChain.FileMapper.class, ForwardChain.ReasoningReducer.class})
public class ForwardChainTest {
    // inputs
    static Fact X_SUB_Y = new Fact(
        TestUtils.uri("x"), TestUtils.uri("subOrganizationOf"), TestUtils.uri("y"));
    static Fact Y_SUB_Z = new Fact(
        TestUtils.uri("y"), TestUtils.uri("subOrganizationOf"), TestUtils.uri("z"));
    // inferences
    static Fact Y_SUPER_X = new Fact(
        TestUtils.uri("y"), TestUtils.uri("hasSubOrganization"), TestUtils.uri("x"),
        1, OwlRule.PRP_INV, TestUtils.uri("y"));
    static Fact X_SUB_Z = new Fact(
        TestUtils.uri("x"), TestUtils.uri("subOrganizationOf"), TestUtils.uri("z"),
        1, OwlRule.PRP_TRP, TestUtils.uri("y"));
    // schema
    static Fact TRP = new Fact(
        TestUtils.uri("subOrganizationOf"), RDF.TYPE, OWL.TRANSITIVEPROPERTY);
    static Fact SPO = new Fact(
        TestUtils.uri("subOrganizationOf"), RDFS.SUBPROPERTYOF,
        TestUtils.uri("affiliatedWith"));
    static Fact INV = new Fact(
        TestUtils.uri("hasSubOrganization"), OWL.INVERSEOF,
        TestUtils.uri("subOrganizationOf"));

    Schema schema;

    @Before
    public void setUp() {
        schema = new Schema();
        schema.processTriple(TRP.getTriple());
        schema.closure();
        Y_SUPER_X.addSource(X_SUB_Y);
        X_SUB_Z.addSource(X_SUB_Y);
        X_SUB_Z.addSource(Y_SUB_Z);
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
        ResourceWritable rw1 = new ResourceWritable();
        ResourceWritable rw2 = new ResourceWritable();
        rw1.set(TestUtils.uri("x"));
        rw2.set(TestUtils.uri("y"));
        new MapDriver<Key, Value, ResourceWritable, Fact>()
            .withMapper(new ForwardChain.TableMapper(schema))
            .withInput(key, val)
            .withOutput(rw1, X_SUB_Y)
            .withOutput(rw2, X_SUB_Y)
            .runTest();
    }

    @Test
    public void testFileMapperOutput() throws Exception {
        ResourceWritable rw1 = new ResourceWritable();
        ResourceWritable rw2 = new ResourceWritable();
        rw1.set(TestUtils.uri("x"));
        rw2.set(TestUtils.uri("y"));
        new MapDriver<Fact, NullWritable, ResourceWritable, Fact>()
            .withMapper(new ForwardChain.FileMapper(schema))
            .withInput(X_SUB_Y, NullWritable.get())
            .withOutput(rw1, X_SUB_Y)
            .withOutput(rw2, X_SUB_Y)
            .runTest();
    }

    @Test
    public void testRdfMapperOutput() throws Exception {
        RyaStatement rya = TestUtils.ryaStatement("x", "subOrganizationOf", "y");
        RyaStatementWritable rsw = new RyaStatementWritable();
        rsw.setRyaStatement(rya);
        LongWritable l = new LongWritable();
        ResourceWritable rw1 = new ResourceWritable();
        ResourceWritable rw2 = new ResourceWritable();
        rw1.set(TestUtils.uri("x"));
        rw2.set(TestUtils.uri("y"));
        new MapDriver<LongWritable, RyaStatementWritable, ResourceWritable,
                Fact>()
            .withMapper(new ForwardChain.RdfMapper(schema))
            .withInput(l, rsw)
            .withOutput(rw1, X_SUB_Y)
            .withOutput(rw2, X_SUB_Y)
            .runTest();
    }

    @Test
    public void testReducerInference() throws Exception {
        schema.processTriple(INV.getTriple());
        schema.closure();
        ResourceWritable rw = new ResourceWritable();
        rw.set(TestUtils.uri("y"));
        List<Fact> facts = new LinkedList<>();
        facts.add(X_SUB_Y);
        new ReduceDriver<ResourceWritable, Fact, Fact, NullWritable>()
            .withReducer(new ForwardChain.ReasoningReducer(schema))
            .withInput(rw, facts)
            .withMultiOutput(MRReasoningUtils.INTERMEDIATE_OUT,
                Y_SUPER_X, NullWritable.get())
            .runTest();
    }

    @Test
    public void testReducerJoin() throws Exception {
        ResourceWritable rw = new ResourceWritable();
        rw.set(TestUtils.uri("y"));
        List<Fact> facts = new LinkedList<>();
        facts.add(X_SUB_Y);
        facts.add(Y_SUB_Z);
        ReduceDriver<ResourceWritable, Fact, Fact,
            NullWritable> driver = new ReduceDriver<>();
        driver.getConfiguration().setInt(MRReasoningUtils.STEP_PROP, 1);
        driver.withReducer(new ForwardChain.ReasoningReducer(schema))
            .withInput(rw, facts)
            .withMultiOutput(MRReasoningUtils.INTERMEDIATE_OUT,
                X_SUB_Z, NullWritable.get())
            .runTest();
    }

    /**
     * MultipleOutputs support is minimal, so we have to check each map/reduce
     * step explicitly
     */
    @Test
    public void testTransitiveChain() throws Exception {
        int max = 8;
        int n = 4;
        URI prop = TestUtils.uri("subOrganizationOf");
        Map<Integer, Map<Integer, Pair<Fact, NullWritable>>> connections
            = new HashMap<>();
        for (int i = 0; i <= max; i++) {
            connections.put(i, new HashMap<Integer, Pair<Fact, NullWritable>>());
        }
        // Initial input: make a chain from org0 to org8
        for (int i = 0; i < max; i++) {
            URI orgI = TestUtils.uri("org" + i);
            URI orgJ = TestUtils.uri("org" + (i + 1));
            Fact triple = new Fact(orgI, prop, orgJ);
            connections.get(i).put(i+1, new Pair<>(triple, NullWritable.get()));
        }
        for (int i = 1; i <= n; i++) {
            // Map:
            MapDriver<Fact, NullWritable, ResourceWritable,
                Fact> mDriver = new MapDriver<>();
            mDriver.getConfiguration().setInt(MRReasoningUtils.STEP_PROP, i);
            mDriver.setMapper(new ForwardChain.FileMapper(schema));
            for (int j : connections.keySet()) {
                for (int k : connections.get(j).keySet()) {
                    mDriver.addInput(connections.get(j).get(k));
                }
            }
            List<Pair<ResourceWritable, Fact>> mapped = mDriver.run();
            // Convert data for reduce phase:
            ReduceFeeder<ResourceWritable, Fact> feeder =
                new ReduceFeeder<>(mDriver.getConfiguration());
            List<KeyValueReuseList<ResourceWritable, Fact>> intermediate
                = feeder.sortAndGroup(mapped,
                new ResourceWritable.SecondaryComparator(),
                new ResourceWritable.PrimaryComparator());
            // Reduce, and compare to expected output:
            ReduceDriver<ResourceWritable, Fact, Fact,
                NullWritable> rDriver = new ReduceDriver<>();
            rDriver.getConfiguration().setInt(MRReasoningUtils.STEP_PROP, i);
            rDriver.setReducer(new ForwardChain.ReasoningReducer(schema));
            rDriver.addAllElements(intermediate);
            int maxSpan = (int) Math.pow(2, i);
            int minSpan = (maxSpan/2) + 1;
            // For each j, build all paths starting with j:
            for (int j = 0; j < max; j++) {
                // This includes any path of length k for appropriate k:
                for (int k = minSpan; k <= maxSpan && j+k <= max; k++) {
                    int middle = j + minSpan - 1;
                    URI left = TestUtils.uri("org" + j);
                    URI right = TestUtils.uri("org" + (j + k));
                    Fact triple = new Fact(left, prop,
                        right, i, OwlRule.PRP_TRP, TestUtils.uri("org" + middle));
                    triple.addSource(connections.get(j).get(middle).getFirst());
                    triple.addSource(connections.get(middle).get(j+k).getFirst());
                    Pair<Fact, NullWritable> expected =
                        new Pair<>(triple, NullWritable.get());
                    connections.get(j).put(j+k, expected);
                    rDriver.addMultiOutput("intermediate", expected);
                }
            }
            rDriver.runTest();
        }
    }
}
