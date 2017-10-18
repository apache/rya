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
package org.apache.rya.mongodb.aggregation;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.algebra.Compare;
import org.eclipse.rdf4j.query.algebra.ExtensionElem;
import org.eclipse.rdf4j.query.algebra.IsLiteral;
import org.eclipse.rdf4j.query.algebra.Not;
import org.eclipse.rdf4j.query.algebra.ProjectionElem;
import org.eclipse.rdf4j.query.algebra.ProjectionElemList;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.Sets;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;

public class AggregationPipelineQueryNodeTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static final String LUBM = "urn:lubm";
    private static final IRI UNDERGRAD = VF.createIRI(LUBM, "UndergraduateStudent");
    private static final IRI TAKES = VF.createIRI(LUBM, "takesCourse");

    private static Var constant(IRI value) {
        return new Var(value.stringValue(), value);
    }

    private MongoCollection<Document> collection;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        collection = Mockito.mock(MongoCollection.class);
        Mockito.when(collection.getNamespace()).thenReturn(new MongoNamespace("db", "collection"));
    }

    @Test
    public void testEquals() {
        final AggregationPipelineQueryNode node1 = new AggregationPipelineQueryNode(
                collection,
                new LinkedList<>(),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y", "opt"),
                HashBiMap.create());
        final AggregationPipelineQueryNode node2 = new AggregationPipelineQueryNode(
                collection,
                new LinkedList<>(),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y", "opt"),
                HashBiMap.create());
        Assert.assertEquals(node1, node2);
        Assert.assertEquals(node1.hashCode(), node2.hashCode());
        final AggregationPipelineQueryNode diff1 = new AggregationPipelineQueryNode(
                collection,
                new LinkedList<>(),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y"),
                HashBiMap.create());
        final AggregationPipelineQueryNode diff2 = new AggregationPipelineQueryNode(
                collection,
                Arrays.asList(new Document()),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y", "opt"),
                HashBiMap.create());
        HashBiMap<String, String> varMapping = HashBiMap.create();
        varMapping.put("field-x", "x");
        final AggregationPipelineQueryNode diff3 = new AggregationPipelineQueryNode(
                collection,
                Arrays.asList(new Document()),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y", "opt"),
                varMapping);
        Assert.assertNotEquals(diff1, node1);
        Assert.assertNotEquals(diff2, node1);
        Assert.assertNotEquals(diff3, node1);
        node1.joinWith(new StatementPattern(new Var("x"), constant(TAKES), new Var("c")));
        node2.joinWith(new StatementPattern(new Var("x"), constant(TAKES), new Var("c")));
        Assert.assertEquals(node1, node2);
        node2.joinWith(new StatementPattern(new Var("x"), constant(TAKES), new Var("c")));
        Assert.assertNotEquals(node1, node2);
    }

    @Test
    public void testClone() {
        final AggregationPipelineQueryNode base = new AggregationPipelineQueryNode(
                collection,
                new LinkedList<>(),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y", "opt"),
                HashBiMap.create());
        final AggregationPipelineQueryNode copy = base.clone();
        Assert.assertEquals(base, copy);
        copy.getPipeline().add(new Document("$project", new Document()));
        Assert.assertNotEquals(base, copy);
        base.getPipeline().add(new Document("$project", new Document()));
        Assert.assertEquals(base, copy);
    }

    @Test
    public void testStatementPattern() throws Exception {
        // All variables
        StatementPattern sp = new StatementPattern(new Var("s"), new Var("p"), new Var("o"));
        AggregationPipelineQueryNode node = new AggregationPipelineQueryNode(collection, sp);
        Assert.assertEquals(Sets.newHashSet("s", "p", "o"), node.getBindingNames());
        Assert.assertEquals(Sets.newHashSet("s", "p", "o"), node.getAssuredBindingNames());
        Assert.assertEquals(2, node.getPipeline().size());
        // All constants
        sp = new StatementPattern(constant(VF.createIRI("urn:Alice")), constant(RDF.TYPE), constant(UNDERGRAD));
        node = new AggregationPipelineQueryNode(collection, sp);
        Assert.assertEquals(Sets.newHashSet(), node.getBindingNames());
        Assert.assertEquals(Sets.newHashSet(), node.getAssuredBindingNames());
        Assert.assertEquals(2, node.getPipeline().size());
        // Mixture
        sp = new StatementPattern(new Var("student"), constant(RDF.TYPE), constant(UNDERGRAD));
        node = new AggregationPipelineQueryNode(collection, sp);
        Assert.assertEquals(Sets.newHashSet("student"), node.getBindingNames());
        Assert.assertEquals(Sets.newHashSet("student"), node.getAssuredBindingNames());
        Assert.assertEquals(2, node.getPipeline().size());
    }

    @Test
    public void testJoin() throws Exception {
        final AggregationPipelineQueryNode base = new AggregationPipelineQueryNode(
                collection,
                new LinkedList<>(),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y", "opt"),
                HashBiMap.create());
        // Join on one shared variable
        AggregationPipelineQueryNode node = base.clone();
        boolean success = node.joinWith(new StatementPattern(new Var("x"), constant(TAKES), new Var("c")));
        Assert.assertTrue(success);
        Assert.assertEquals(Sets.newHashSet("x", "y", "c", "opt"), node.getBindingNames());
        Assert.assertEquals(Sets.newHashSet("x", "y", "c"), node.getAssuredBindingNames());
        Assert.assertEquals(4, node.getPipeline().size());
        // Join on multiple shared variables
        node = base.clone();
        success = node.joinWith(new StatementPattern(new Var("x"), constant(TAKES), new Var("y")));
        Assert.assertTrue(success);
        Assert.assertEquals(Sets.newHashSet("x", "y", "opt"), node.getBindingNames());
        Assert.assertEquals(Sets.newHashSet("x", "y"), node.getAssuredBindingNames());
        Assert.assertEquals(5, node.getPipeline().size());
    }

    @Test
    public void testProject() {
        final AggregationPipelineQueryNode base = new AggregationPipelineQueryNode(
                collection,
                new LinkedList<>(),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y", "opt"),
                HashBiMap.create());
        // Add a single projection
        ProjectionElemList singleProjection = new ProjectionElemList();
        singleProjection.addElement(new ProjectionElem("x", "z"));
        singleProjection.addElement(new ProjectionElem("y", "y"));
        List<ProjectionElemList> projections = Arrays.asList(singleProjection);
        AggregationPipelineQueryNode node = base.clone();
        boolean success = node.project(projections);
        Assert.assertTrue(success);
        Assert.assertEquals(1, node.getPipeline().size());
        Assert.assertEquals(Sets.newHashSet("z", "y"),
                node.getAssuredBindingNames());
        Assert.assertEquals(Sets.newHashSet("z", "y"),
                node.getBindingNames());
        // Add a multi-projection
        ProjectionElemList p1 = new ProjectionElemList();
        p1.addElement(new ProjectionElem("x", "solution"));
        ProjectionElemList p2 = new ProjectionElemList();
        p2.addElement(new ProjectionElem("y", "solution"));
        ProjectionElemList p3 = new ProjectionElemList();
        p3.addElement(new ProjectionElem("x", "x"));
        p3.addElement(new ProjectionElem("x", "solution"));
        p3.addElement(new ProjectionElem("y", "y"));
        projections = Arrays.asList(p1, p2, p3);
        node = base.clone();
        success = node.project(projections);
        Assert.assertTrue(success);
        Assert.assertEquals(3, node.getPipeline().size());
        Assert.assertEquals(Sets.newHashSet("solution"),
                node.getAssuredBindingNames());
        Assert.assertEquals(Sets.newHashSet("x", "y", "solution"),
                node.getBindingNames());
        // Add no projections
        node = base.clone();
        success = node.project(Arrays.asList());
        Assert.assertFalse(success);
        Assert.assertEquals(base, node);
    }

    @Test
    public void testExtend() {
        final AggregationPipelineQueryNode base = new AggregationPipelineQueryNode(
                collection,
                new LinkedList<>(),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y", "opt"),
                HashBiMap.create());
        // Extend with a mix of variables and constants
        List<ExtensionElem> extensionElements = Arrays.asList(
                new ExtensionElem(new Var("x"), "subject"),
                new ExtensionElem(new ValueConstant(RDF.TYPE), "predicate"),
                new ExtensionElem(new Var("y"), "object"));
        AggregationPipelineQueryNode node = base.clone();
        boolean success = node.extend(extensionElements);
        Assert.assertTrue(success);
        Assert.assertEquals(1, node.getPipeline().size());
        Assert.assertEquals(Sets.newHashSet("x", "y", "subject", "predicate", "object"),
                node.getAssuredBindingNames());
        Assert.assertEquals(Sets.newHashSet("x", "y", "subject", "predicate", "object", "opt"),
                node.getBindingNames());
        // Attempt to extend with an unsupported expression
        extensionElements = Arrays.asList(
                new ExtensionElem(new Var("x"), "subject"),
                new ExtensionElem(new Not(new ValueConstant(VF.createLiteral(true))), "notTrue"));
        node = base.clone();
        success = node.extend(extensionElements);
        Assert.assertFalse(success);
        Assert.assertEquals(base, node);
    }

    @Test
    public void testDistinct() {
        final AggregationPipelineQueryNode base = new AggregationPipelineQueryNode(
                collection,
                new LinkedList<>(),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y", "opt"),
                HashBiMap.create());
        AggregationPipelineQueryNode node = base.clone();
        boolean success = node.distinct();
        Assert.assertTrue(success);
        Assert.assertEquals(Sets.newHashSet("x", "y", "opt"), node.getBindingNames());
        Assert.assertEquals(Sets.newHashSet("x", "y"), node.getAssuredBindingNames());
        Assert.assertEquals(1, node.getPipeline().size());
    }

    @Test
    public void testFilter() {
        final AggregationPipelineQueryNode base = new AggregationPipelineQueryNode(
                collection,
                new LinkedList<>(),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y", "opt"),
                HashBiMap.create());
        // Extend with a supported filter
        AggregationPipelineQueryNode node = base.clone();
        boolean success = node.filter(new Compare(new Var("x"), new Var("y"), Compare.CompareOp.EQ));
        Assert.assertTrue(success);
        Assert.assertEquals(Sets.newHashSet("x", "y", "opt"), node.getBindingNames());
        Assert.assertEquals(Sets.newHashSet("x", "y"), node.getAssuredBindingNames());
        Assert.assertEquals(3, node.getPipeline().size());
        // Extend with an unsupported filter
        node = base.clone();
        success = node.filter(new IsLiteral(new Var("opt")));
        Assert.assertFalse(success);
        Assert.assertEquals(Sets.newHashSet("x", "y", "opt"), node.getBindingNames());
        Assert.assertEquals(Sets.newHashSet("x", "y"), node.getAssuredBindingNames());
        Assert.assertEquals(0, node.getPipeline().size());
    }

    @Test
    public void testRequireSourceDerivationLevel() throws Exception {
        final AggregationPipelineQueryNode base = new AggregationPipelineQueryNode(
                collection,
                new LinkedList<>(),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y", "opt"),
                HashBiMap.create());
        // Extend with a level greater than zero
        AggregationPipelineQueryNode node = base.clone();
        node.requireSourceDerivationDepth(3);
        Assert.assertEquals(Sets.newHashSet("x", "y", "opt"), node.getBindingNames());
        Assert.assertEquals(Sets.newHashSet("x", "y"), node.getAssuredBindingNames());
        Assert.assertEquals(1, node.getPipeline().size());
        // Extend with a level of zero (no effect)
        node = base.clone();
        node.requireSourceDerivationDepth(0);
        Assert.assertEquals(Sets.newHashSet("x", "y", "opt"), node.getBindingNames());
        Assert.assertEquals(Sets.newHashSet("x", "y"), node.getAssuredBindingNames());
        Assert.assertEquals(0, node.getPipeline().size());
    }

    @Test
    public void testRequireSourceTimestamp() {
        final AggregationPipelineQueryNode base = new AggregationPipelineQueryNode(
                collection,
                new LinkedList<>(),
                Sets.newHashSet("x", "y"),
                Sets.newHashSet("x", "y", "opt"),
                HashBiMap.create());
        // Extend with a level greater than zero
        AggregationPipelineQueryNode node = base.clone();
        node.requireSourceTimestamp(System.currentTimeMillis());
        Assert.assertEquals(Sets.newHashSet("x", "y", "opt"), node.getBindingNames());
        Assert.assertEquals(Sets.newHashSet("x", "y"), node.getAssuredBindingNames());
        Assert.assertEquals(1, node.getPipeline().size());
    }
}
