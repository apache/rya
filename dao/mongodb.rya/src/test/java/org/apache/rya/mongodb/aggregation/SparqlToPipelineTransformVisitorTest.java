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
import java.util.List;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.algebra.Extension;
import org.openrdf.query.algebra.ExtensionElem;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Not;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.Var;

import com.google.common.collect.Sets;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;

public class SparqlToPipelineTransformVisitorTest {

    private static final ValueFactory VF = ValueFactoryImpl.getInstance();

    private static final String LUBM = "urn:lubm";
    private static final URI UNDERGRAD = VF.createURI(LUBM, "UndergraduateStudent");
    private static final URI PROFESSOR = VF.createURI(LUBM, "Professor");
    private static final URI COURSE = VF.createURI(LUBM, "Course");
    private static final URI TAKES = VF.createURI(LUBM, "takesCourse");
    private static final URI TEACHES = VF.createURI(LUBM, "teachesCourse");

    private static Var constant(URI value) {
        return new Var(value.stringValue(), value);
    }

    MongoCollection<Document> collection;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        collection = Mockito.mock(MongoCollection.class);
        Mockito.when(collection.getNamespace()).thenReturn(new MongoNamespace("db", "collection"));
    }

    @Test
    public void testStatementPattern() throws Exception {
        QueryRoot query = new QueryRoot(new StatementPattern(
                new Var("x"), constant(RDF.TYPE), constant(UNDERGRAD)));
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(collection);
        query.visit(visitor);
        Assert.assertTrue(query.getArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) query.getArg();
        Assert.assertEquals(Sets.newHashSet("x"), pipelineNode.getAssuredBindingNames());
    }

    @Test
    public void testJoin() throws Exception {
        QueryRoot query = new QueryRoot(new Join(
                new StatementPattern(new Var("x"), constant(RDF.TYPE), constant(UNDERGRAD)),
                new StatementPattern(new Var("x"), constant(TAKES), new Var("course"))));
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(collection);
        query.visit(visitor);
        Assert.assertTrue(query.getArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) query.getArg();
        Assert.assertEquals(Sets.newHashSet("x", "course"), pipelineNode.getAssuredBindingNames());
    }

    @Test
    public void testNestedJoins() throws Exception {
        StatementPattern isUndergrad = new StatementPattern(new Var("x"), constant(RDF.TYPE), constant(UNDERGRAD));
        StatementPattern isProfessor = new StatementPattern(new Var("y"), constant(RDF.TYPE), constant(PROFESSOR));
        StatementPattern takesCourse = new StatementPattern(new Var("x"), constant(TAKES), new Var("c"));
        StatementPattern teachesCourse = new StatementPattern(new Var("y"), constant(TEACHES), new Var("c"));
        QueryRoot queryTree = new QueryRoot(new Join(
                isProfessor,
                new Join(
                        new Join(isUndergrad, takesCourse),
                        teachesCourse)));
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(collection);
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) queryTree.getArg();
        Assert.assertEquals(Sets.newHashSet("x", "y", "c"), pipelineNode.getAssuredBindingNames());
    }

    @Test
    public void testComplexJoin() throws Exception {
        StatementPattern isUndergrad = new StatementPattern(new Var("x"), constant(RDF.TYPE), constant(UNDERGRAD));
        StatementPattern isProfessor = new StatementPattern(new Var("y"), constant(RDF.TYPE), constant(PROFESSOR));
        StatementPattern takesCourse = new StatementPattern(new Var("x"), constant(TAKES), new Var("c"));
        StatementPattern teachesCourse = new StatementPattern(new Var("y"), constant(TEACHES), new Var("c"));
        QueryRoot queryTree = new QueryRoot(new Join(
                new Join(isUndergrad, takesCourse),
                new Join(isProfessor, teachesCourse)));
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(collection);
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof Join);
        Join topJoin = (Join) queryTree.getArg();
        Assert.assertTrue(topJoin.getLeftArg() instanceof AggregationPipelineQueryNode);
        Assert.assertTrue(topJoin.getRightArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode leftPipeline = (AggregationPipelineQueryNode) topJoin.getLeftArg();
        AggregationPipelineQueryNode rightPipeline = (AggregationPipelineQueryNode) topJoin.getRightArg();
        Assert.assertEquals(Sets.newHashSet("x", "c"), leftPipeline.getAssuredBindingNames());
        Assert.assertEquals(Sets.newHashSet("y", "c"), rightPipeline.getAssuredBindingNames());
    }

    @Test
    public void testProjection() throws Exception {
        StatementPattern isUndergrad = new StatementPattern(new Var("x"), constant(RDF.TYPE), constant(UNDERGRAD));
        StatementPattern isCourse = new StatementPattern(new Var("course"), constant(RDF.TYPE), constant(COURSE));
        StatementPattern hasEdge = new StatementPattern(new Var("x"), new Var("p"), new Var("course"));
        ProjectionElemList projectionElements = new ProjectionElemList(
                new ProjectionElem("p", "relation"),
                new ProjectionElem("course"));
        QueryRoot queryTree = new QueryRoot(new Projection(
                new Join(new Join(isCourse, hasEdge), isUndergrad),
                projectionElements));
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(collection);
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) queryTree.getArg();
        Assert.assertEquals(Sets.newHashSet("relation", "course"), pipelineNode.getAssuredBindingNames());
    }

    @Test
    public void testEmptyProjection() throws Exception {
        StatementPattern isClass = new StatementPattern(constant(UNDERGRAD), constant(RDF.TYPE), constant(OWL.CLASS));
        QueryRoot queryTree = new QueryRoot(new Projection(isClass, new ProjectionElemList()));
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(collection);
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof Projection);
        Projection projectNode = (Projection) queryTree.getArg();
        Assert.assertTrue(projectNode.getArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) projectNode.getArg();
        Assert.assertEquals(Sets.newHashSet(), pipelineNode.getAssuredBindingNames());
    }

    @Test
    public void testMultiProjection() throws Exception {
        StatementPattern isUndergrad = new StatementPattern(new Var("x"), constant(RDF.TYPE), constant(UNDERGRAD));
        StatementPattern isCourse = new StatementPattern(new Var("course"), constant(RDF.TYPE), constant(COURSE));
        StatementPattern hasEdge = new StatementPattern(new Var("x"), new Var("p"), new Var("course"));
        ProjectionElemList courseHasRelation = new ProjectionElemList(
                new ProjectionElem("p", "relation"),
                new ProjectionElem("course"));
        ProjectionElemList studentHasRelation = new ProjectionElemList(
                new ProjectionElem("p", "relation"),
                new ProjectionElem("x", "student"));
        QueryRoot queryTree = new QueryRoot(new MultiProjection(
                new Join(new Join(isCourse, hasEdge), isUndergrad),
                Arrays.asList(courseHasRelation, studentHasRelation)));
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(collection);
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) queryTree.getArg();
        Assert.assertEquals(Sets.newHashSet("relation"), pipelineNode.getAssuredBindingNames());
        Assert.assertEquals(Sets.newHashSet("relation", "course", "student"), pipelineNode.getBindingNames());
    }

    @Test
    public void testExtension() throws Exception {
        QueryRoot queryTree = new QueryRoot(new Extension(
                new StatementPattern(new Var("x"), constant(TAKES), new Var("c")),
                new ExtensionElem(new Var("x"), "renamed"),
                new ExtensionElem(new ValueConstant(TAKES), "constant")));
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(collection);
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) queryTree.getArg();
        Assert.assertEquals(Sets.newHashSet("x", "c", "renamed", "constant"), pipelineNode.getAssuredBindingNames());
    }

    @Test
    public void testUnsupportedExtension() throws Exception {
        StatementPattern sp = new StatementPattern(new Var("x"), constant(TAKES), new Var("c"));
        List<ExtensionElem> elements = Arrays.asList(new ExtensionElem(new Var("x"), "renamed"),
                new ExtensionElem(new Not(new ValueConstant(VF.createLiteral(true))), "notTrue"),
                new ExtensionElem(new ValueConstant(TAKES), "constant"));
        Extension extensionNode = new Extension(sp, elements);
        QueryRoot queryTree = new QueryRoot(extensionNode);
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(collection);
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof Extension);
        Assert.assertEquals(elements, ((Extension) queryTree.getArg()).getElements());
        TupleExpr innerQuery = ((Extension) queryTree.getArg()).getArg();
        Assert.assertTrue(innerQuery instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) innerQuery;
        Assert.assertEquals(Sets.newHashSet("x", "c"), pipelineNode.getAssuredBindingNames());
    }
}
