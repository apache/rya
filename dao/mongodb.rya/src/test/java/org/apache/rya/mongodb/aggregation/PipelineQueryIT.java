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

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.MongoITBase;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.model.vocabulary.OWL;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import info.aduna.iteration.CloseableIteration;

public class PipelineQueryIT extends MongoITBase {

    private static ValueFactory VF = ValueFactoryImpl.getInstance();
    private static SPARQLParser PARSER = new SPARQLParser();

    private MongoDBRyaDAO dao;

    @Before
    @Override
    public void setupTest() throws Exception {
        super.setupTest();
        dao = new MongoDBRyaDAO();
        dao.setConf(conf);
        dao.init();
    }

    private void insert(Resource subject, URI predicate, Value object) throws RyaDAOException {
        insert(subject, predicate, object, 0);
    }

    private void insert(Resource subject, URI predicate, Value object, int derivationLevel) throws RyaDAOException {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setSubject(RdfToRyaConversions.convertResource(subject));
        builder.setPredicate(RdfToRyaConversions.convertURI(predicate));
        builder.setObject(RdfToRyaConversions.convertValue(object));
        final RyaStatement rstmt = builder.build();
        if (derivationLevel > 0) {
            DBObject obj = new SimpleMongoDBStorageStrategy().serialize(builder.build());
            obj.put("derivation_level", derivationLevel);
            getRyaDbCollection().insert(obj);
        }
        else {
            dao.add(rstmt);
        }
    }

    private void testPipelineQuery(String query, Multiset<BindingSet> expectedSolutions) throws Exception {
        // Prepare query and convert to pipeline
        QueryRoot queryTree = new QueryRoot(PARSER.parseQuery(query, null).getTupleExpr());
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(getRyaCollection());
        queryTree.visit(visitor);
        // Execute pipeline and verify results
        Assert.assertTrue(queryTree.getArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) queryTree.getArg();
        Multiset<BindingSet> solutions = HashMultiset.create();
        CloseableIteration<BindingSet, QueryEvaluationException> iter = pipelineNode.evaluate(new QueryBindingSet());
        while (iter.hasNext()) {
            solutions.add(iter.next());
        }
        Assert.assertEquals(expectedSolutions, solutions);
    }

    @Test
    public void testSingleStatementPattern() throws Exception {
        // Insert data
        insert(OWL.THING, RDF.TYPE, OWL.CLASS);
        insert(FOAF.PERSON, RDF.TYPE, OWL.CLASS, 1);
        insert(FOAF.PERSON, RDFS.SUBCLASSOF, OWL.THING);
        insert(VF.createURI("urn:Alice"), RDF.TYPE, FOAF.PERSON);
        dao.flush();
        // Define query and expected results
        final String query = "SELECT * WHERE {\n"
                + "  ?individual a ?type .\n"
                + "}";
        List<String> varNames = Arrays.asList("individual", "type");
        Multiset<BindingSet> expectedSolutions = HashMultiset.create();
        expectedSolutions.add(new ListBindingSet(varNames, OWL.THING, OWL.CLASS));
        expectedSolutions.add(new ListBindingSet(varNames, FOAF.PERSON, OWL.CLASS));
        expectedSolutions.add(new ListBindingSet(varNames, VF.createURI("urn:Alice"), FOAF.PERSON));
        // Execute pipeline and verify results
        testPipelineQuery(query, expectedSolutions);
    }

    @Test
    public void testNoVariableSP() throws Exception {
        // Insert data
        insert(OWL.THING, RDF.TYPE, OWL.CLASS);
        insert(FOAF.PERSON, RDF.TYPE, OWL.CLASS, 1);
        insert(FOAF.PERSON, RDFS.SUBCLASSOF, OWL.THING);
        insert(VF.createURI("urn:Alice"), RDF.TYPE, FOAF.PERSON);
        dao.flush();
        // Define query and expected results
        final String query = "SELECT * WHERE {\n"
                + "  owl:Thing a owl:Class .\n"
                + "}";
        Multiset<BindingSet> expectedSolutions = HashMultiset.create();
        expectedSolutions.add(new EmptyBindingSet());
        // Execute pipeline and verify results
        QueryRoot queryTree = new QueryRoot(PARSER.parseQuery(query, null).getTupleExpr());
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(getRyaCollection());
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof Projection);
        Projection projection = (Projection) queryTree.getArg();
        Assert.assertTrue(projection.getArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) projection.getArg();
        Multiset<BindingSet> solutions = HashMultiset.create();
        CloseableIteration<BindingSet, QueryEvaluationException> iter = pipelineNode.evaluate(new QueryBindingSet());
        while (iter.hasNext()) {
            solutions.add(iter.next());
        }
        Assert.assertEquals(expectedSolutions, solutions);
    }

    @Test
    public void testJoinTwoSharedVariables() throws Exception {
        // Insert data
        URI person = VF.createURI("urn:Person");
        URI livingThing = VF.createURI("urn:LivingThing");
        URI human = VF.createURI("urn:Human");
        URI programmer = VF.createURI("urn:Programmer");
        URI thing = VF.createURI("urn:Thing");
        insert(programmer, RDFS.SUBCLASSOF, person);
        insert(person, RDFS.SUBCLASSOF, FOAF.PERSON);
        insert(FOAF.PERSON, RDFS.SUBCLASSOF, person);
        insert(person, OWL.EQUIVALENTCLASS, human);
        insert(person, RDFS.SUBCLASSOF, livingThing);
        insert(livingThing, RDFS.SUBCLASSOF, thing);
        insert(thing, RDFS.SUBCLASSOF, OWL.THING);
        insert(OWL.THING, RDFS.SUBCLASSOF, thing);
        dao.flush();
        // Define query and expected results
        final String query = "SELECT ?A ?B WHERE {\n"
                + "  ?A rdfs:subClassOf ?B .\n"
                + "  ?B rdfs:subClassOf ?A .\n"
                + "}";
        List<String> varNames = Arrays.asList("A", "B");
        Multiset<BindingSet> expectedSolutions = HashMultiset.create();
        expectedSolutions.add(new ListBindingSet(varNames, person, FOAF.PERSON));
        expectedSolutions.add(new ListBindingSet(varNames, FOAF.PERSON, person));
        expectedSolutions.add(new ListBindingSet(varNames, thing, OWL.THING));
        expectedSolutions.add(new ListBindingSet(varNames, OWL.THING, thing));
        // Execute query and verify results
        testPipelineQuery(query, expectedSolutions);
    }

    @Test
    public void testVariableRename() throws Exception {
        // Insert data
        URI alice = VF.createURI("urn:Alice");
        URI bob = VF.createURI("urn:Bob");
        URI carol = VF.createURI("urn:Carol");
        URI dan = VF.createURI("urn:Dan");
        URI eve = VF.createURI("urn:Eve");
        URI friend = VF.createURI("urn:friend");
        insert(alice, friend, bob);
        insert(alice, friend, carol);
        insert(bob, friend, eve);
        insert(carol, friend, eve);
        insert(dan, friend, carol);
        insert(eve, friend, alice);
        // Define non-distinct query and expected results
        final String query1 = "SELECT ?x (?z as ?friendOfFriend) WHERE {\n"
                + "  ?x <urn:friend> ?y .\n"
                + "  ?y <urn:friend> ?z .\n"
                + "}";
        Multiset<BindingSet> expectedSolutions1 = HashMultiset.create();
        List<String> varNames = Arrays.asList("x", "friendOfFriend");
        expectedSolutions1.add(new ListBindingSet(varNames, alice, eve));
        expectedSolutions1.add(new ListBindingSet(varNames, alice, eve));
        expectedSolutions1.add(new ListBindingSet(varNames, bob, alice));
        expectedSolutions1.add(new ListBindingSet(varNames, carol, alice));
        expectedSolutions1.add(new ListBindingSet(varNames, dan, eve));
        expectedSolutions1.add(new ListBindingSet(varNames, eve, bob));
        expectedSolutions1.add(new ListBindingSet(varNames, eve, carol));
        // Define distinct query and expected results
        final String query2 = "SELECT DISTINCT ?x (?z as ?friendOfFriend) WHERE {\n"
                + "  ?x <urn:friend> ?y .\n"
                + "  ?y <urn:friend> ?z .\n"
                + "}";
        Multiset<BindingSet> expectedSolutions2 = HashMultiset.create();
        expectedSolutions2.add(new ListBindingSet(varNames, alice, eve));
        expectedSolutions2.add(new ListBindingSet(varNames, bob, alice));
        expectedSolutions2.add(new ListBindingSet(varNames, carol, alice));
        expectedSolutions2.add(new ListBindingSet(varNames, dan, eve));
        expectedSolutions2.add(new ListBindingSet(varNames, eve, bob));
        expectedSolutions2.add(new ListBindingSet(varNames, eve, carol));
        // Execute and verify results
        testPipelineQuery(query1, expectedSolutions1);
        testPipelineQuery(query2, expectedSolutions2);
    }

    @Test
    public void testFilterQuery() throws Exception {
        // Insert data
        URI alice = VF.createURI("urn:Alice");
        URI bob = VF.createURI("urn:Bob");
        URI eve = VF.createURI("urn:Eve");
        URI relatedTo = VF.createURI("urn:relatedTo");
        insert(alice, FOAF.KNOWS, bob);
        insert(alice, FOAF.KNOWS, alice);
        insert(alice, FOAF.KNOWS, eve);
        insert(alice, relatedTo, bob);
        insert(bob, FOAF.KNOWS, eve);
        insert(bob, relatedTo, bob);
        dao.flush();
        // Define query 1 and expected results
        final String query1 = "SELECT * WHERE {\n"
                + "  ?x <" + FOAF.KNOWS.stringValue() + "> ?y1 .\n"
                + "  ?x <" + relatedTo.stringValue() + "> ?y2 .\n"
                + "  FILTER (?y1 != ?y2) .\n"
                + "}";
        final List<String> varNames = Arrays.asList("x", "y1", "y2");
        final Multiset<BindingSet> expected1 = HashMultiset.create();
        expected1.add(new ListBindingSet(varNames, alice, alice, bob));
        expected1.add(new ListBindingSet(varNames, alice, eve, bob));
        expected1.add(new ListBindingSet(varNames, bob, eve, bob));
        // Define query 2 and expected results
        final String query2 = "SELECT * WHERE {\n"
                + "  ?x <" + FOAF.KNOWS.stringValue() + "> ?y1 .\n"
                + "  ?x <" + relatedTo.stringValue() + "> ?y2 .\n"
                + "  FILTER (?y1 = ?y2) .\n"
                + "}";
        final Multiset<BindingSet> expected2 = HashMultiset.create();
        expected2.add(new ListBindingSet(varNames, alice, bob, bob));
        // Execute and verify results
        testPipelineQuery(query1, expected1);
        testPipelineQuery(query2, expected2);
    }

    @Test
    public void testMultiConstruct() throws Exception {
        // Insert data
        URI alice = VF.createURI("urn:Alice");
        URI bob = VF.createURI("urn:Bob");
        URI eve = VF.createURI("urn:Eve");
        URI friend = VF.createURI("urn:friend");
        URI knows = VF.createURI("urn:knows");
        URI person = VF.createURI("urn:Person");
        insert(alice, friend, bob);
        insert(bob, knows, eve);
        insert(eve, knows, alice);
        // Define query and expected results
        final String query = "CONSTRUCT {\n"
                + "    ?x rdf:type owl:Thing .\n"
                + "    ?x rdf:type <urn:Person> .\n"
                + "} WHERE { ?x <urn:knows> ?y }";
        final Multiset<BindingSet> expected = HashMultiset.create();
        List<String> varNames = Arrays.asList("subject", "predicate", "object");
        expected.add(new ListBindingSet(varNames, bob, RDF.TYPE, OWL.THING));
        expected.add(new ListBindingSet(varNames, bob, RDF.TYPE, person));
        expected.add(new ListBindingSet(varNames, eve, RDF.TYPE, OWL.THING));
        expected.add(new ListBindingSet(varNames, eve, RDF.TYPE, person));
        // Test query
        testPipelineQuery(query, expected);
    }

    @Test
    public void testTriplePipeline() throws Exception {
        URI alice = VF.createURI("urn:Alice");
        URI bob = VF.createURI("urn:Bob");
        URI eve = VF.createURI("urn:Eve");
        URI friend = VF.createURI("urn:friend");
        URI knows = VF.createURI("urn:knows");
        URI year = VF.createURI("urn:year");
        Literal yearLiteral = VF.createLiteral("2017", XMLSchema.GYEAR);
        final String query = "CONSTRUCT {\n"
                + "    ?x <urn:knows> ?y .\n"
                + "    ?x <urn:year> \"2017\"^^<" + XMLSchema.GYEAR + "> .\n"
                + "} WHERE { ?x <urn:friend> ?y }";
        insert(alice, friend, bob);
        insert(bob, knows, eve);
        insert(eve, knows, alice);
        // Prepare query and convert to pipeline
        QueryRoot queryTree = new QueryRoot(PARSER.parseQuery(query, null).getTupleExpr());
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(getRyaCollection());
        queryTree.visit(visitor);
        // Get pipeline, add triple conversion, and verify that the result is a
        // properly serialized statement
        Assert.assertTrue(queryTree.getArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) queryTree.getArg();
        List<Bson> triplePipeline = pipelineNode.getTriplePipeline(System.currentTimeMillis(), false);
        SimpleMongoDBStorageStrategy strategy = new SimpleMongoDBStorageStrategy();
        List<Statement> results = new LinkedList<>();
        for (Document doc : getRyaCollection().aggregate(triplePipeline)) {
            final DBObject dbo = (DBObject) JSON.parse(doc.toJson());
            RyaStatement rstmt = strategy.deserializeDBObject(dbo);
            Statement stmt = RyaToRdfConversions.convertStatement(rstmt);
            results.add(stmt);
        }
        Assert.assertEquals(2, results.size());
        Assert.assertTrue(results.contains(VF.createStatement(alice, knows, bob)));
        Assert.assertTrue(results.contains(VF.createStatement(alice, year, yearLiteral)));
    }

    @Test
    public void testRequiredDerivationLevel() throws Exception {
        // Insert data
        URI person = VF.createURI("urn:Person");
        URI livingThing = VF.createURI("urn:LivingThing");
        URI human = VF.createURI("urn:Human");
        URI programmer = VF.createURI("urn:Programmer");
        URI thing = VF.createURI("urn:Thing");
        insert(programmer, RDFS.SUBCLASSOF, person);
        insert(person, RDFS.SUBCLASSOF, FOAF.PERSON);
        insert(FOAF.PERSON, RDFS.SUBCLASSOF, person);
        insert(person, OWL.EQUIVALENTCLASS, human);
        insert(person, RDFS.SUBCLASSOF, livingThing);
        insert(livingThing, RDFS.SUBCLASSOF, thing);
        insert(thing, RDFS.SUBCLASSOF, OWL.THING, 1);
        insert(OWL.THING, RDFS.SUBCLASSOF, thing);
        dao.flush();
        // Define query and expected results
        final String query = "SELECT ?A ?B WHERE {\n"
                + "  ?A rdfs:subClassOf ?B .\n"
                + "  ?B rdfs:subClassOf ?A .\n"
                + "}";
        List<String> varNames = Arrays.asList("A", "B");
        Multiset<BindingSet> expectedSolutions = HashMultiset.create();
        expectedSolutions.add(new ListBindingSet(varNames, person, FOAF.PERSON));
        expectedSolutions.add(new ListBindingSet(varNames, FOAF.PERSON, person));
        expectedSolutions.add(new ListBindingSet(varNames, thing, OWL.THING));
        expectedSolutions.add(new ListBindingSet(varNames, OWL.THING, thing));
        // Prepare query and convert to pipeline
        QueryRoot queryTree = new QueryRoot(PARSER.parseQuery(query, null).getTupleExpr());
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(getRyaCollection());
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) queryTree.getArg();
        // Extend the pipeline by requiring a derivation level of zero (should have no effect)
        pipelineNode.requireSourceDerivationDepth(0);
        Multiset<BindingSet> solutions = HashMultiset.create();
        CloseableIteration<BindingSet, QueryEvaluationException> iter = pipelineNode.evaluate(new QueryBindingSet());
        while (iter.hasNext()) {
            solutions.add(iter.next());
        }
        Assert.assertEquals(expectedSolutions, solutions);
        // Extend the pipeline by requiring a derivation level of one (should produce the thing/thing pair)
        expectedSolutions = HashMultiset.create();
        expectedSolutions.add(new ListBindingSet(varNames, thing, OWL.THING));
        expectedSolutions.add(new ListBindingSet(varNames, OWL.THING, thing));
        pipelineNode.requireSourceDerivationDepth(1);
        solutions = HashMultiset.create();
        iter = pipelineNode.evaluate(new QueryBindingSet());
        while (iter.hasNext()) {
            solutions.add(iter.next());
        }
        Assert.assertEquals(expectedSolutions, solutions);
    }

    @Test
    public void testRequiredTimestamp() throws Exception {
        // Insert data
        URI person = VF.createURI("urn:Person");
        URI livingThing = VF.createURI("urn:LivingThing");
        URI human = VF.createURI("urn:Human");
        URI programmer = VF.createURI("urn:Programmer");
        URI thing = VF.createURI("urn:Thing");
        insert(programmer, RDFS.SUBCLASSOF, person);
        insert(person, RDFS.SUBCLASSOF, FOAF.PERSON, 2);
        insert(FOAF.PERSON, RDFS.SUBCLASSOF, person);
        insert(person, OWL.EQUIVALENTCLASS, human);
        insert(person, RDFS.SUBCLASSOF, livingThing);
        insert(livingThing, RDFS.SUBCLASSOF, thing);
        insert(thing, RDFS.SUBCLASSOF, OWL.THING);
        insert(OWL.THING, RDFS.SUBCLASSOF, thing);
        dao.flush();
        // Define query and expected results
        final String query = "SELECT ?A ?B WHERE {\n"
                + "  ?A rdfs:subClassOf ?B .\n"
                + "  ?B rdfs:subClassOf ?A .\n"
                + "}";
        List<String> varNames = Arrays.asList("A", "B");
        Multiset<BindingSet> expectedSolutions = HashMultiset.create();
        expectedSolutions.add(new ListBindingSet(varNames, person, FOAF.PERSON));
        expectedSolutions.add(new ListBindingSet(varNames, FOAF.PERSON, person));
        expectedSolutions.add(new ListBindingSet(varNames, thing, OWL.THING));
        expectedSolutions.add(new ListBindingSet(varNames, OWL.THING, thing));
        // Prepare query and convert to pipeline
        QueryRoot queryTree = new QueryRoot(PARSER.parseQuery(query, null).getTupleExpr());
        SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(getRyaCollection());
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof AggregationPipelineQueryNode);
        AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) queryTree.getArg();
        // Extend the pipeline by requiring a timestamp of zero (should have no effect)
        pipelineNode.requireSourceTimestamp(0);
        Multiset<BindingSet> solutions = HashMultiset.create();
        CloseableIteration<BindingSet, QueryEvaluationException> iter = pipelineNode.evaluate(new QueryBindingSet());
        while (iter.hasNext()) {
            solutions.add(iter.next());
        }
        Assert.assertEquals(expectedSolutions, solutions);
        // Extend the pipeline by requiring a future timestamp (should produce no results)
        long delta = 1000 * 60 * 60 * 24;
        pipelineNode.requireSourceTimestamp(System.currentTimeMillis() + delta);
        iter = pipelineNode.evaluate(new QueryBindingSet());
        Assert.assertFalse(iter.hasNext());
    }
}
