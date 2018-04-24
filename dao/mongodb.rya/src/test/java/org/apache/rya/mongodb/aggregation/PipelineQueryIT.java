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
import org.apache.rya.mongodb.MongoRyaITBase;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.OWL;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.query.impl.ListBindingSet;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class PipelineQueryIT extends MongoRyaITBase {

    private static final ValueFactory VF = SimpleValueFactory.getInstance();
    private static final SPARQLParser PARSER = new SPARQLParser();

    private MongoDBRyaDAO dao;

    @Before
    @Override
    public void setupTest() throws Exception {
        super.setupTest();
        dao = new MongoDBRyaDAO();
        dao.setConf(conf);
        dao.init();
    }

    private void insert(final Resource subject, final IRI predicate, final Value object) throws RyaDAOException {
        insert(subject, predicate, object, 0);
    }

    private void insert(final Resource subject, final IRI predicate, final Value object, final int derivationLevel) throws RyaDAOException {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setSubject(RdfToRyaConversions.convertResource(subject));
        builder.setPredicate(RdfToRyaConversions.convertIRI(predicate));
        builder.setObject(RdfToRyaConversions.convertValue(object));
        final RyaStatement rstmt = builder.build();
        if (derivationLevel > 0) {
            final DBObject obj = new SimpleMongoDBStorageStrategy().serialize(builder.build());
            obj.put("derivation_level", derivationLevel);
            getRyaDbCollection().insert(obj);
        }
        else {
            dao.add(rstmt);
        }
    }

    private void testPipelineQuery(final String query, final Multiset<BindingSet> expectedSolutions) throws Exception {
        // Prepare query and convert to pipeline
        final QueryRoot queryTree = new QueryRoot(PARSER.parseQuery(query, null).getTupleExpr());
        final SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(getRyaCollection());
        queryTree.visit(visitor);
        // Execute pipeline and verify results
        Assert.assertTrue(queryTree.getArg() instanceof AggregationPipelineQueryNode);
        final AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) queryTree.getArg();
        final Multiset<BindingSet> solutions = HashMultiset.create();
        final CloseableIteration<BindingSet, QueryEvaluationException> iter = pipelineNode.evaluate(new QueryBindingSet());
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
        insert(VF.createIRI("urn:Alice"), RDF.TYPE, FOAF.PERSON);
        dao.flush();
        // Define query and expected results
        final String query = "SELECT * WHERE {\n"
                + "  ?individual a ?type .\n"
                + "}";
        final List<String> varNames = Arrays.asList("individual", "type");
        final Multiset<BindingSet> expectedSolutions = HashMultiset.create();
        expectedSolutions.add(new ListBindingSet(varNames, OWL.THING, OWL.CLASS));
        expectedSolutions.add(new ListBindingSet(varNames, FOAF.PERSON, OWL.CLASS));
        expectedSolutions.add(new ListBindingSet(varNames, VF.createIRI("urn:Alice"), FOAF.PERSON));
        // Execute pipeline and verify results
        testPipelineQuery(query, expectedSolutions);
    }

    @Test
    public void testNoVariableSP() throws Exception {
        // Insert data
        insert(OWL.THING, RDF.TYPE, OWL.CLASS);
        insert(FOAF.PERSON, RDF.TYPE, OWL.CLASS, 1);
        insert(FOAF.PERSON, RDFS.SUBCLASSOF, OWL.THING);
        insert(VF.createIRI("urn:Alice"), RDF.TYPE, FOAF.PERSON);
        dao.flush();
        // Define query and expected results
        final String query = "SELECT * WHERE {\n"
                + "  owl:Thing a owl:Class .\n"
                + "}";
        final Multiset<BindingSet> expectedSolutions = HashMultiset.create();
        expectedSolutions.add(new EmptyBindingSet());
        // Execute pipeline and verify results
        final QueryRoot queryTree = new QueryRoot(PARSER.parseQuery(query, null).getTupleExpr());
        final SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(getRyaCollection());
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof Projection);
        final Projection projection = (Projection) queryTree.getArg();
        Assert.assertTrue(projection.getArg() instanceof AggregationPipelineQueryNode);
        final AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) projection.getArg();
        final Multiset<BindingSet> solutions = HashMultiset.create();
        final CloseableIteration<BindingSet, QueryEvaluationException> iter = pipelineNode.evaluate(new QueryBindingSet());
        while (iter.hasNext()) {
            solutions.add(iter.next());
        }
        Assert.assertEquals(expectedSolutions, solutions);
    }

    @Test
    public void testJoinTwoSharedVariables() throws Exception {
        // Insert data
        final IRI person = VF.createIRI("urn:Person");
        final IRI livingThing = VF.createIRI("urn:LivingThing");
        final IRI human = VF.createIRI("urn:Human");
        final IRI programmer = VF.createIRI("urn:Programmer");
        final IRI thing = VF.createIRI("urn:Thing");
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
        final List<String> varNames = Arrays.asList("A", "B");
        final Multiset<BindingSet> expectedSolutions = HashMultiset.create();
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
        final IRI alice = VF.createIRI("urn:Alice");
        final IRI bob = VF.createIRI("urn:Bob");
        final IRI carol = VF.createIRI("urn:Carol");
        final IRI dan = VF.createIRI("urn:Dan");
        final IRI eve = VF.createIRI("urn:Eve");
        final IRI friend = VF.createIRI("urn:friend");
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
        final Multiset<BindingSet> expectedSolutions1 = HashMultiset.create();
        final List<String> varNames = Arrays.asList("x", "friendOfFriend");
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
        final Multiset<BindingSet> expectedSolutions2 = HashMultiset.create();
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
        final IRI alice = VF.createIRI("urn:Alice");
        final IRI bob = VF.createIRI("urn:Bob");
        final IRI eve = VF.createIRI("urn:Eve");
        final IRI relatedTo = VF.createIRI("urn:relatedTo");
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
        final IRI alice = VF.createIRI("urn:Alice");
        final IRI bob = VF.createIRI("urn:Bob");
        final IRI eve = VF.createIRI("urn:Eve");
        final IRI friend = VF.createIRI("urn:friend");
        final IRI knows = VF.createIRI("urn:knows");
        final IRI person = VF.createIRI("urn:Person");
        insert(alice, friend, bob);
        insert(bob, knows, eve);
        insert(eve, knows, alice);
        // Define query and expected results
        final String query = "CONSTRUCT {\n"
                + "    ?x rdf:type owl:Thing .\n"
                + "    ?x rdf:type <urn:Person> .\n"
                + "} WHERE { ?x <urn:knows> ?y }";
        final Multiset<BindingSet> expected = HashMultiset.create();
        final List<String> varNames = Arrays.asList("subject", "predicate", "object");
        expected.add(new ListBindingSet(varNames, bob, RDF.TYPE, OWL.THING));
        expected.add(new ListBindingSet(varNames, bob, RDF.TYPE, person));
        expected.add(new ListBindingSet(varNames, eve, RDF.TYPE, OWL.THING));
        expected.add(new ListBindingSet(varNames, eve, RDF.TYPE, person));
        // Test query
        testPipelineQuery(query, expected);
    }

    @Test
    public void testTriplePipeline() throws Exception {
        final IRI alice = VF.createIRI("urn:Alice");
        final IRI bob = VF.createIRI("urn:Bob");
        final IRI eve = VF.createIRI("urn:Eve");
        final IRI friend = VF.createIRI("urn:friend");
        final IRI knows = VF.createIRI("urn:knows");
        final IRI year = VF.createIRI("urn:year");
        final Literal yearLiteral = VF.createLiteral("2017", XMLSchema.GYEAR);
        final String query = "CONSTRUCT {\n"
                + "    ?x <urn:knows> ?y .\n"
                + "    ?x <urn:year> \"2017\"^^<" + XMLSchema.GYEAR + "> .\n"
                + "} WHERE { ?x <urn:friend> ?y }";
        insert(alice, friend, bob);
        insert(bob, knows, eve);
        insert(eve, knows, alice);
        // Prepare query and convert to pipeline
        final QueryRoot queryTree = new QueryRoot(PARSER.parseQuery(query, null).getTupleExpr());
        final SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(getRyaCollection());
        queryTree.visit(visitor);
        // Get pipeline, add triple conversion, and verify that the result is a
        // properly serialized statement
        Assert.assertTrue(queryTree.getArg() instanceof AggregationPipelineQueryNode);
        final AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) queryTree.getArg();
        final List<Bson> triplePipeline = pipelineNode.getTriplePipeline(System.currentTimeMillis(), false);
        final SimpleMongoDBStorageStrategy strategy = new SimpleMongoDBStorageStrategy();
        final List<Statement> results = new LinkedList<>();
        for (final Document doc : getRyaCollection().aggregate(triplePipeline)) {
            final DBObject dbo = (DBObject) JSON.parse(doc.toJson());
            final RyaStatement rstmt = strategy.deserializeDBObject(dbo);
            final Statement stmt = RyaToRdfConversions.convertStatement(rstmt);
            results.add(stmt);
        }
        Assert.assertEquals(2, results.size());
        Assert.assertTrue(results.contains(VF.createStatement(alice, knows, bob)));
        Assert.assertTrue(results.contains(VF.createStatement(alice, year, yearLiteral)));
    }

    @Test
    public void testRequiredDerivationLevel() throws Exception {
        // Insert data
        final IRI person = VF.createIRI("urn:Person");
        final IRI livingThing = VF.createIRI("urn:LivingThing");
        final IRI human = VF.createIRI("urn:Human");
        final IRI programmer = VF.createIRI("urn:Programmer");
        final IRI thing = VF.createIRI("urn:Thing");
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
        final List<String> varNames = Arrays.asList("A", "B");
        Multiset<BindingSet> expectedSolutions = HashMultiset.create();
        expectedSolutions.add(new ListBindingSet(varNames, person, FOAF.PERSON));
        expectedSolutions.add(new ListBindingSet(varNames, FOAF.PERSON, person));
        expectedSolutions.add(new ListBindingSet(varNames, thing, OWL.THING));
        expectedSolutions.add(new ListBindingSet(varNames, OWL.THING, thing));
        // Prepare query and convert to pipeline
        final QueryRoot queryTree = new QueryRoot(PARSER.parseQuery(query, null).getTupleExpr());
        final SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(getRyaCollection());
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof AggregationPipelineQueryNode);
        final AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) queryTree.getArg();
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
        final IRI person = VF.createIRI("urn:Person");
        final IRI livingThing = VF.createIRI("urn:LivingThing");
        final IRI human = VF.createIRI("urn:Human");
        final IRI programmer = VF.createIRI("urn:Programmer");
        final IRI thing = VF.createIRI("urn:Thing");
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
        final List<String> varNames = Arrays.asList("A", "B");
        final Multiset<BindingSet> expectedSolutions = HashMultiset.create();
        expectedSolutions.add(new ListBindingSet(varNames, person, FOAF.PERSON));
        expectedSolutions.add(new ListBindingSet(varNames, FOAF.PERSON, person));
        expectedSolutions.add(new ListBindingSet(varNames, thing, OWL.THING));
        expectedSolutions.add(new ListBindingSet(varNames, OWL.THING, thing));
        // Prepare query and convert to pipeline
        final QueryRoot queryTree = new QueryRoot(PARSER.parseQuery(query, null).getTupleExpr());
        final SparqlToPipelineTransformVisitor visitor = new SparqlToPipelineTransformVisitor(getRyaCollection());
        queryTree.visit(visitor);
        Assert.assertTrue(queryTree.getArg() instanceof AggregationPipelineQueryNode);
        final AggregationPipelineQueryNode pipelineNode = (AggregationPipelineQueryNode) queryTree.getArg();
        // Extend the pipeline by requiring a timestamp of zero (should have no effect)
        pipelineNode.requireSourceTimestamp(0);
        final Multiset<BindingSet> solutions = HashMultiset.create();
        CloseableIteration<BindingSet, QueryEvaluationException> iter = pipelineNode.evaluate(new QueryBindingSet());
        while (iter.hasNext()) {
            solutions.add(iter.next());
        }
        Assert.assertEquals(expectedSolutions, solutions);
        // Extend the pipeline by requiring a future timestamp (should produce no results)
        final long delta = 1000 * 60 * 60 * 24;
        pipelineNode.requireSourceTimestamp(System.currentTimeMillis() + delta);
        iter = pipelineNode.evaluate(new QueryBindingSet());
        Assert.assertFalse(iter.hasNext());
    }
}
