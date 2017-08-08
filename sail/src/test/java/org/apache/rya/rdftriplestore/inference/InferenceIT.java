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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.FOAF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.impl.ListBindingSet;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;

import junit.framework.TestCase;

public class InferenceIT extends TestCase {
    private Connector connector;
    private AccumuloRyaDAO dao;
    private final ValueFactory vf = new ValueFactoryImpl();
    private AccumuloRdfConfiguration conf;
    private RdfCloudTripleStore store;
    private InferenceEngine inferenceEngine;
    private SailRepository repository;
    private SailRepositoryConnection conn;
    private TupleQueryResultHandler resultHandler;
    private List<BindingSet> solutions;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dao = new AccumuloRyaDAO();
        connector = new MockInstance().getConnector("", new PasswordToken(""));
        dao.setConnector(connector);
        conf = new AccumuloRdfConfiguration();
        conf.setInfer(true);
        dao.setConf(conf);
        dao.init();
        store = new RdfCloudTripleStore();
        store.setConf(conf);
        store.setRyaDAO(dao);
        inferenceEngine = new InferenceEngine();
        inferenceEngine.setRyaDAO(dao);
        store.setInferenceEngine(inferenceEngine);
        inferenceEngine.refreshGraph();
        store.initialize();
        repository = new SailRepository(store);
        conn = repository.getConnection();
        solutions = new LinkedList<>();
        resultHandler = new TupleQueryResultHandler() {
            @Override
            public void endQueryResult() throws TupleQueryResultHandlerException { }
            @Override
            public void handleBoolean(final boolean value) throws QueryResultHandlerException { }
            @Override
            public void handleLinks(final List<String> linkUrls) throws QueryResultHandlerException { }
            @Override
            public void handleSolution(final BindingSet bindingSet) throws TupleQueryResultHandlerException {
                if (bindingSet != null && bindingSet.iterator().hasNext()) {
                    solutions.add(bindingSet);
                }
            }
            @Override
            public void startQueryResult(final List<String> bindingNames) throws TupleQueryResultHandlerException {
                solutions.clear();
            }
        };
    }

    @Override
    @After
    public void tearDown() throws Exception {
        conn.close();
        repository.shutDown();
        store.shutDown();
        dao.purge(conf);
        dao.destroy();
    }

    @Test
    public void testSubClassInferenceQuery() throws Exception {
        final String ontology = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Agent> owl:equivalentClass <http://dbpedia.org/ontology/Agent> . \n"
                + "  <urn:Person> rdfs:subClassOf <urn:Agent> . \n"
                + "  [ owl:equivalentClass <http://schema.org/Person> ] rdfs:subClassOf <http://dbpedia.org/ontology/Agent> . \n"
                + "  <" + FOAF.PERSON.stringValue() + "> owl:equivalentClass <http://dbpedia.org/ontology/Person> . \n"
                + "  <" + FOAF.PERSON.stringValue() + "> owl:equivalentClass <urn:Person> . \n"
                + "  <http://dbpedia.org/ontology/Engineer> rdfs:subClassOf <http://dbpedia.org/ontology/Person> . \n"
                + "  <http://dbpedia.org/ontology/Engineer> rdfs:subClassOf <http://example.org/Person> . \n"
                + "  <http://dbpedia.org/ontology/Engineer> owl:equivalentClass <http://www.wikidata.org/entity/Q81096> . \n"
                + "}}";
        final String instances = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Alice> a <http://schema.org/Person> . \n"
                + "  <urn:Bob> a <http://www.wikidata.org/entity/Q81096> . \n"
                + "  <urn:Carol> a <http://example.org/Person> . \n"
                + "  <urn:Dan> a <http://example.org/Engineer> . \n"
                + "  <urn:Eve> a <urn:Agent> . \n"
                + "}}";
        final String query = "SELECT ?x { GRAPH <http://updated/test> { ?x a <urn:Agent> } } \n";
        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        inferenceEngine.refreshGraph();
        conn.prepareUpdate(QueryLanguage.SPARQL, instances).execute();
        conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate(resultHandler);
        final Set<Value> expected = new HashSet<>();
        expected.add(vf.createURI("urn:Alice"));
        expected.add(vf.createURI("urn:Bob"));
        expected.add(vf.createURI("urn:Eve"));
        final Set<Value> returned = new HashSet<>();
        for (final BindingSet bs : solutions) {
            returned.add(bs.getBinding("x").getValue());
        }
        Assert.assertEquals(expected, returned);
        Assert.assertEquals(expected.size(), solutions.size());
    }

    @Test
    public void testDomainRangeQuery() throws Exception {
        final String ontology = "PREFIX lubm: <http://swat.lehigh.edu/onto/univ-bench.owl#>\n"
                + "INSERT DATA {\n"
                + "  lubm:advisor rdfs:domain lubm:Person ;\n"
                + "               rdfs:range lubm:Professor ;"
                + "               owl:inverseOf lubm:advisee .\n"
                + "  lubm:teachesCourse rdfs:domain lubm:Professor ;\n"
                + "               rdfs:range lubm:Course ."
                + "  lubm:takesCourse rdfs:domain lubm:Student ;\n"
                + "               rdfs:range lubm:Course ."
                + "  lubm:FullProfessor rdfs:subClassOf lubm:Professor .\n"
                + "  lubm:Professor rdfs:subClassOf lubm:Faculty .\n"
                + "  lubm:Faculty rdfs:subClassOf lubm:Person .\n"
                + "  lubm:Student rdfs:subClassOf lubm:Person .\n"
                + "}";
        final String instances = "PREFIX lubm: <http://swat.lehigh.edu/onto/univ-bench.owl#>\n"
                + "INSERT DATA {\n"
                + "  <urn:Professor1> a lubm:Professor .\n"
                + "  <urn:Student1> a lubm:Student .\n"
                + "  <urn:Student2> lubm:advisor <urn:Professor2> .\n"
                + "  <urn:Student3> lubm:advisor <urn:Professor2> .\n"
                + "  <urn:Professor3> lubm:advisee <urn:Student4> .\n"
                + "  <urn:Professor4> lubm:teachesCourse <urn:CS100> .\n"
                + "  <urn:Student1> lubm:takesCourse <urn:CS100> .\n"
                + "}";
        final String query = "SELECT ?x { ?x a <http://swat.lehigh.edu/onto/univ-bench.owl#Faculty> }";
        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        inferenceEngine.refreshGraph();
        conn.prepareUpdate(QueryLanguage.SPARQL, instances).execute();
        conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate(resultHandler);
        final Set<Value> expected = new HashSet<>();
        expected.add(vf.createURI("urn:Professor1"));
        expected.add(vf.createURI("urn:Professor2"));
        expected.add(vf.createURI("urn:Professor3"));
        expected.add(vf.createURI("urn:Professor4"));
        final Set<Value> returned = new HashSet<>();
        for (final BindingSet bs : solutions) {
            returned.add(bs.getBinding("x").getValue());
        }
        Assert.assertEquals(expected, returned);
        Assert.assertEquals(5, solutions.size());
    }

    @Test
    public void testAllValuesFromQuery() throws Exception {
        final String ontology = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Cairn_Terrier> rdfs:subClassOf <urn:Terrier> .\n"
                + "  <urn:Terrier> rdfs:subClassOf <urn:Dog> ;\n"
                + "    owl:onProperty <urn:relative> ;\n"
                + "    owl:allValuesFrom <urn:Terrier> .\n"
                + "  <urn:Dog> rdfs:subClassOf [\n"
                + "    owl:onProperty <urn:portrays> ; owl:allValuesFrom <urn:FictionalDog>\n"
                + "  ] .\n"
                + "  <urn:parent> rdfs:subPropertyOf <urn:relative> .\n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        inferenceEngine.refreshGraph();
        final String instances = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Rommy> a <urn:Cairn_Terrier> .\n"
                + "  <urn:Rommy> <urn:parent> <urn:Terry> .\n"
                + "  <urn:Terry> <urn:portrays> <urn:Toto> .\n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, instances).execute();
        conn.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT ?x { ?x a <urn:Dog> }").evaluate(resultHandler);
        Assert.assertEquals(2, solutions.size());
        final Set<Value> answers = new HashSet<>();
        for (final BindingSet solution : solutions) {
            answers.add(solution.getBinding("x").getValue());
        }
        Assert.assertTrue(answers.contains(vf.createURI("urn:Terry")));
        Assert.assertTrue(answers.contains(vf.createURI("urn:Rommy")));
        // If allValuesFrom inference were applied recursively, this triple wouldn't be needed:
        conn.prepareUpdate(QueryLanguage.SPARQL, "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Terry> a <urn:Cairn_Terrier> .\n"
                + "}}").execute();
        conn.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT ?x { ?x a <urn:FictionalDog> }").evaluate(resultHandler);
        Assert.assertEquals(1, solutions.size());
        Assert.assertEquals(vf.createURI("urn:Toto"), solutions.get(0).getBinding("x").getValue());
    }

    @Test
    public void testHasValueTypeQuery() throws Exception {
        final String ontology = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Biped> owl:onProperty <urn:walksOnLegs>  ; owl:hasValue \"2\"^^<xsd:integer> . \n"
                + "  <urn:Quadruped> owl:onProperty <urn:walksOnLegs>  ; owl:hasValue \"4\"^^<xsd:int> . \n"
                + "  <urn:Person> owl:onProperty <urn:taxon>  ; owl:hasValue <urn:Hominidae> . \n"
                + "  <urn:Person> rdfs:subClassOf <urn:Biped> . \n"
                + "  <urn:Horse> rdfs:subClassOf <urn:Quadruped> . \n"
                + "  <urn:Biped> rdfs:subClassOf <urn:Animal> . \n"
                + "}}";
        final String instances = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Alice> a <urn:Person>  . \n"
                + "  <urn:Bob> <urn:walksOnLegs> \"2\"^^<xsd:integer> . \n"
                + "  <urn:Carol> <urn:walksOnLegs> \"2\" . \n"
                + "  <urn:Dan> <urn:taxon> <urn:Hominidae> . \n"
                + "  <urn:Laika> a <urn:Quadruped> . \n"
                + "  <urn:Lucy> a <urn:Biped> . \n"
                + "  <urn:Hank> <urn:walksOnLegs> \"4\"^^<xsd:int> . \n"
                + "  <urn:Hans> a <urn:Horse> . \n"
                + "}}";
        final String query = "SELECT ?x { GRAPH <http://updated/test> { ?x a <urn:Biped> } } \n";
        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        inferenceEngine.refreshGraph();
        conn.prepareUpdate(QueryLanguage.SPARQL, instances).execute();
        conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate(resultHandler);
        final Set<Value> expected = new HashSet<>();
        expected.add(vf.createURI("urn:Alice"));
        expected.add(vf.createURI("urn:Bob"));
        expected.add(vf.createURI("urn:Carol"));
        expected.add(vf.createURI("urn:Dan"));
        expected.add(vf.createURI("urn:Lucy"));
        final Set<Value> returned = new HashSet<>();
        for (final BindingSet bs : solutions) {
            returned.add(bs.getBinding("x").getValue());
        }
        Assert.assertEquals(expected, returned);
        Assert.assertEquals(5, solutions.size());
    }

    @Test
    public void testHasValueValueQuery() throws Exception {
        final String ontology = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Hominid> owl:onProperty <urn:taxon> ; owl:hasValue <urn:Hominidae> . \n"
                + "  <urn:Carnivoran> owl:onProperty <urn:taxon>  ; owl:hasValue <urn:Carnivora> . \n"
                + "  <urn:Mammal> owl:onProperty <urn:taxon>  ; owl:hasValue <urn:Mammalia> . \n"
                + "  <urn:Tunicate> owl:onProperty <urn:taxon>  ; owl:hasValue <urn:Tunicata> . \n"
                + "  <urn:Person> rdfs:subClassOf <urn:Hominid> . \n"
                + "  <urn:Hominid> rdfs:subClassOf <urn:Mammal> . \n"
                + "  <urn:Cat> rdfs:subClassOf <urn:Carnivoran> . \n"
                + "  <urn:Carnivoran> rdfs:subClassOf <urn:Mammal> . \n"
                + "}}";
        final String instances = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Alice> a <urn:Person> . \n"
                + "  <urn:Bigfoot> a <urn:Mammal> . \n"
                + "  <urn:Carol> <urn:taxon> <urn:Hominidae> . \n"
                + "  <urn:Hank> a <urn:Cat> . \n"
                + "}}";
        final String query = "SELECT ?individual ?taxon { GRAPH <http://updated/test> { ?individual <urn:taxon> ?taxon } } \n";
        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        conn.prepareUpdate(QueryLanguage.SPARQL, instances).execute();
        inferenceEngine.refreshGraph();
        conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate(resultHandler);
        final Set<BindingSet> expected = new HashSet<BindingSet>();
        final List<String> varNames = new LinkedList<>();
        varNames.add("individual");
        varNames.add("taxon");
        expected.add(new ListBindingSet(varNames, vf.createURI("urn:Alice"), vf.createURI("urn:Hominidae")));
        expected.add(new ListBindingSet(varNames, vf.createURI("urn:Alice"), vf.createURI("urn:Mammalia")));
        expected.add(new ListBindingSet(varNames, vf.createURI("urn:Bigfoot"), vf.createURI("urn:Mammalia")));
        expected.add(new ListBindingSet(varNames, vf.createURI("urn:Carol"), vf.createURI("urn:Hominidae")));
        expected.add(new ListBindingSet(varNames, vf.createURI("urn:Hank"), vf.createURI("urn:Carnivora")));
        expected.add(new ListBindingSet(varNames, vf.createURI("urn:Hank"), vf.createURI("urn:Mammalia")));
        Assert.assertEquals(expected, new HashSet<>(solutions));
    }

    @Test
    public void testUnionQuery() throws Exception {
        final String ontology = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:A> owl:unionOf <urn:list1> . \n"
                + "  <urn:B> owl:unionOf <urn:list2> . \n"
                + "  <urn:list1> rdf:first <urn:X> . \n"
                + "  <urn:list1> rdf:rest <urn:list2> . \n"
                + "  <urn:list2> rdf:first <urn:Y> . \n"
                + "  <urn:list2> rdf:rest <urn:list3> . \n"
                + "  <urn:list3> rdf:first <urn:Z> . \n"
                + "  <urn:SubY> rdfs:subClassOf <urn:Y> . \n"
                + "  <urn:Y> rdfs:subClassOf <urn:SuperY> . \n"
                + "}}";
        final String instances = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Alice> a <urn:X>  . \n"
                + "  <urn:Bob> a <urn:Y>  . \n"
                + "  <urn:Carol> a <urn:Z>  . \n"
                + "  <urn:Dan> a <urn:SuperY>  . \n"
                + "  <urn:Eve> a <urn:SubY>  . \n"
                + "}}";
        final String query = "SELECT ?x { GRAPH <http://updated/test> { ?x a <urn:B> } } \n";
        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        inferenceEngine.refreshGraph();
        conn.prepareUpdate(QueryLanguage.SPARQL, instances).execute();
        conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate(resultHandler);
        final Set<Value> expected = new HashSet<>();
        expected.add(vf.createURI("urn:Bob"));
        expected.add(vf.createURI("urn:Carol"));
        expected.add(vf.createURI("urn:Eve"));
        final Set<Value> returned = new HashSet<>();
        for (final BindingSet bs : solutions) {
            returned.add(bs.getBinding("x").getValue());
        }
        Assert.assertEquals(expected, returned);
        Assert.assertEquals(expected.size(), solutions.size());
    }

    public void testIntersectionOfQuery() throws Exception {
        final String ontology = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Mother> owl:intersectionOf _:bnode1 . \n"
                + "  _:bnode1 rdf:first <urn:Woman> . \n"
                + "  _:bnode1 rdf:rest _:bnode2 . \n"
                + "  _:bnode2 rdf:first <urn:Parent> . \n"
                + "  _:bnode2 rdf:rest rdf:nil . \n"
                + "  <urn:Father> owl:intersectionOf _:bnode3 . \n"
                + "  _:bnode3 rdf:first <urn:Man> . \n"
                + "  _:bnode3 rdf:rest _:bnode4 . \n"
                + "  _:bnode4 rdf:first <urn:Parent> . \n"
                + "  _:bnode4 rdf:rest rdf:nil . \n"
                + "}}";
        final String instances = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Susan> a <urn:Mother> . \n"
                + "  <urn:Bob> a <urn:Man> . \n"
                + "  <urn:Bob> a <urn:Parent> . \n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        conn.prepareUpdate(QueryLanguage.SPARQL, instances).execute();
        inferenceEngine.refreshGraph();

        final List<String> varNames = new LinkedList<>();
        varNames.add("individual");

        // Find all <urn:Mother> types (expect 1 result)
        final String motherQuery = "SELECT ?individual { GRAPH <http://updated/test> { ?individual rdf:type <urn:Mother> } } \n";
        conn.prepareTupleQuery(QueryLanguage.SPARQL, motherQuery).evaluate(resultHandler);
        final Set<BindingSet> expectedMothers = new HashSet<>();
        expectedMothers.add(new ListBindingSet(varNames, vf.createURI("urn:Susan")));
        Assert.assertEquals(expectedMothers, new HashSet<>(solutions));

        // Find all <urn:Father> types (expect 1 result)
        final String fatherQuery = "SELECT ?individual { GRAPH <http://updated/test> { ?individual rdf:type <urn:Father> } } \n";
        conn.prepareTupleQuery(QueryLanguage.SPARQL, fatherQuery).evaluate(resultHandler);
        final Set<BindingSet> expectedFathers = new HashSet<>();
        expectedFathers.add(new ListBindingSet(varNames, vf.createURI("urn:Bob")));
        Assert.assertEquals(expectedFathers, new HashSet<>(solutions));

        // Find all <urn:Parent> types (expect 2 results)
        final String parentQuery = "SELECT ?individual { GRAPH <http://updated/test> { ?individual rdf:type <urn:Parent> } } \n";
        conn.prepareTupleQuery(QueryLanguage.SPARQL, parentQuery).evaluate(resultHandler);
        final Set<BindingSet> expectedParents = new HashSet<>();
        expectedParents.add(new ListBindingSet(varNames, vf.createURI("urn:Bob")));
        expectedParents.add(new ListBindingSet(varNames, vf.createURI("urn:Susan")));
        Assert.assertEquals(expectedParents, new HashSet<>(solutions));

        // Find all <urn:Woman> types (expect 1 result)
        final String womanQuery = "SELECT ?individual { GRAPH <http://updated/test> { ?individual rdf:type <urn:Woman> } } \n";
        conn.prepareTupleQuery(QueryLanguage.SPARQL, womanQuery).evaluate(resultHandler);
        final Set<BindingSet> expectedWomen = new HashSet<>();
        expectedWomen.add(new ListBindingSet(varNames, vf.createURI("urn:Susan")));
        Assert.assertEquals(expectedWomen, new HashSet<>(solutions));

        // Find all <urn:Man> types (expect 1 result)
        final String manQuery = "SELECT ?individual { GRAPH <http://updated/test> { ?individual rdf:type <urn:Man> } } \n";
        conn.prepareTupleQuery(QueryLanguage.SPARQL, manQuery).evaluate(resultHandler);
        final Set<BindingSet> expectedMen = new HashSet<>();
        expectedMen.add(new ListBindingSet(varNames, vf.createURI("urn:Bob")));
        Assert.assertEquals(expectedMen, new HashSet<>(solutions));
    }

    @Test
    public void testOneOfQuery() throws Exception {
        final String ontology = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Suits> owl:oneOf _:bnodeS1 . \n"
                + "  _:bnodeS1 rdf:first <urn:Clubs> . \n"
                + "  _:bnodeS1 rdf:rest _:bnodeS2 . \n"
                + "  _:bnodeS2 rdf:first <urn:Diamonds> . \n"
                + "  _:bnodeS2 rdf:rest _:bnodeS3 . \n"
                + "  _:bnodeS3 rdf:first <urn:Hearts> . \n"
                + "  _:bnodeS3 rdf:rest _:bnodeS4 . \n"
                + "  _:bnodeS4 rdf:first <urn:Spades> . \n"
                + "  _:bnodeS4 rdf:rest rdf:nil . \n"
                + "  <urn:Ranks> owl:oneOf _:bnodeR1 . \n"
                + "  _:bnodeR1 rdf:first <urn:Ace> . \n"
                + "  _:bnodeR1 rdf:rest _:bnodeR2 . \n"
                + "  _:bnodeR2 rdf:first <urn:2> . \n"
                + "  _:bnodeR2 rdf:rest _:bnodeR3 . \n"
                + "  _:bnodeR3 rdf:first <urn:3> . \n"
                + "  _:bnodeR3 rdf:rest _:bnodeR4 . \n"
                + "  _:bnodeR4 rdf:first <urn:4> . \n"
                + "  _:bnodeR4 rdf:rest _:bnodeR5 . \n"
                + "  _:bnodeR5 rdf:first <urn:5> . \n"
                + "  _:bnodeR5 rdf:rest _:bnodeR6 . \n"
                + "  _:bnodeR6 rdf:first <urn:6> . \n"
                + "  _:bnodeR6 rdf:rest _:bnodeR7 . \n"
                + "  _:bnodeR7 rdf:first <urn:7> . \n"
                + "  _:bnodeR7 rdf:rest _:bnodeR8 . \n"
                + "  _:bnodeR8 rdf:first <urn:8> . \n"
                + "  _:bnodeR8 rdf:rest _:bnodeR9 . \n"
                + "  _:bnodeR9 rdf:first <urn:9> . \n"
                + "  _:bnodeR9 rdf:rest _:bnodeR10 . \n"
                + "  _:bnodeR10 rdf:first <urn:10> . \n"
                + "  _:bnodeR10 rdf:rest _:bnodeR11 . \n"
                + "  _:bnodeR11 rdf:first <urn:Jack> . \n"
                + "  _:bnodeR11 rdf:rest _:bnodeR12 . \n"
                + "  _:bnodeR12 rdf:first <urn:Queen> . \n"
                + "  _:bnodeR12 rdf:rest _:bnodeR13 . \n"
                + "  _:bnodeR13 rdf:first <urn:King> . \n"
                + "  _:bnodeR13 rdf:rest rdf:nil . \n"
                + "  <urn:Card> owl:intersectionOf (\n"
                + "    [ owl:onProperty <urn:HasRank> ; owl:someValuesFrom <urn:Ranks> ]\n"
                + "    [ owl:onProperty <urn:HasSuit> ; owl:someValuesFrom <urn:Suits> ]\n"
                + "  ) . \n"
                + "  <urn:HasRank> owl:range <urn:Ranks> . \n"
                + "  <urn:HasSuit> owl:range <urn:Suits> . \n"
                + "}}";
        final String instances = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:FlopCard1> a <urn:Card> . \n"
                + "    <urn:FlopCard1> <urn:HasRank> <urn:Ace> . \n"
                + "    <urn:FlopCard1> <urn:HasSuit> <urn:Diamonds> . \n"
                + "  <urn:FlopCard2> a <urn:Card> . \n"
                + "    <urn:FlopCard2> <urn:HasRank> <urn:Ace> . \n"
                + "    <urn:FlopCard2> <urn:HasSuit> <urn:Hearts> . \n"
                + "  <urn:FlopCard3> a <urn:Card> . \n"
                + "    <urn:FlopCard3> <urn:HasRank> <urn:King> . \n"
                + "    <urn:FlopCard3> <urn:HasSuit> <urn:Spades> . \n"
                + "  <urn:TurnCard> a <urn:Card> . \n"
                + "    <urn:TurnCard> <urn:HasRank> <urn:10> . \n"
                + "    <urn:TurnCard> <urn:HasSuit> <urn:Clubs> . \n"
                + "  <urn:RiverCard> a <urn:Card> . \n"
                + "    <urn:RiverCard> <urn:HasRank> <urn:Queen> . \n"
                + "    <urn:RiverCard> <urn:HasSuit> <urn:Hearts> . \n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        conn.prepareUpdate(QueryLanguage.SPARQL, instances).execute();
        inferenceEngine.refreshGraph();

        final List<String> varNames = new LinkedList<>();
        varNames.add("card");

        // Find all cards with a <urn:Suits> type (expect 5 results)
        final String cardSuitQuery = "SELECT ?card { GRAPH <http://updated/test> { ?card a <urn:Card> . ?suit a <urn:Suits> . ?card <urn:HasSuit> ?suit} } \n";
        conn.prepareTupleQuery(QueryLanguage.SPARQL, cardSuitQuery).evaluate(resultHandler);
        final Set<BindingSet> expectedCardSuits = new HashSet<>();
        expectedCardSuits.add(new ListBindingSet(varNames, vf.createURI("urn:FlopCard1")));
        expectedCardSuits.add(new ListBindingSet(varNames, vf.createURI("urn:FlopCard2")));
        expectedCardSuits.add(new ListBindingSet(varNames, vf.createURI("urn:FlopCard3")));
        expectedCardSuits.add(new ListBindingSet(varNames, vf.createURI("urn:TurnCard")));
        expectedCardSuits.add(new ListBindingSet(varNames, vf.createURI("urn:RiverCard")));
        Assert.assertEquals(expectedCardSuits.size(), solutions.size());
        Assert.assertEquals(expectedCardSuits, new HashSet<>(solutions));

        // Find all cards with a <urn:Ranks> type (expect 5 results)
        final String cardRankQuery = "SELECT ?card { GRAPH <http://updated/test> { ?card a <urn:Card> . ?rank a <urn:Ranks> . ?card <urn:HasRank> ?rank} } \n";
        conn.prepareTupleQuery(QueryLanguage.SPARQL, cardRankQuery).evaluate(resultHandler);
        final Set<BindingSet> expectedCardRanks = new HashSet<>();
        expectedCardRanks.add(new ListBindingSet(varNames, vf.createURI("urn:FlopCard1")));
        expectedCardRanks.add(new ListBindingSet(varNames, vf.createURI("urn:FlopCard2")));
        expectedCardRanks.add(new ListBindingSet(varNames, vf.createURI("urn:FlopCard3")));
        expectedCardRanks.add(new ListBindingSet(varNames, vf.createURI("urn:TurnCard")));
        expectedCardRanks.add(new ListBindingSet(varNames, vf.createURI("urn:RiverCard")));
        Assert.assertEquals(expectedCardRanks.size(), solutions.size());
        Assert.assertEquals(expectedCardRanks, new HashSet<>(solutions));
    }

    @Test
    public void testHasSelfQuery() throws Exception {
        final String ontology = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Narcissist> owl:onProperty <urn:love> ; owl:hasSelf \"true\" . \n"
                + "}}";
        final String instances = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Alice> a <urn:Narcissist> . \n"
                + "  <urn:Narcissus> <urn:love> <urn:Narcissus> . \n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        conn.prepareUpdate(QueryLanguage.SPARQL, instances).execute();
        inferenceEngine.refreshGraph();

        String query = "SELECT ?who ?self { GRAPH <http://updated/test> { ?self <urn:love> ?who } } \n";
        conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate(resultHandler);
        final Set<BindingSet> expected = new HashSet<BindingSet>();
        final List<String> varNames = new LinkedList<>();
        varNames.add("who");
        varNames.add("self");
        expected.add(new ListBindingSet(varNames, vf.createURI("urn:Alice"), vf.createURI("urn:Alice")));
        expected.add(new ListBindingSet(varNames, vf.createURI("urn:Narcissus"), vf.createURI("urn:Narcissus")));
        Assert.assertEquals(expected, new HashSet<>(solutions));

        query = "SELECT ?self { GRAPH <http://updated/test> { <urn:Alice> <urn:love> ?self } } \n";
        conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate(resultHandler);
        expected.clear();
        varNames.clear();
        varNames.add("self");
        expected.add(new ListBindingSet(varNames, vf.createURI("urn:Alice")));
        Assert.assertEquals(expected, new HashSet<>(solutions));

        query = "SELECT ?who { GRAPH <http://updated/test> { ?who <urn:love> <urn:Alice> } } \n";
        conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate(resultHandler);
        expected.clear();
        varNames.clear();
        varNames.add("who");
        expected.add(new ListBindingSet(varNames, vf.createURI("urn:Alice")));
        Assert.assertEquals(expected, new HashSet<>(solutions));

        query = "SELECT ?who { GRAPH <http://updated/test> { ?who a <urn:Narcissist> } } \n";
        conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate(resultHandler);
        expected.clear();
        varNames.clear();
        varNames.add("who");
        expected.add(new ListBindingSet(varNames, vf.createURI("urn:Narcissus")));
        expected.add(new ListBindingSet(varNames, vf.createURI("urn:Alice")));
        Assert.assertEquals(expected, new HashSet<>(solutions));
    }
}
