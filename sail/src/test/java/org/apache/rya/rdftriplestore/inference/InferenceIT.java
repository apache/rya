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
    private ValueFactory vf = new ValueFactoryImpl();
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
            public void handleBoolean(boolean arg0) throws QueryResultHandlerException { }
            @Override
            public void handleLinks(List<String> arg0) throws QueryResultHandlerException { }
            @Override
            public void handleSolution(BindingSet arg0) throws TupleQueryResultHandlerException {
                solutions.add(arg0);
            }
            @Override
            public void startQueryResult(List<String> arg0) throws TupleQueryResultHandlerException { }
        };
    }

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
        Set<Value> expected = new HashSet<>();
        expected.add(vf.createURI("urn:Alice"));
        expected.add(vf.createURI("urn:Bob"));
        expected.add(vf.createURI("urn:Eve"));
        Set<Value> returned = new HashSet<>();
        for (BindingSet bs : solutions) {
            returned.add(bs.getBinding("x").getValue());
        }
        Assert.assertEquals(expected, returned);
        Assert.assertEquals(expected.size(), solutions.size());
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
        Set<Value> expected = new HashSet<>();
        expected.add(vf.createURI("urn:Alice"));
        expected.add(vf.createURI("urn:Bob"));
        expected.add(vf.createURI("urn:Carol"));
        expected.add(vf.createURI("urn:Dan"));
        expected.add(vf.createURI("urn:Lucy"));
        Set<Value> returned = new HashSet<>();
        for (BindingSet bs : solutions) {
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
        Set<BindingSet> expected = new HashSet<BindingSet>();
        List<String> varNames = new LinkedList<>();
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
        Set<Value> expected = new HashSet<>();
        expected.add(vf.createURI("urn:Bob"));
        expected.add(vf.createURI("urn:Carol"));
        expected.add(vf.createURI("urn:Eve"));
        Set<Value> returned = new HashSet<>();
        for (BindingSet bs : solutions) {
            returned.add(bs.getBinding("x").getValue());
        }
        Assert.assertEquals(expected, returned);
        Assert.assertEquals(expected.size(), solutions.size());
    }
}
