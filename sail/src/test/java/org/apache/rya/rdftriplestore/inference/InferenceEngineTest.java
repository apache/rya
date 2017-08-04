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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.After;
import org.junit.Assert;

import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.QueryLanguage;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;

import junit.framework.TestCase;

public class InferenceEngineTest extends TestCase {
    private Connector connector;
    private AccumuloRyaDAO dao;
    private ValueFactory vf = new ValueFactoryImpl();
    private AccumuloRdfConfiguration conf;
    private RdfCloudTripleStore store;
    private InferenceEngine inferenceEngine;
    private SailRepository repository;
    private SailRepositoryConnection conn;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dao = new AccumuloRyaDAO();
        connector = new MockInstance().getConnector("", new PasswordToken(""));
        dao.setConnector(connector);
        conf = new AccumuloRdfConfiguration();
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
    public void testSubClassGraph() throws Exception {
        String insert = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:A> rdfs:subClassOf <urn:C> . \n"
                + "  <urn:B> rdfs:subClassOf <urn:C> . \n"
                + "  <urn:C> rdfs:subClassOf <urn:D> . \n"
                + "  <urn:E> owl:equivalentClass <urn:D> . \n"
                + "  <urn:E> rdfs:subClassOf <urn:G> . \n"
                + "  <urn:Z> a owl:Class . \n"
                + "  <urn:F> owl:equivalentClass <urn:G> . \n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, insert).execute();
        inferenceEngine.refreshGraph();
        Graph graph = inferenceEngine.getSubClassOfGraph();
        URI a = vf.createURI("urn:A");
        URI b = vf.createURI("urn:B");
        URI c = vf.createURI("urn:C");
        URI d = vf.createURI("urn:D");
        URI e = vf.createURI("urn:E");
        URI f = vf.createURI("urn:F");
        URI g = vf.createURI("urn:G");
        URI z = vf.createURI("urn:Z");
        URI missing = vf.createURI("urn:Missing");
        Set<URI> empty = new HashSet<>();
        Set<URI> belowLevel2 = new HashSet<>(Arrays.asList(new URI[] { a, b }));
        Set<URI> belowLevel3 = new HashSet<>(Arrays.asList(new URI[] { a, b, c, d, e }));
        Set<URI> belowLevel4 = new HashSet<>(Arrays.asList(new URI[] { a, b, c, d, e, f, g }));
        Assert.assertEquals(empty, inferenceEngine.findParents(graph, a));
        Assert.assertEquals(empty, inferenceEngine.findParents(graph, b));
        Assert.assertEquals(empty, inferenceEngine.findParents(graph, z));
        Assert.assertEquals(empty, inferenceEngine.findParents(graph, missing));
        Assert.assertEquals(belowLevel2, inferenceEngine.findParents(graph, c));
        Assert.assertEquals(belowLevel3, inferenceEngine.findParents(graph, d));
        Assert.assertEquals(belowLevel3, inferenceEngine.findParents(graph, e));
        Assert.assertEquals(belowLevel4, inferenceEngine.findParents(graph, f));
        Assert.assertEquals(belowLevel4, inferenceEngine.findParents(graph, g));
    }

    @Test
    public void testSubPropertyGraph() throws Exception {
        String insert = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:p> rdfs:subPropertyOf <urn:q> . \n"
                + "  <urn:p> rdfs:subPropertyOf <urn:r> . \n"
                + "  <urn:r> owl:equivalentProperty <urn:s> . \n"
                + "  <urn:q> rdfs:subPropertyOf <urn:t> . \n"
                + "  <urn:t> rdfs:subPropertyOf <urn:u> . \n"
                + "  <urn:s> rdfs:subPropertyOf <urn:u> . \n"
                + "  <urn:v> owl:equivalentProperty <urn:u> . \n"
                + "  <urn:w> a owl:FunctionalProperty . \n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, insert).execute();
        inferenceEngine.refreshGraph();
        Graph graph = inferenceEngine.getSubPropertyOfGraph();
        URI p = vf.createURI("urn:p");
        URI q = vf.createURI("urn:q");
        URI r = vf.createURI("urn:r");
        URI s = vf.createURI("urn:s");
        URI t = vf.createURI("urn:t");
        URI u = vf.createURI("urn:u");
        URI v = vf.createURI("urn:v");
        URI w = vf.createURI("urn:w");
        URI missing = vf.createURI("urn:Missing");
        Set<URI> empty = new HashSet<>();
        Set<URI> belowQ = new HashSet<>(Arrays.asList(new URI[] { p }));
        Set<URI> belowR = new HashSet<>(Arrays.asList(new URI[] { p, r, s }));
        Set<URI> belowT = new HashSet<>(Arrays.asList(new URI[] { p, q }));
        Set<URI> belowU = new HashSet<>(Arrays.asList(new URI[] { p, q, r, s, t, u, v }));
        Assert.assertEquals(empty, inferenceEngine.findParents(graph, p));
        Assert.assertEquals(empty, inferenceEngine.findParents(graph, w));
        Assert.assertEquals(empty, inferenceEngine.findParents(graph, missing));
        Assert.assertEquals(belowQ, inferenceEngine.findParents(graph, q));
        Assert.assertEquals(belowR, inferenceEngine.findParents(graph, r));
        Assert.assertEquals(belowR, inferenceEngine.findParents(graph, s));
        Assert.assertEquals(belowT, inferenceEngine.findParents(graph, t));
        Assert.assertEquals(belowU, inferenceEngine.findParents(graph, u));
        Assert.assertEquals(belowU, inferenceEngine.findParents(graph, v));
    }

    @Test
    public void testDomainRange() throws Exception {
        String insert = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:p1> rdfs:subPropertyOf <urn:p2> . \n"
                + "  <urn:p2> rdfs:subPropertyOf <urn:p3> . \n"
                + "  <urn:q1> rdfs:subPropertyOf <urn:q2> . \n"
                + "  <urn:q2> rdfs:subPropertyOf <urn:q3> . \n"
                + "  <urn:i1> rdfs:subPropertyOf <urn:i2> . \n"
                + "  <urn:i2> rdfs:subPropertyOf <urn:i3> . \n"
                + "  <urn:j1> rdfs:subPropertyOf <urn:j2> . \n"
                + "  <urn:j2> rdfs:subPropertyOf <urn:j3> . \n"
                + "  <urn:p2> owl:inverseOf <urn:i2> . \n"
                + "  <urn:i1> owl:inverseOf <urn:q2> . \n"
                + "  <urn:q1> owl:inverseOf <urn:j2> . \n"
                + "  <urn:D1> rdfs:subClassOf <urn:D2> . \n"
                + "  <urn:D2> rdfs:subClassOf <urn:D3> . \n"
                + "  <urn:R1> rdfs:subClassOf <urn:R2> . \n"
                + "  <urn:R2> rdfs:subClassOf <urn:R3> . \n"
                + "  <urn:p2> rdfs:domain <urn:D2> . \n"
                + "  <urn:p2> rdfs:range <urn:R2> . \n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, insert).execute();
        inferenceEngine.refreshGraph();
        Set<URI> hasDomainD1 = inferenceEngine.getPropertiesWithDomain(vf.createURI("urn:D1"));
        Set<URI> hasDomainD2 = inferenceEngine.getPropertiesWithDomain(vf.createURI("urn:D2"));
        Set<URI> hasDomainD3 = inferenceEngine.getPropertiesWithDomain(vf.createURI("urn:D3"));
        Set<URI> hasRangeD1 = inferenceEngine.getPropertiesWithRange(vf.createURI("urn:D1"));
        Set<URI> hasRangeD2 = inferenceEngine.getPropertiesWithRange(vf.createURI("urn:D2"));
        Set<URI> hasRangeD3 = inferenceEngine.getPropertiesWithRange(vf.createURI("urn:D3"));
        Set<URI> hasDomainR1 = inferenceEngine.getPropertiesWithDomain(vf.createURI("urn:R1"));
        Set<URI> hasDomainR2 = inferenceEngine.getPropertiesWithDomain(vf.createURI("urn:R2"));
        Set<URI> hasDomainR3 = inferenceEngine.getPropertiesWithDomain(vf.createURI("urn:R3"));
        Set<URI> hasRangeR1 = inferenceEngine.getPropertiesWithRange(vf.createURI("urn:R1"));
        Set<URI> hasRangeR2 = inferenceEngine.getPropertiesWithRange(vf.createURI("urn:R2"));
        Set<URI> hasRangeR3 = inferenceEngine.getPropertiesWithRange(vf.createURI("urn:R3"));
        Set<URI> empty = new HashSet<>();
        Set<URI> expectedForward = new HashSet<>();
        expectedForward.add(vf.createURI("urn:p2"));
        expectedForward.add(vf.createURI("urn:p1"));
        expectedForward.add(vf.createURI("urn:q2"));
        expectedForward.add(vf.createURI("urn:q1"));
        Set<URI> expectedInverse = new HashSet<>();
        expectedInverse.add(vf.createURI("urn:i1"));
        expectedInverse.add(vf.createURI("urn:i2"));
        expectedInverse.add(vf.createURI("urn:j1"));
        expectedInverse.add(vf.createURI("urn:j2"));
        Assert.assertEquals(empty, hasDomainD1);
        Assert.assertEquals(empty, hasRangeD1);
        Assert.assertEquals(empty, hasDomainR1);
        Assert.assertEquals(empty, hasRangeR1);
        Assert.assertEquals(expectedForward, hasDomainD2);
        Assert.assertEquals(expectedInverse, hasRangeD2);
        Assert.assertEquals(expectedInverse, hasDomainR2);
        Assert.assertEquals(expectedForward, hasRangeR2);
        Assert.assertEquals(expectedForward, hasDomainD3);
        Assert.assertEquals(expectedInverse, hasRangeD3);
        Assert.assertEquals(expectedInverse, hasDomainR3);
        Assert.assertEquals(expectedForward, hasRangeR3);
    }

    @Test
    public void testHasValueGivenProperty() throws Exception {
        String insert = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Biped> owl:onProperty <urn:walksUsingLegs>  . \n"
                + "  <urn:Biped> owl:hasValue \"2\" . \n"
                + "  <urn:Mammal> owl:onProperty <urn:taxon> . \n"
                + "  <urn:Mammal> owl:hasValue <urn:Mammalia> . \n"
                + "  <urn:Vertebrate> owl:onProperty <urn:taxon> . \n"
                + "  <urn:Vertebrate> owl:hasValue <urn:Vertebrata> . \n"
                + "  <urn:Tunicate> owl:onProperty <urn:taxon> . \n"
                + "  <urn:Tunicate> owl:hasValue <urn:Tunicata> . \n"
                + "  <urn:Mammal> rdfs:subClassOf <urn:Vertebrate> . \n"
                + "  <urn:Vertebrate> rdfs:subClassOf <urn:Animal> . \n"
                + "  <urn:Tunicate> rdfs:subClassOf <urn:Animal> . \n"
                + "  <urn:Biped> rdfs:subClassOf <urn:Animal> . \n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, insert).execute();
        inferenceEngine.refreshGraph();
        final Map<Resource, Set<Value>> typeToValueImplications = new HashMap<>();
        final Set<Value> vertebrateTaxa = new HashSet<>();
        final Set<Value> tunicateTaxa = new HashSet<>();
        vertebrateTaxa.add(vf.createURI("urn:Vertebrata"));
        tunicateTaxa.add(vf.createURI("urn:Tunicata"));
        final Set<Value> mammalTaxa = new HashSet<>(vertebrateTaxa);
        mammalTaxa.add(vf.createURI("urn:Mammalia"));
        typeToValueImplications.put(vf.createURI("urn:Vertebrate"), vertebrateTaxa);
        typeToValueImplications.put(vf.createURI("urn:Tunicate"), tunicateTaxa);
        typeToValueImplications.put(vf.createURI("urn:Mammal"), mammalTaxa);
        Assert.assertEquals(typeToValueImplications, inferenceEngine.getHasValueByProperty(vf.createURI("urn:taxon")));
    }

    @Test
    public void testHasValueGivenType() throws Exception {
        String insert = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Biped> owl:onProperty <urn:walksUsingLegs>  . \n"
                + "  <urn:Biped> owl:hasValue \"2\" . \n"
                + "  <urn:Mammal> owl:onProperty <urn:taxon> . \n"
                + "  <urn:Mammal> owl:hasValue <urn:Mammalia> . \n"
                + "  <urn:Vertebrate> owl:onProperty <urn:taxon> . \n"
                + "  <urn:Vertebrate> owl:hasValue <urn:Vertebrata> . \n"
                + "  <urn:Tunicate> owl:onProperty <urn:taxon> . \n"
                + "  <urn:Tunicate> owl:hasValue <urn:Tunicata> . \n"
                + "  <urn:Plant> owl:onProperty <urn:taxon>  . \n"
                + "  <urn:Plant> owl:hasValue <urn:Plantae> . \n"
                + "  <urn:Mammal> rdfs:subClassOf <urn:Vertebrate> . \n"
                + "  <urn:Vertebrate> rdfs:subClassOf <urn:Animal> . \n"
                + "  <urn:Tunicate> rdfs:subClassOf <urn:Animal> . \n"
                + "  <urn:Biped> rdfs:subClassOf <urn:Animal> . \n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, insert).execute();
        inferenceEngine.refreshGraph();
        final URI legs = vf.createURI("urn:walksUsingLegs");
        final URI taxon = vf.createURI("urn:taxon");
        // Verify direct restrictions:
        final Map<URI, Set<Value>> valuesImplyingBiped = new HashMap<>();
        valuesImplyingBiped.put(legs, new HashSet<>());
        valuesImplyingBiped.get(legs).add(vf.createLiteral("2"));
        Assert.assertEquals(valuesImplyingBiped, inferenceEngine.getHasValueByType(vf.createURI("urn:Biped")));
        final Map<URI, Set<Value>> valuesImplyingMammal = new HashMap<>();
        valuesImplyingMammal.put(taxon, new HashSet<>());
        valuesImplyingMammal.get(taxon).add(vf.createURI("urn:Mammalia"));
        Assert.assertEquals(valuesImplyingMammal, inferenceEngine.getHasValueByType(vf.createURI("urn:Mammal")));
        final Map<URI, Set<Value>> valuesImplyingTunicate = new HashMap<>();
        valuesImplyingTunicate.put(taxon, new HashSet<>());
        valuesImplyingTunicate.get(taxon).add(vf.createURI("urn:Tunicata"));
        Assert.assertEquals(valuesImplyingTunicate, inferenceEngine.getHasValueByType(vf.createURI("urn:Tunicate")));
        final Map<URI, Set<Value>> valuesImplyingPlant = new HashMap<>();
        valuesImplyingPlant.put(taxon, new HashSet<>());
        valuesImplyingPlant.get(taxon).add(vf.createURI("urn:Plantae"));
        Assert.assertEquals(valuesImplyingPlant, inferenceEngine.getHasValueByType(vf.createURI("urn:Plant")));
        // Verify indirect restrictions given a supertype, including multiple properties where relevant:
        final Map<URI, Set<Value>> valuesImplyingVertebrate = new HashMap<>();
        valuesImplyingVertebrate.put(taxon, new HashSet<>(valuesImplyingMammal.get(taxon)));
        valuesImplyingVertebrate.get(taxon).add(vf.createURI("urn:Vertebrata"));
        Assert.assertEquals(valuesImplyingVertebrate, inferenceEngine.getHasValueByType(vf.createURI("urn:Vertebrate")));
        final Map<URI, Set<Value>> valuesImplyingAnimal = new HashMap<>();
        valuesImplyingAnimal.put(legs, valuesImplyingBiped.get(legs));
        valuesImplyingAnimal.put(taxon, new HashSet<>(valuesImplyingVertebrate.get(taxon)));
        valuesImplyingAnimal.get(taxon).addAll(valuesImplyingTunicate.get(taxon));
        Assert.assertEquals(valuesImplyingAnimal, inferenceEngine.getHasValueByType(vf.createURI("urn:Animal")));
    }

    @Test
    public void testUnionOf() throws Exception {
        final String ontology = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:A> owl:unionOf <urn:list1> . \n"
                + "  <urn:B> owl:unionOf <urn:list2> . \n"
                + "  <urn:list1> rdf:first <urn:X> . \n"
                + "  <urn:list1> rdf:rest <urn:list2> . \n"
                + "  <urn:list2> rdf:first <urn:Y> . \n"
                + "  <urn:list2> rdf:rest <urn:list3> . \n"
                + "  <urn:list3> rdf:first <urn:Z> . \n"
                + "  <urn:Y> rdfs:subClassOf <urn:SuperY> . \n"
                + "  <urn:SubY> rdfs:subClassOf <urn:Y> . \n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        inferenceEngine.refreshGraph();
        Graph subClassGraph = inferenceEngine.getSubClassOfGraph();
        Set<URI> subClassesA = inferenceEngine.findParents(subClassGraph, vf.createURI("urn:A"));
        Set<URI> subClassesB = inferenceEngine.findParents(subClassGraph, vf.createURI("urn:B"));
        Set<URI> expectedA = new HashSet<>();
        Set<URI> expectedB = new HashSet<>();
        expectedB.add(vf.createURI("urn:Y"));
        expectedB.add(vf.createURI("urn:SubY"));
        expectedB.add(vf.createURI("urn:Z"));
        expectedA.addAll(expectedB);
        expectedA.add(vf.createURI("urn:X"));
        Assert.assertEquals(expectedA, subClassesA);
        Assert.assertEquals(expectedB, subClassesB);
    }
}
