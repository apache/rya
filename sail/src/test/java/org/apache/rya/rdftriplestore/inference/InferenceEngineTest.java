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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;

import junit.framework.TestCase;

public class InferenceEngineTest extends TestCase {
    private Connector connector;
    private AccumuloRyaDAO dao;
    private static final ValueFactory VF = SimpleValueFactory.getInstance();
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
    public void testSubClassGraph() throws Exception {
        final String insert = "INSERT DATA { GRAPH <http://updated/test> {\n"
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
        final IRI a = VF.createIRI("urn:A");
        final IRI b = VF.createIRI("urn:B");
        final IRI c = VF.createIRI("urn:C");
        final IRI d = VF.createIRI("urn:D");
        final IRI e = VF.createIRI("urn:E");
        final IRI f = VF.createIRI("urn:F");
        final IRI g = VF.createIRI("urn:G");
        final IRI z = VF.createIRI("urn:Z");
        final IRI missing = VF.createIRI("urn:Missing");
        final Set<IRI> empty = new HashSet<>();
        final Set<IRI> belowLevel2 = new HashSet<>(Arrays.asList(a, b));
        final Set<IRI> belowLevel3 = new HashSet<>(Arrays.asList(a, b, c, d, e));
        final Set<IRI> belowLevel4 = new HashSet<>(Arrays.asList(a, b, c, d, e, f, g));
        Assert.assertEquals(empty, inferenceEngine.getSubClasses(a));
        Assert.assertEquals(empty, inferenceEngine.getSubClasses(b));
        Assert.assertEquals(empty, inferenceEngine.getSubClasses(z));
        Assert.assertEquals(empty, inferenceEngine.getSubClasses(missing));
        Assert.assertEquals(belowLevel2, inferenceEngine.getSubClasses(c));
        Assert.assertEquals(belowLevel3, inferenceEngine.getSubClasses(d));
        Assert.assertEquals(belowLevel3, inferenceEngine.getSubClasses(e));
        Assert.assertEquals(belowLevel4, inferenceEngine.getSubClasses(f));
        Assert.assertEquals(belowLevel4, inferenceEngine.getSubClasses(g));
    }

    @Test
    public void testSubPropertyGraph() throws Exception {
        final String insert = "INSERT DATA { GRAPH <http://updated/test> {\n"
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
        final Graph graph = inferenceEngine.getSubPropertyOfGraph();
        final IRI p = VF.createIRI("urn:p");
        final IRI q = VF.createIRI("urn:q");
        final IRI r = VF.createIRI("urn:r");
        final IRI s = VF.createIRI("urn:s");
        final IRI t = VF.createIRI("urn:t");
        final IRI u = VF.createIRI("urn:u");
        final IRI v = VF.createIRI("urn:v");
        final IRI w = VF.createIRI("urn:w");
        final IRI missing = VF.createIRI("urn:Missing");
        final Set<IRI> empty = new HashSet<>();
        final Set<IRI> belowQ = new HashSet<>(Arrays.asList(p));
        final Set<IRI> belowR = new HashSet<>(Arrays.asList(p, r, s));
        final Set<IRI> belowT = new HashSet<>(Arrays.asList(p, q));
        final Set<IRI> belowU = new HashSet<>(Arrays.asList(p, q, r, s, t, u, v));
        Assert.assertEquals(empty, InferenceEngine.findParents(graph, p));
        Assert.assertEquals(empty, InferenceEngine.findParents(graph, w));
        Assert.assertEquals(empty, InferenceEngine.findParents(graph, missing));
        Assert.assertEquals(belowQ, InferenceEngine.findParents(graph, q));
        Assert.assertEquals(belowR, InferenceEngine.findParents(graph, r));
        Assert.assertEquals(belowR, InferenceEngine.findParents(graph, s));
        Assert.assertEquals(belowT, InferenceEngine.findParents(graph, t));
        Assert.assertEquals(belowU, InferenceEngine.findParents(graph, u));
        Assert.assertEquals(belowU, InferenceEngine.findParents(graph, v));
    }

    @Test
    public void testDomainRange() throws Exception {
        final String insert = "INSERT DATA { GRAPH <http://updated/test> {\n"
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
        final Set<IRI> hasDomainD1 = inferenceEngine.getPropertiesWithDomain(VF.createIRI("urn:D1"));
        final Set<IRI> hasDomainD2 = inferenceEngine.getPropertiesWithDomain(VF.createIRI("urn:D2"));
        final Set<IRI> hasDomainD3 = inferenceEngine.getPropertiesWithDomain(VF.createIRI("urn:D3"));
        final Set<IRI> hasRangeD1 = inferenceEngine.getPropertiesWithRange(VF.createIRI("urn:D1"));
        final Set<IRI> hasRangeD2 = inferenceEngine.getPropertiesWithRange(VF.createIRI("urn:D2"));
        final Set<IRI> hasRangeD3 = inferenceEngine.getPropertiesWithRange(VF.createIRI("urn:D3"));
        final Set<IRI> hasDomainR1 = inferenceEngine.getPropertiesWithDomain(VF.createIRI("urn:R1"));
        final Set<IRI> hasDomainR2 = inferenceEngine.getPropertiesWithDomain(VF.createIRI("urn:R2"));
        final Set<IRI> hasDomainR3 = inferenceEngine.getPropertiesWithDomain(VF.createIRI("urn:R3"));
        final Set<IRI> hasRangeR1 = inferenceEngine.getPropertiesWithRange(VF.createIRI("urn:R1"));
        final Set<IRI> hasRangeR2 = inferenceEngine.getPropertiesWithRange(VF.createIRI("urn:R2"));
        final Set<IRI> hasRangeR3 = inferenceEngine.getPropertiesWithRange(VF.createIRI("urn:R3"));
        final Set<IRI> empty = new HashSet<>();
        final Set<IRI> expectedForward = new HashSet<>();
        expectedForward.add(VF.createIRI("urn:p2"));
        expectedForward.add(VF.createIRI("urn:p1"));
        expectedForward.add(VF.createIRI("urn:q2"));
        expectedForward.add(VF.createIRI("urn:q1"));
        final Set<IRI> expectedInverse = new HashSet<>();
        expectedInverse.add(VF.createIRI("urn:i1"));
        expectedInverse.add(VF.createIRI("urn:i2"));
        expectedInverse.add(VF.createIRI("urn:j1"));
        expectedInverse.add(VF.createIRI("urn:j2"));
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
    public void testSomeValuesFrom() throws Exception {
        final String insert = "INSERT DATA { GRAPH <http://updated/test> {\n"
                // base restrictions
                + "  <urn:Chair> owl:onProperty <urn:headOf> ; owl:someValuesFrom <urn:Department> .\n"
                + "  <urn:Dean> owl:onProperty <urn:headOf> ; owl:someValuesFrom <urn:College> .\n"
                // classes related to the restriction type
                + "  <urn:ScienceDepartmentChair> rdfs:subClassOf <urn:Chair> .\n"
                + "  <urn:Chair> rdfs:subClassOf <urn:Person> .\n"
                + "  <urn:Dean> rdfs:subClassOf <urn:Person> .\n"
                + "  <urn:Student> rdfs:subClassOf <urn:Person> .\n"
                // classes related to the value type
                + "  <urn:ScienceDepartment> rdfs:subClassOf <urn:Department> .\n"
                + "  <urn:HumanitiesDepartment> rdfs:subClassOf <urn:Department> .\n"
                + "  <urn:Department> rdfs:subClassOf <urn:Organization> .\n"
                + "  <urn:College> rdfs:subClassOf <urn:Organization> .\n"
                // properties related to the restriction property
                + "  <urn:temporaryHeadOf> rdfs:subPropertyOf <urn:headOf> .\n"
                + "  <urn:headOf> rdfs:subPropertyOf <urn:worksFor> .\n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, insert).execute();
        inferenceEngine.refreshGraph();
        final Set<IRI> properties = new HashSet<>();
        properties.add(VF.createIRI("urn:headOf"));
        properties.add(VF.createIRI("urn:temporaryHeadOf"));
        final Map<Resource, Set<IRI>> chairDerivations = new HashMap<>();
        chairDerivations.put(VF.createIRI("urn:Department"), properties);
        chairDerivations.put(VF.createIRI("urn:ScienceDepartment"), properties);
        chairDerivations.put(VF.createIRI("urn:HumanitiesDepartment"), properties);
        final Map<Resource, Set<IRI>> deanDerivations = new HashMap<>();
        deanDerivations.put(VF.createIRI("urn:College"), properties);
        final Map<Resource, Set<IRI>> combinedDerivations = new HashMap<>(chairDerivations);
        combinedDerivations.put(VF.createIRI("urn:College"), properties);
        // Get someValuesFrom restrictions given the direct types
        Assert.assertEquals(deanDerivations, inferenceEngine.getSomeValuesFromByRestrictionType(VF.createIRI("urn:Dean")));
        Assert.assertEquals(chairDerivations, inferenceEngine.getSomeValuesFromByRestrictionType(VF.createIRI("urn:Chair")));
        // Finds the subtype's restrictions given the supertype
        Assert.assertEquals(combinedDerivations, inferenceEngine.getSomeValuesFromByRestrictionType(VF.createIRI("urn:Person")));
        // Finds nothing if given a subtype which is not a restriction
        Assert.assertEquals(new HashMap<>(), inferenceEngine.getSomeValuesFromByRestrictionType(VF.createIRI("urn:ScienceDepartmentChair")));
    }

    @Test
    public void testAllValuesFrom() throws Exception {
        final String insert = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Dog> owl:onProperty <urn:relative> ; owl:allValuesFrom <urn:Dog> .\n"
                + "  <urn:Retriever> rdfs:subClassOf <urn:Dog> .\n"
                + "  <urn:Terrier> rdfs:subClassOf <urn:Dog> .\n"
                + "  <urn:Terrier> owl:onProperty <urn:relative> ; owl:allValuesFrom <urn:Terrier> .\n"
                + "  <urn:Cairn_Terrier> rdfs:subClassOf <urn:Terrier> .\n"
                + "  <urn:parent> rdfs:subPropertyOf <urn:relative> .\n"
                + "  <urn:Dog> rdfs:subClassOf <urn:Mammal> .\n"
                + "  <urn:Person> rdfs:subClassOf <urn:Mammal> .\n"
                + "  <urn:Person> owl:onProperty <urn:relative> ; owl:allValuesFrom <urn:Person> .\n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, insert).execute();
        inferenceEngine.refreshGraph();
        final Map<Resource, Set<IRI>> restrictionsImplyingTerrier = new HashMap<>();
        final Set<IRI> properties = new HashSet<>();
        properties.add(VF.createIRI("urn:parent"));
        properties.add(VF.createIRI("urn:relative"));
        restrictionsImplyingTerrier.put(VF.createIRI("urn:Terrier"), properties);
        restrictionsImplyingTerrier.put(VF.createIRI("urn:Cairn_Terrier"), properties);
        Assert.assertEquals(restrictionsImplyingTerrier, inferenceEngine.getAllValuesFromByValueType(VF.createIRI("urn:Terrier")));
        final Map<Resource, Set<IRI>> restrictionsImplyingDog = new HashMap<>(restrictionsImplyingTerrier);
        restrictionsImplyingDog.put(VF.createIRI("urn:Dog"), properties);
        restrictionsImplyingDog.put(VF.createIRI("urn:Retriever"), properties);
        Assert.assertEquals(restrictionsImplyingDog, inferenceEngine.getAllValuesFromByValueType(VF.createIRI("urn:Dog")));
        final Map<Resource, Set<IRI>> restrictionsImplyingMammal = new HashMap<>(restrictionsImplyingDog);
        restrictionsImplyingMammal.put(VF.createIRI("urn:Person"), properties);
        Assert.assertEquals(restrictionsImplyingMammal, inferenceEngine.getAllValuesFromByValueType(VF.createIRI("urn:Mammal")));
    }

    @Test
    public void testHasValueGivenProperty() throws Exception {
        final String insert = "INSERT DATA { GRAPH <http://updated/test> {\n"
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
        vertebrateTaxa.add(VF.createIRI("urn:Vertebrata"));
        tunicateTaxa.add(VF.createIRI("urn:Tunicata"));
        final Set<Value> mammalTaxa = new HashSet<>(vertebrateTaxa);
        mammalTaxa.add(VF.createIRI("urn:Mammalia"));
        typeToValueImplications.put(VF.createIRI("urn:Vertebrate"), vertebrateTaxa);
        typeToValueImplications.put(VF.createIRI("urn:Tunicate"), tunicateTaxa);
        typeToValueImplications.put(VF.createIRI("urn:Mammal"), mammalTaxa);
        Assert.assertEquals(typeToValueImplications, inferenceEngine.getHasValueByProperty(VF.createIRI("urn:taxon")));
    }

    @Test
    public void testHasValueGivenType() throws Exception {
        final String insert = "INSERT DATA { GRAPH <http://updated/test> {\n"
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
        final IRI legs = VF.createIRI("urn:walksUsingLegs");
        final IRI taxon = VF.createIRI("urn:taxon");
        // Verify direct restrictions:
        final Map<IRI, Set<Value>> valuesImplyingBiped = new HashMap<>();
        valuesImplyingBiped.put(legs, new HashSet<>());
        valuesImplyingBiped.get(legs).add(VF.createLiteral("2"));
        Assert.assertEquals(valuesImplyingBiped, inferenceEngine.getHasValueByType(VF.createIRI("urn:Biped")));
        final Map<IRI, Set<Value>> valuesImplyingMammal = new HashMap<>();
        valuesImplyingMammal.put(taxon, new HashSet<>());
        valuesImplyingMammal.get(taxon).add(VF.createIRI("urn:Mammalia"));
        Assert.assertEquals(valuesImplyingMammal, inferenceEngine.getHasValueByType(VF.createIRI("urn:Mammal")));
        final Map<IRI, Set<Value>> valuesImplyingTunicate = new HashMap<>();
        valuesImplyingTunicate.put(taxon, new HashSet<>());
        valuesImplyingTunicate.get(taxon).add(VF.createIRI("urn:Tunicata"));
        Assert.assertEquals(valuesImplyingTunicate, inferenceEngine.getHasValueByType(VF.createIRI("urn:Tunicate")));
        final Map<IRI, Set<Value>> valuesImplyingPlant = new HashMap<>();
        valuesImplyingPlant.put(taxon, new HashSet<>());
        valuesImplyingPlant.get(taxon).add(VF.createIRI("urn:Plantae"));
        Assert.assertEquals(valuesImplyingPlant, inferenceEngine.getHasValueByType(VF.createIRI("urn:Plant")));
        // Verify indirect restrictions given a supertype, including multiple properties where relevant:
        final Map<IRI, Set<Value>> valuesImplyingVertebrate = new HashMap<>();
        valuesImplyingVertebrate.put(taxon, new HashSet<>(valuesImplyingMammal.get(taxon)));
        valuesImplyingVertebrate.get(taxon).add(VF.createIRI("urn:Vertebrata"));
        Assert.assertEquals(valuesImplyingVertebrate, inferenceEngine.getHasValueByType(VF.createIRI("urn:Vertebrate")));
        final Map<IRI, Set<Value>> valuesImplyingAnimal = new HashMap<>();
        valuesImplyingAnimal.put(legs, valuesImplyingBiped.get(legs));
        valuesImplyingAnimal.put(taxon, new HashSet<>(valuesImplyingVertebrate.get(taxon)));
        valuesImplyingAnimal.get(taxon).addAll(valuesImplyingTunicate.get(taxon));
        Assert.assertEquals(valuesImplyingAnimal, inferenceEngine.getHasValueByType(VF.createIRI("urn:Animal")));
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
        final Set<IRI> subClassesA = inferenceEngine.getSubClasses(VF.createIRI("urn:A"));
        final Set<IRI> subClassesB = inferenceEngine.getSubClasses(VF.createIRI("urn:B"));
        final Set<IRI> expectedA = new HashSet<>();
        final Set<IRI> expectedB = new HashSet<>();
        expectedB.add(VF.createIRI("urn:Y"));
        expectedB.add(VF.createIRI("urn:SubY"));
        expectedB.add(VF.createIRI("urn:Z"));
        expectedA.addAll(expectedB);
        expectedA.add(VF.createIRI("urn:X"));
        Assert.assertEquals(expectedA, subClassesA);
        Assert.assertEquals(expectedB, subClassesB);
    }

    public void testIntersectionOf() throws Exception {
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
                + "  <urn:Mom> owl:intersectionOf _:bnode5 . \n"
                + "  _:bnode5 rdf:first <urn:Woman> . \n"
                + "  _:bnode5 rdf:rest _:bnode6 . \n"
                + "  _:bnode6 rdf:first <urn:Parent> . \n"
                + "  _:bnode6 rdf:rest rdf:nil . \n"
                + "  <urn:Mother> rdfs:subClassOf <urn:ImmediateFamilyMember> . \n"
                + "  <urn:ImmediateFamilyMember> rdfs:subClassOf <urn:Relative> . \n"
                + "}}";

        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        inferenceEngine.refreshGraph();

        final IRI mother = VF.createIRI("urn:Mother");
        final IRI father = VF.createIRI("urn:Father");
        final IRI woman = VF.createIRI("urn:Woman");
        final IRI parent = VF.createIRI("urn:Parent");
        final IRI man = VF.createIRI("urn:Man");
        final IRI mom = VF.createIRI("urn:Mom");
        final IRI immediateFamilyMember = VF.createIRI("urn:ImmediateFamilyMember");
        final IRI relative = VF.createIRI("urn:Relative");

        final List<Set<Resource>> intersectionsImplyingMother = Arrays.asList(Sets.newHashSet(woman, parent));
        Assert.assertEquals(intersectionsImplyingMother, inferenceEngine.getIntersectionsImplying(mother));
        final List<Set<Resource>> intersectionsImplyingFather = Arrays.asList(Sets.newHashSet(man, parent));
        Assert.assertEquals(intersectionsImplyingFather, inferenceEngine.getIntersectionsImplying(father));

        // Check that Mother is a subclassOf Parent and Woman and
        // ImmediateFamilyMember and Relative. Also, Mother is a subclassOf
        // Mother and Mom through inferring equivalentClass.
        final Set<IRI> motherSuperClassUris = inferenceEngine.getSuperClasses(mother);
        Assert.assertNotNull(motherSuperClassUris);
        Assert.assertEquals(6, motherSuperClassUris.size());
        Assert.assertTrue(motherSuperClassUris.contains(parent));
        Assert.assertTrue(motherSuperClassUris.contains(woman));
        Assert.assertTrue(motherSuperClassUris.contains(immediateFamilyMember));
        Assert.assertTrue(motherSuperClassUris.contains(relative));
        Assert.assertTrue(motherSuperClassUris.contains(mother));
        Assert.assertTrue(motherSuperClassUris.contains(mom));
        // Check that Father is a subclassOf Parent and Man
        final Set<IRI> fatherSuperClassUris = inferenceEngine.getSuperClasses(father);
        Assert.assertNotNull(fatherSuperClassUris);
        Assert.assertEquals(2, fatherSuperClassUris.size());
        Assert.assertTrue(fatherSuperClassUris.contains(parent));
        Assert.assertTrue(fatherSuperClassUris.contains(man));

        // Check that Mom is a subclassOf Parent and Woman and
        // ImmediateFamilyMember and Relative. The last 2 should be inferred
        // from having the same intersection as Mother. Also, Mom is a
        // subclassOf Mother and Mom through inferring equivalentClass.
        final Set<IRI> momSuperClassUris = inferenceEngine.getSuperClasses(mom);
        Assert.assertNotNull(momSuperClassUris);
        Assert.assertEquals(6, momSuperClassUris.size());
        Assert.assertTrue(momSuperClassUris.contains(parent));
        Assert.assertTrue(momSuperClassUris.contains(woman));
        Assert.assertTrue(momSuperClassUris.contains(immediateFamilyMember));
        Assert.assertTrue(momSuperClassUris.contains(relative));
        Assert.assertTrue(momSuperClassUris.contains(mother));
        Assert.assertTrue(momSuperClassUris.contains(mom));
    }

    @Test
    public void testOneOf() throws Exception {
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
                + "}}";

        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        inferenceEngine.refreshGraph();

        final IRI suits = VF.createIRI("urn:Suits");
        final IRI ranks = VF.createIRI("urn:Ranks");

        final IRI clubs = VF.createIRI("urn:Clubs");
        final IRI diamonds = VF.createIRI("urn:Diamonds");
        final IRI hearts = VF.createIRI("urn:Hearts");
        final IRI spades = VF.createIRI("urn:Spades");

        final IRI ace = VF.createIRI("urn:Ace");
        final IRI two = VF.createIRI("urn:2");
        final IRI three = VF.createIRI("urn:3");
        final IRI four = VF.createIRI("urn:4");
        final IRI five = VF.createIRI("urn:5");
        final IRI six = VF.createIRI("urn:6");
        final IRI seven = VF.createIRI("urn:7");
        final IRI eight = VF.createIRI("urn:8");
        final IRI nine = VF.createIRI("urn:9");
        final IRI ten = VF.createIRI("urn:10");
        final IRI jack = VF.createIRI("urn:Jack");
        final IRI queen = VF.createIRI("urn:Queen");
        final IRI king = VF.createIRI("urn:King");

        final IRI joker = VF.createIRI("urn:Joker");

        final boolean isJokerEnumeratedType = inferenceEngine.isEnumeratedType(joker);
        Assert.assertFalse(isJokerEnumeratedType);

        final boolean isSuitsEnumeratedType = inferenceEngine.isEnumeratedType(suits);
        Assert.assertTrue(isSuitsEnumeratedType);
        final Set<Resource> enumerationImplyingSuits = Sets.newHashSet(clubs, diamonds, hearts, spades);
        final Set<Resource> actualCardSuits = inferenceEngine.getEnumeration(suits);
        Assert.assertEquals(enumerationImplyingSuits, actualCardSuits);

        final boolean isRanksEnumeratedType = inferenceEngine.isEnumeratedType(ranks);
        Assert.assertTrue(isRanksEnumeratedType);
        final Set<Resource> enumerationImplyingRanks = Sets.newHashSet(ace, two, three, four, five, six, seven, eight, nine, ten, jack, queen, king);
        final Set<Resource> actualCardRanks = inferenceEngine.getEnumeration(ranks);
        Assert.assertEquals(enumerationImplyingRanks, actualCardRanks);
   }

    @Test
    public void hasSelfTest() throws Exception {
        final String ontology = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:Narcissist> owl:onProperty <urn:love> ; owl:hasSelf \"true\" . \n" + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        inferenceEngine.refreshGraph();
        final Set<Resource> expectedTypes = new HashSet<>();
        expectedTypes.add(VF.createIRI("urn:Narcissist"));
        Assert.assertEquals(expectedTypes, inferenceEngine.getHasSelfImplyingProperty(VF.createIRI("urn:love")));

        final Set<IRI> expectedProperties = new HashSet<>();
        expectedProperties.add(VF.createIRI("urn:love"));
        Assert.assertEquals(expectedProperties, inferenceEngine.getHasSelfImplyingType(VF.createIRI("urn:Narcissist")));
    }

    @Test
    public void testPropertyTypes() throws Exception {
        final String ontology = "INSERT DATA { GRAPH <http://updated/test> {\n"
                + "  <urn:comment> a owl:AnnotationProperty .\n"
                + "  <urn:olderThan> a owl:TransitiveProperty, owl:IrreflexiveProperty, owl:AsymmetricProperty .\n"
                + "  <urn:notYoungerThan> a owl:TransitiveProperty, owl:ReflexiveProperty .\n"
                + "  <urn:related> a owl:Property, owl:SymmetricProperty, owl:TransitiveProperty .\n"
                + "  <urn:knows> a owl:SymmetricProperty, owl:ObjectProperty, owl:ReflexiveProperty .\n"
                + "  <urn:sameAgeAs> a owl:SymmetricProperty, owl:ReflexiveProperty, owl:TransitiveProperty .\n"
                + "}}";
        conn.prepareUpdate(QueryLanguage.SPARQL, ontology).execute();
        inferenceEngine.refreshGraph();
        final IRI comment = VF.createIRI("urn:comment"); // none of the three supported types
        final IRI older = VF.createIRI("urn:olderThan"); // transitive only
        final IRI notYounger = VF.createIRI("urn:notYoungerThan"); // transitive and reflexive
        final IRI related = VF.createIRI("urn:related"); // transitive and symmetric
        final IRI knows = VF.createIRI("urn:knows"); // reflexive and symmetric
        final IRI sameAge = VF.createIRI("urn:sameAgeAs"); // all three
        // symmetry
        Assert.assertFalse(inferenceEngine.isSymmetricProperty(comment));
        Assert.assertFalse(inferenceEngine.isSymmetricProperty(older));
        Assert.assertFalse(inferenceEngine.isSymmetricProperty(notYounger));
        Assert.assertTrue(inferenceEngine.isSymmetricProperty(related));
        Assert.assertTrue(inferenceEngine.isSymmetricProperty(knows));
        Assert.assertTrue(inferenceEngine.isSymmetricProperty(sameAge));
        // transitivity
        Assert.assertFalse(inferenceEngine.isTransitiveProperty(comment));
        Assert.assertTrue(inferenceEngine.isTransitiveProperty(older));
        Assert.assertTrue(inferenceEngine.isTransitiveProperty(notYounger));
        Assert.assertTrue(inferenceEngine.isTransitiveProperty(related));
        Assert.assertFalse(inferenceEngine.isTransitiveProperty(knows));
        Assert.assertTrue(inferenceEngine.isTransitiveProperty(sameAge));
        // reflexivity
        Assert.assertFalse(inferenceEngine.isReflexiveProperty(comment));
        Assert.assertFalse(inferenceEngine.isReflexiveProperty(older));
        Assert.assertTrue(inferenceEngine.isReflexiveProperty(notYounger));
        Assert.assertFalse(inferenceEngine.isReflexiveProperty(related));
        Assert.assertTrue(inferenceEngine.isReflexiveProperty(knows));
        Assert.assertTrue(inferenceEngine.isReflexiveProperty(sameAge));
    }
}
