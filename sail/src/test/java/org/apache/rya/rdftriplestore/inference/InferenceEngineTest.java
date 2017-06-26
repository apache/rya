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
}
