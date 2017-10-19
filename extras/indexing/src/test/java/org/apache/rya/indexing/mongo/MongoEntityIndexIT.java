/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.mongo;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.TypeStorage;
import org.apache.rya.indexing.entity.update.mongo.MongoEntityIndexer;
import org.apache.rya.mongodb.MongoTestBase;
import org.apache.rya.sail.config.RyaSailFactory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.Sail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MongoEntityIndexIT extends MongoTestBase {
    private static final ValueFactory vf = SimpleValueFactory.getInstance();
    private SailRepositoryConnection conn;
    private MongoEntityIndexer indexer;

    @Before
    public void setUp() throws Exception {
        conf.setBoolean(ConfigUtils.USE_MONGO, true);
        conf.setBoolean(ConfigUtils.USE_ENTITY, true);

        final Sail sail = RyaSailFactory.getInstance(conf);
        conn = new SailRepository(sail).getConnection();
        conn.begin();

        indexer = new MongoEntityIndexer();
        indexer.setConf(conf);
        indexer.init();
    }

    @After
    public void tearDown() throws Exception {
        if (conn != null) {
            conn.clear();
        }
        if (indexer != null) {
            indexer.close();
        }
    }

    @Test
    public void ensureInEntityStore_Test() throws Exception {
        setupTypes();
        addStatements();

        final EntityStorage entities = indexer.getEntityStorage(conf);
        final RyaURI subject = new RyaURI("urn:alice");
        final Optional<Entity> alice = entities.get(subject);
        assertTrue(alice.isPresent());
    }

    @Test
    public void sparqlQuery_Test() throws Exception {
        setupTypes();
        addStatements();
        //conn.commit();

        final String query = "SELECT * WHERE { " +
                "<urn:strawberry> <" + RDF.TYPE + "> <urn:icecream> ."+
                "<urn:strawberry> <urn:brand> ?brand . " +
                "<urn:strawberry> <urn:flavor> ?flavor . " +
            "}";

        final TupleQueryResult rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
        final Set<BindingSet> results = new HashSet<>();
        while(rez.hasNext()) {
            final BindingSet bs = rez.next();
            results.add(bs);
        }
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("flavor", SimpleValueFactory.getInstance().createLiteral("Strawberry"));
        expected.addBinding("brand", SimpleValueFactory.getInstance().createLiteral("Awesome Icecream"));

        assertEquals(1, results.size());
        assertEquals(expected, results.iterator().next());
    }

    @Test
    public void partialQuery_Test() throws Exception {
        setupTypes();
        addStatements();
        conn.commit();

        final String query = "SELECT * WHERE { " +
                "<urn:george> <" + RDF.TYPE + "> <urn:person> ."+
                "<urn:george> <urn:name> ?name . " +
                "<urn:george> <urn:eye> ?eye . " +
            "}";

        final TupleQueryResult rez = conn.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
        final Set<BindingSet> results = new HashSet<>();
        while(rez.hasNext()) {
            final BindingSet bs = rez.next();
            System.out.println(bs);
            results.add(bs);
        }
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final MapBindingSet expected = new MapBindingSet();
        //expected.addBinding("name", vf.createIRI("http://www.w3.org/2001/SMLSchema#string", "George"));
        expected.addBinding("name", vf.createLiteral("George"));
        expected.addBinding("eye", vf.createLiteral("blue"));

        assertEquals(1, results.size());
        assertEquals(expected, results.iterator().next());
    }

    private void setupTypes() throws Exception {
        final TypeStorage typeStore = indexer.getTypeStorage(conf);
        // Add some Types to the storage.
        final Type cat = new Type(new RyaURI("urn:cat"),
                ImmutableSet.<RyaURI>builder()
                    .add(new RyaURI("urn:numLegs"))
                    .add(new RyaURI("urn:eye"))
                    .add(new RyaURI("urn:species"))
                    .build());

        final Type dog = new Type(new RyaURI("urn:dog"),
                ImmutableSet.<RyaURI>builder()
                    .add(new RyaURI("urn:numLegs"))
                    .add(new RyaURI("urn:eye"))
                    .add(new RyaURI("urn:species"))
                    .build());

        final Type icecream = new Type(new RyaURI("urn:icecream"),
                ImmutableSet.<RyaURI>builder()
                    .add(new RyaURI("urn:brand"))
                    .add(new RyaURI("urn:flavor"))
                    .add(new RyaURI("urn:cost"))
                    .build());

        final Type person = new Type(new RyaURI("urn:person"),
                ImmutableSet.<RyaURI>builder()
                    .add(new RyaURI("urn:name"))
                    .add(new RyaURI("urn:age"))
                    .add(new RyaURI("urn:eye"))
                    .build());

        typeStore.create(cat);
        typeStore.create(dog);
        typeStore.create(icecream);
        typeStore.create(person);
    }

    private void addStatements() throws Exception {
        //alice
        IRI subject = vf.createIRI("urn:alice");
        IRI predicate = vf.createIRI("urn:name");
        Value object = vf.createLiteral("Alice");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:eye");
        object = vf.createLiteral("blue");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:age");
        object = vf.createLiteral(30);
        conn.add(vf.createStatement(subject, predicate, object));
        conn.add(vf.createStatement(subject, RDF.TYPE, vf.createIRI("urn:person")));

        //bob
        subject = vf.createIRI("urn:bob");
        predicate = vf.createIRI("urn:name");
        object = vf.createLiteral("Bob");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:eye");
        object = vf.createLiteral("brown");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:age");
        object = vf.createLiteral(57);
        conn.add(vf.createStatement(subject, predicate, object));
        conn.add(vf.createStatement(subject, RDF.TYPE, vf.createIRI("urn:person")));

        //charlie
        subject = vf.createIRI("urn:charlie");
        predicate = vf.createIRI("urn:name");
        object = vf.createLiteral("Charlie");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:eye");
        object = vf.createLiteral("hazel");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:age");
        object = vf.createLiteral(25);
        conn.add(vf.createStatement(subject, predicate, object));
        conn.add(vf.createStatement(subject, RDF.TYPE, vf.createIRI("urn:person")));

        //david
        subject = vf.createIRI("urn:david");
        predicate = vf.createIRI("urn:name");
        object = vf.createLiteral("David");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:eye");
        object = vf.createLiteral("brown");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:age");
        object = vf.createLiteral(30);
        conn.add(vf.createStatement(subject, predicate, object));
        conn.add(vf.createStatement(subject, RDF.TYPE, vf.createIRI("urn:person")));

        //eve
        subject = vf.createIRI("urn:eve");
        predicate = vf.createIRI("urn:name");
        object = vf.createLiteral("Bob");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:age");
        object = vf.createLiteral(25);
        conn.add(vf.createStatement(subject, predicate, object));
        conn.add(vf.createStatement(subject, RDF.TYPE, vf.createIRI("urn:person")));

        //frank
        subject = vf.createIRI("urn:frank");
        predicate = vf.createIRI("urn:name");
        object = vf.createLiteral("Frank");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:eye");
        object = vf.createLiteral("Hazel");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:age");
        object = vf.createLiteral(57);
        conn.add(vf.createStatement(subject, predicate, object));
        conn.add(vf.createStatement(subject, RDF.TYPE, vf.createIRI("urn:person")));

        //george
        subject = vf.createIRI("urn:george");
        predicate = vf.createIRI("urn:name");
        object = vf.createLiteral("George");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:eye");
        object = vf.createLiteral("blue");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:age");
        object = vf.createLiteral(30);
        conn.add(vf.createStatement(subject, predicate, object));
        conn.add(vf.createStatement(subject, RDF.TYPE, vf.createIRI("urn:person")));

        // Some Icecream typed objects.
        //chocolate
        subject = vf.createIRI("urn:chocolate");
        predicate = vf.createIRI("urn:brand");
        object = vf.createLiteral("Awesome Icecream");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:flavor");
        object = vf.createLiteral("Chocolate");
        conn.add(vf.createStatement(subject, predicate, object));
        conn.add(vf.createStatement(subject, RDF.TYPE, vf.createIRI("urn:icecream")));

        //vanilla
        subject = vf.createIRI("urn:vanilla");
        predicate = vf.createIRI("urn:brand");
        object = vf.createLiteral("Awesome Icecream");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:flavor");
        object = vf.createLiteral("Vanilla");
        conn.add(vf.createStatement(subject, predicate, object));
        conn.add(vf.createStatement(subject, RDF.TYPE, vf.createIRI("urn:icecream")));

        //strawberry
        subject = vf.createIRI("urn:strawberry");
        predicate = vf.createIRI("urn:brand");
        object = vf.createLiteral("Awesome Icecream");
        conn.add(vf.createStatement(subject, predicate, object));
        predicate = vf.createIRI("urn:flavor");
        object = vf.createLiteral("Strawberry");
        conn.add(vf.createStatement(subject, predicate, object));
        conn.add(vf.createStatement(subject, RDF.TYPE, vf.createIRI("urn:icecream")));
    }
}
