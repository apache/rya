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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.TypeStorage;
import org.apache.rya.indexing.entity.update.mongo.MongoEntityIndexer;
import org.apache.rya.mongodb.MongoTestBase;
import org.apache.rya.sail.config.RyaSailFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import com.google.common.collect.ImmutableSet;

public class MongoEntityIndexIT extends MongoTestBase {
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();
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
        expected.addBinding("flavor", ValueFactoryImpl.getInstance().createLiteral("Strawberry"));
        expected.addBinding("brand", ValueFactoryImpl.getInstance().createLiteral("Awesome Icecream"));

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
        final ValueFactory vf = ValueFactoryImpl.getInstance();
        final MapBindingSet expected = new MapBindingSet();
        //expected.addBinding("name", vf.createURI("http://www.w3.org/2001/SMLSchema#string", "George"));
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
        URI subject = VF.createURI("urn:alice");
        URI predicate = VF.createURI("urn:name");
        Value object = VF.createLiteral("Alice");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:eye");
        object = VF.createLiteral("blue");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:age");
        object = VF.createLiteral(30);
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createURI("urn:person")));

        //bob
        subject = VF.createURI("urn:bob");
        predicate = VF.createURI("urn:name");
        object = VF.createLiteral("Bob");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:eye");
        object = VF.createLiteral("brown");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:age");
        object = VF.createLiteral(57);
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createURI("urn:person")));

        //charlie
        subject = VF.createURI("urn:charlie");
        predicate = VF.createURI("urn:name");
        object = VF.createLiteral("Charlie");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:eye");
        object = VF.createLiteral("hazel");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:age");
        object = VF.createLiteral(25);
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createURI("urn:person")));

        //david
        subject = VF.createURI("urn:david");
        predicate = VF.createURI("urn:name");
        object = VF.createLiteral("David");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:eye");
        object = VF.createLiteral("brown");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:age");
        object = VF.createLiteral(30);
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createURI("urn:person")));

        //eve
        subject = VF.createURI("urn:eve");
        predicate = VF.createURI("urn:name");
        object = VF.createLiteral("Bob");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:age");
        object = VF.createLiteral(25);
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createURI("urn:person")));

        //frank
        subject = VF.createURI("urn:frank");
        predicate = VF.createURI("urn:name");
        object = VF.createLiteral("Frank");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:eye");
        object = VF.createLiteral("Hazel");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:age");
        object = VF.createLiteral(57);
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createURI("urn:person")));

        //george
        subject = VF.createURI("urn:george");
        predicate = VF.createURI("urn:name");
        object = VF.createLiteral("George");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:eye");
        object = VF.createLiteral("blue");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:age");
        object = VF.createLiteral(30);
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createURI("urn:person")));

        // Some Icecream typed objects.
        //chocolate
        subject = VF.createURI("urn:chocolate");
        predicate = VF.createURI("urn:brand");
        object = VF.createLiteral("Awesome Icecream");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:flavor");
        object = VF.createLiteral("Chocolate");
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createURI("urn:icecream")));

        //vanilla
        subject = VF.createURI("urn:vanilla");
        predicate = VF.createURI("urn:brand");
        object = VF.createLiteral("Awesome Icecream");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:flavor");
        object = VF.createLiteral("Vanilla");
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createURI("urn:icecream")));

        //strawberry
        subject = VF.createURI("urn:strawberry");
        predicate = VF.createURI("urn:brand");
        object = VF.createLiteral("Awesome Icecream");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createURI("urn:flavor");
        object = VF.createLiteral("Strawberry");
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createURI("urn:icecream")));
    }
}
