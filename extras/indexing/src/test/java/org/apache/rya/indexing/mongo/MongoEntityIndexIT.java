/*
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

import java.math.BigInteger;
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
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoITBase;
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
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class MongoEntityIndexIT extends MongoITBase {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Override
    protected void updateConfiguration(final MongoDBRdfConfiguration conf) {
        conf.setBoolean(ConfigUtils.USE_MONGO, true);
        conf.setBoolean(ConfigUtils.USE_ENTITY, true);
    }

    @Test
    public void ensureInEntityStore_Test() throws Exception {
        final Sail sail = RyaSailFactory.getInstance(conf);
        SailRepositoryConnection conn = new SailRepository(sail).getConnection();
        conn.begin();

        try(MongoEntityIndexer indexer = new MongoEntityIndexer()) {
            indexer.setConf(conf);
            indexer.init();

            setupTypes(indexer);
            addStatements(conn);

            final EntityStorage entities = indexer.getEntityStorage();
            final RyaURI subject = new RyaURI("urn:alice");
            final Optional<Entity> alice = entities.get(subject);
            assertTrue(alice.isPresent());
        } finally {
            conn.close();
        }
    }

    @Test
    public void sparqlQuery_Test() throws Exception {
        final Sail sail = RyaSailFactory.getInstance(conf);
        SailRepositoryConnection conn = new SailRepository(sail).getConnection();
        conn.begin();

        try(MongoEntityIndexer indexer = new MongoEntityIndexer()) {
            indexer.setConf(conf);
            indexer.init();

            setupTypes(indexer);
            addStatements(conn);

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
            expected.addBinding("flavor", VF.createLiteral("Strawberry"));
            expected.addBinding("brand", VF.createLiteral("Awesome Icecream"));

            assertEquals(1, results.size());
            assertEquals(expected, results.iterator().next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void partialQuery_Test() throws Exception {
        final Sail sail = RyaSailFactory.getInstance(conf);
        SailRepositoryConnection conn = new SailRepository(sail).getConnection();
        conn.begin();

        try(MongoEntityIndexer indexer = new MongoEntityIndexer()) {
            indexer.setConf(conf);
            indexer.init();

            setupTypes(indexer);
            addStatements(conn);
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
            final MapBindingSet expected = new MapBindingSet();
            //expected.addBinding("name", VF.createIRI("http://www.w3.org/2001/SMLSchema#string", "George"));
            expected.addBinding("name", VF.createLiteral("George"));
            expected.addBinding("eye", VF.createLiteral("blue"));

            assertEquals(1, results.size());
            assertEquals(expected, results.iterator().next());
        } finally {
            conn.close();
        }
    }

    private void setupTypes(MongoEntityIndexer indexer) throws Exception {
        final TypeStorage typeStore = indexer.getTypeStorage();
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

    private void addStatements(SailRepositoryConnection conn) throws Exception {
        //alice
        IRI subject = VF.createIRI("urn:alice");
        IRI predicate = VF.createIRI("urn:name");
        Value object = VF.createLiteral("Alice");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:eye");
        object = VF.createLiteral("blue");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:age");
        object = VF.createLiteral(BigInteger.valueOf(30));
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createIRI("urn:person")));

        //bob
        subject = VF.createIRI("urn:bob");
        predicate = VF.createIRI("urn:name");
        object = VF.createLiteral("Bob");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:eye");
        object = VF.createLiteral("brown");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:age");
        object = VF.createLiteral(BigInteger.valueOf(57));
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createIRI("urn:person")));

        //charlie
        subject = VF.createIRI("urn:charlie");
        predicate = VF.createIRI("urn:name");
        object = VF.createLiteral("Charlie");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:eye");
        object = VF.createLiteral("hazel");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:age");
        object = VF.createLiteral(BigInteger.valueOf(25));
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createIRI("urn:person")));

        //david
        subject = VF.createIRI("urn:david");
        predicate = VF.createIRI("urn:name");
        object = VF.createLiteral("David");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:eye");
        object = VF.createLiteral("brown");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:age");
        object = VF.createLiteral(BigInteger.valueOf(30));
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createIRI("urn:person")));

        //eve
        subject = VF.createIRI("urn:eve");
        predicate = VF.createIRI("urn:name");
        object = VF.createLiteral("Bob");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:age");
        object = VF.createLiteral(BigInteger.valueOf(25));
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createIRI("urn:person")));

        //frank
        subject = VF.createIRI("urn:frank");
        predicate = VF.createIRI("urn:name");
        object = VF.createLiteral("Frank");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:eye");
        object = VF.createLiteral("Hazel");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:age");
        object = VF.createLiteral(BigInteger.valueOf(57));
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createIRI("urn:person")));

        //george
        subject = VF.createIRI("urn:george");
        predicate = VF.createIRI("urn:name");
        object = VF.createLiteral("George");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:eye");
        object = VF.createLiteral("blue");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:age");
        object = VF.createLiteral(BigInteger.valueOf(30));
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createIRI("urn:person")));

        // Some Icecream typed objects.
        //chocolate
        subject = VF.createIRI("urn:chocolate");
        predicate = VF.createIRI("urn:brand");
        object = VF.createLiteral("Awesome Icecream");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:flavor");
        object = VF.createLiteral("Chocolate");
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createIRI("urn:icecream")));

        //vanilla
        subject = VF.createIRI("urn:vanilla");
        predicate = VF.createIRI("urn:brand");
        object = VF.createLiteral("Awesome Icecream");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:flavor");
        object = VF.createLiteral("Vanilla");
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createIRI("urn:icecream")));

        //strawberry
        subject = VF.createIRI("urn:strawberry");
        predicate = VF.createIRI("urn:brand");
        object = VF.createLiteral("Awesome Icecream");
        conn.add(VF.createStatement(subject, predicate, object));
        predicate = VF.createIRI("urn:flavor");
        object = VF.createLiteral("Strawberry");
        conn.add(VF.createStatement(subject, predicate, object));
        conn.add(VF.createStatement(subject, RDF.TYPE, VF.createIRI("urn:icecream")));

        conn.commit();
    }
}