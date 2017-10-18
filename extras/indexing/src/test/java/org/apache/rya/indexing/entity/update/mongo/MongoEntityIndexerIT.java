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
package org.apache.rya.indexing.entity.update.mongo;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.TypeStorage;
import org.apache.rya.indexing.entity.storage.mongo.MongoEntityStorage;
import org.apache.rya.indexing.entity.storage.mongo.MongoTypeStorage;
import org.apache.rya.mongodb.MongoITBase;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Integration tests the methods of {@link MongoEntityIndexer}.
 */
public class MongoEntityIndexerIT extends MongoITBase {

    private static final Type PERSON_TYPE =
            new Type(new RyaURI("urn:person"),
                    ImmutableSet.<RyaURI>builder()
                    .add(new RyaURI("urn:name"))
                    .add(new RyaURI("urn:age"))
                    .add(new RyaURI("urn:eye"))
                    .build());

    private static final Type EMPLOYEE_TYPE =
            new Type(new RyaURI("urn:employee"),
                    ImmutableSet.<RyaURI>builder()
                    .add(new RyaURI("urn:name"))
                    .add(new RyaURI("urn:hoursPerWeek"))
                    .build());

    @Test
    public void addStatement_setsType() throws Exception {
        try(MongoEntityIndexer indexer = new MongoEntityIndexer()) {
            indexer.setConf(conf);
            indexer.init();
            // Load a type into the TypeStorage.
            final TypeStorage types = new MongoTypeStorage(getMongoClient(), conf.getRyaInstanceName());
            types.create(PERSON_TYPE);

            // Index a RyaStatement that will create an Entity with an explicit type.
            final RyaStatement statement = new RyaStatement(new RyaURI("urn:SSN/111-11-1111"), new RyaURI( RDF.TYPE.toString() ), new RyaType(XMLSchema.ANYURI, "urn:person"));
            indexer.storeStatement(statement);

            // Fetch the Entity from storage and ensure it looks correct.
            final EntityStorage entities = new MongoEntityStorage(getMongoClient(), conf.getRyaInstanceName());
            final Entity entity = entities.get(new RyaURI("urn:SSN/111-11-1111")).get();

            final Entity expected = Entity.builder()
                    .setSubject(new RyaURI("urn:SSN/111-11-1111"))
                    .setExplicitType(new RyaURI("urn:person"))
                    .build();

            assertEquals(expected, entity);
        }
    }

    @Test
    public void addStatement_setsProperty() throws Exception {
        try(MongoEntityIndexer indexer = new MongoEntityIndexer()) {
            indexer.setConf(conf);
            indexer.init();
            // Load the types into the TypeStorage.
            final TypeStorage types = new MongoTypeStorage(getMongoClient(), conf.getRyaInstanceName());
            types.create(PERSON_TYPE);
            types.create(EMPLOYEE_TYPE);

            // Index a RyaStatement that will create an Entity with two implicit types.
            final RyaStatement statement = new RyaStatement(new RyaURI("urn:SSN/111-11-1111"), new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice"));
            indexer.storeStatement(statement);

            // Fetch the Entity from storage and ensure it looks correct.
            final EntityStorage entities = new MongoEntityStorage(getMongoClient(), conf.getRyaInstanceName());
            final Entity entity = entities.get(new RyaURI("urn:SSN/111-11-1111")).get();

            final Entity expected = Entity.builder()
                    .setSubject(new RyaURI("urn:SSN/111-11-1111"))
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")))
                    .setProperty(new RyaURI("urn:employee"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")))
                    .build();

            assertEquals(expected, entity);
        }
    }

    @Test
    public void addStatement_manyUpdates() throws Exception {
        try(MongoEntityIndexer indexer = new MongoEntityIndexer()) {
            indexer.setConf(conf);
            indexer.init();
            // Load the types into the TypeStorage.
            final TypeStorage types = new MongoTypeStorage(getMongoClient(), conf.getRyaInstanceName());
            types.create(PERSON_TYPE);
            types.create(EMPLOYEE_TYPE);

            // Index a bunch of RyaStatements.
            final RyaURI aliceSSN = new RyaURI("urn:SSN/111-11-1111");
            indexer.storeStatement(new RyaStatement(aliceSSN, new RyaURI( RDF.TYPE.toString() ), new RyaType(XMLSchema.ANYURI, "urn:person")));
            indexer.storeStatement(new RyaStatement(aliceSSN, new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")));
            indexer.storeStatement(new RyaStatement(aliceSSN, new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")));
            indexer.storeStatement(new RyaStatement(aliceSSN, new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")));

            // Fetch the Entity from storage and ensure it looks correct.
            final EntityStorage entities = new MongoEntityStorage(getMongoClient(), conf.getRyaInstanceName());
            final Entity entity = entities.get(new RyaURI("urn:SSN/111-11-1111")).get();

            final Entity expected = Entity.builder()
                    .setSubject(aliceSSN)
                    .setExplicitType(new RyaURI("urn:person"))
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")))
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                    .setProperty(new RyaURI("urn:employee"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")))
                    .setVersion( entity.getVersion() )
                    .build();

            assertEquals(expected, entity);
        }
    }

    @Test
    public void addStatements() throws Exception {
        try(MongoEntityIndexer indexer = new MongoEntityIndexer()) {
            indexer.setConf(conf);
            indexer.init();
            // Load the types into the TypeStorage.
            final TypeStorage types = new MongoTypeStorage(getMongoClient(), conf.getRyaInstanceName());
            types.create(PERSON_TYPE);
            types.create(EMPLOYEE_TYPE);

            // Index a bunch of RyaStatements.
            final RyaURI aliceSSN = new RyaURI("urn:SSN/111-11-1111");
            final RyaURI bobSSN = new RyaURI("urn:SSN/222-22-2222");

            indexer.storeStatements(Sets.newHashSet(
                    new RyaStatement(aliceSSN, new RyaURI( RDF.TYPE.toString() ), new RyaType(XMLSchema.ANYURI, "urn:person")),
                    new RyaStatement(aliceSSN, new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")),
                    new RyaStatement(aliceSSN, new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")),
                    new RyaStatement(aliceSSN, new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")),

                    new RyaStatement(bobSSN, new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Bob")),
                    new RyaStatement(bobSSN, new RyaURI("urn:hoursPerWeek"), new RyaType(XMLSchema.INT, "40")),
                    new RyaStatement(bobSSN, new RyaURI( RDF.TYPE.toString() ), new RyaType(XMLSchema.ANYURI, "urn:employee"))));

            // Fetch the Entity from storage and ensure it looks correct.
            final EntityStorage entities = new MongoEntityStorage(getMongoClient(), conf.getRyaInstanceName());

            final Entity alice = entities.get(aliceSSN).get();
            final Entity bob = entities.get(bobSSN).get();
            final Set<Entity> storedEntities = Sets.newHashSet(alice, bob);

            final Entity expectedAlice = Entity.builder()
                    .setSubject(aliceSSN)
                    .setExplicitType(new RyaURI("urn:person"))
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")))
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                    .setProperty(new RyaURI("urn:employee"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")))
                    .setVersion( alice.getVersion() )
                    .build();
            final Entity expectedBob = Entity.builder()
                    .setSubject(bobSSN)
                    .setExplicitType(new RyaURI("urn:employee"))
                    .setProperty(new RyaURI("urn:employee"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Bob")))
                    .setProperty(new RyaURI("urn:employee"), new Property(new RyaURI("urn:hoursPerWeek"), new RyaType(XMLSchema.INT, "40")))
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Bob")))
                    .setVersion( bob.getVersion() )
                    .build();
            final Set<Entity> expected = Sets.newHashSet(expectedAlice, expectedBob);

            assertEquals(expected, storedEntities);
        }
    }

    @Test
    public void deleteStatement_deletesType() throws Exception {
        try(MongoEntityIndexer indexer = new MongoEntityIndexer()) {
            indexer.setConf(conf);
            indexer.init();
            // Load the type into the TypeStorage.
            final TypeStorage types = new MongoTypeStorage(getMongoClient(), conf.getRyaInstanceName());
            types.create(PERSON_TYPE);
            types.create(EMPLOYEE_TYPE);

            // Index a bunch of RyaStatements.
            final RyaURI aliceSSN = new RyaURI("urn:SSN/111-11-1111");

            indexer.storeStatements(Sets.newHashSet(
                    new RyaStatement(aliceSSN, new RyaURI( RDF.TYPE.toString() ), new RyaType(XMLSchema.ANYURI, "urn:person")),
                    new RyaStatement(aliceSSN, new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")),
                    new RyaStatement(aliceSSN, new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")),
                    new RyaStatement(aliceSSN, new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue"))));

            // Remove the explicit type from Alice.
            indexer.deleteStatement(new RyaStatement(aliceSSN, new RyaURI( RDF.TYPE.toString() ), new RyaType(XMLSchema.ANYURI, "urn:person")));

            // Fetch the Entity from storage and ensure it looks correct.
            final EntityStorage entities = new MongoEntityStorage(getMongoClient(), conf.getRyaInstanceName());
            final Entity entity = entities.get(new RyaURI("urn:SSN/111-11-1111")).get();

            final Entity expected = Entity.builder()
                    .setSubject(aliceSSN)
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")))
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                    .setProperty(new RyaURI("urn:employee"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")))
                    .setVersion( entity.getVersion() )
                    .build();

            assertEquals(expected, entity);
        }
    }

    @Test
    public void deleteStatement_deletesProperty() throws Exception {
        try(MongoEntityIndexer indexer = new MongoEntityIndexer()) {
            indexer.setConf(conf);
            indexer.init();
            // Load the type into the TypeStorage.
            final TypeStorage types = new MongoTypeStorage(getMongoClient(), conf.getRyaInstanceName());
            types.create(PERSON_TYPE);
            types.create(EMPLOYEE_TYPE);

            // Index a bunch of RyaStatements.
            final RyaURI aliceSSN = new RyaURI("urn:SSN/111-11-1111");

            indexer.storeStatements(Sets.newHashSet(
                    new RyaStatement(aliceSSN, new RyaURI( RDF.TYPE.toString() ), new RyaType(XMLSchema.ANYURI, "urn:person")),
                    new RyaStatement(aliceSSN, new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")),
                    new RyaStatement(aliceSSN, new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")),
                    new RyaStatement(aliceSSN, new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue"))));

            // Remove the name property from Alice.
            indexer.deleteStatement(new RyaStatement(aliceSSN, new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")));

            // Fetch the Entity from storage and ensure it looks correct.
            final EntityStorage entities = new MongoEntityStorage(getMongoClient(), conf.getRyaInstanceName());
            final Entity entity = entities.get(new RyaURI("urn:SSN/111-11-1111")).get();

            final Entity expected = Entity.builder()
                    .setSubject(aliceSSN)
                    .setExplicitType(new RyaURI("urn:person"))
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                    .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                    .setVersion( entity.getVersion() )
                    .build();

            assertEquals(expected, entity);
        }
    }
}