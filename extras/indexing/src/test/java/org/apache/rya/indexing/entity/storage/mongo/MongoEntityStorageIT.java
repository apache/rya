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
package org.apache.rya.indexing.entity.storage.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.model.TypedEntity;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.EntityStorage.EntityAlreadyExistsException;
import org.apache.rya.indexing.entity.storage.EntityStorage.EntityStorageException;
import org.apache.rya.indexing.entity.storage.EntityStorage.StaleUpdateException;
import org.junit.Test;
import org.openrdf.model.vocabulary.XMLSchema;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Integration tests the methods of {@link MongoEntityStorage}.
 */
public class MongoEntityStorageIT extends MongoITBase {

    private static final String RYA_INSTANCE_NAME = "testInstance";

    @Test
    public void create_and_get() throws Exception {
        // An Entity that will be stored.
        final Entity entity = Entity.builder()
                .setSubject(new RyaURI("urn:GTIN-14/00012345600012"))
                .setExplicitType(new RyaURI("urn:icecream"))
                .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:brand"), new RyaType(XMLSchema.STRING, "Awesome Icecream")))
                .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:flavor"), new RyaType(XMLSchema.STRING, "Chocolate")))
                .build();

        // Create it.
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        storage.create(entity);

        // Get it.
        final Optional<Entity> storedEntity = storage.get(new RyaURI("urn:GTIN-14/00012345600012"));

        // Verify the correct value was returned.
        assertEquals(entity, storedEntity.get());
    }

    @Test
    public void can_not_create_with_same_subject() throws Exception {
        // A Type that will be stored.
        final Entity entity = Entity.builder()
                .setSubject(new RyaURI("urn:GTIN-14/00012345600012"))
                .setExplicitType(new RyaURI("urn:icecream"))
                .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:brand"), new RyaType(XMLSchema.STRING, "Awesome Icecream")))
                .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:flavor"), new RyaType(XMLSchema.STRING, "Chocolate")))
                .build();

        // Create it.
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        storage.create(entity);

        // Try to create it again. This will fail.
        boolean failed = false;
        try {
            storage.create(entity);
        } catch(final EntityAlreadyExistsException e) {
            failed = true;
        }
        assertTrue(failed);
    }

    @Test
    public void get_noneExisting() throws Exception {
        // Get a Type that hasn't been created.
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        final Optional<Entity> storedEntity = storage.get(new RyaURI("urn:GTIN-14/00012345600012"));

        // Verify nothing was returned.
        assertFalse(storedEntity.isPresent());
    }

    @Test
    public void delete() throws Exception {
        // An Entity that will be stored.
        final Entity entity = Entity.builder()
                .setSubject(new RyaURI("urn:GTIN-14/00012345600012"))
                .setExplicitType(new RyaURI("urn:icecream"))
                .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:brand"), new RyaType(XMLSchema.STRING, "Awesome Icecream")))
                .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:flavor"), new RyaType(XMLSchema.STRING, "Chocolate")))
                .build();

        // Create it.
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        storage.create(entity);

        // Delete it.
        final boolean deleted = storage.delete( new RyaURI("urn:GTIN-14/00012345600012") );

        // Verify a document was deleted.
        assertTrue( deleted );
    }

    @Test
    public void delete_nonExisting() throws Exception {
        // Delete an Entity that has not been created.
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        final boolean deleted = storage.delete( new RyaURI("urn:GTIN-14/00012345600012") );

        // Verify no document was deleted.
        assertFalse( deleted );
    }

    @Test
    public void search_byDataType() throws Exception {
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), RYA_INSTANCE_NAME);

        // The Type we will search by.
        final Type icecreamType = new Type(new RyaURI("urn:icecream"),
                ImmutableSet.<RyaURI>builder()
                    .add(new RyaURI("urn:brand"))
                    .add(new RyaURI("urn:flavor"))
                    .add(new RyaURI("urn:cost"))
                    .build());

        // Some Person typed entities.
        final Entity alice = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/111-11-1111") )
                .setExplicitType(new RyaURI("urn:person"))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                .build();

        final Entity bob = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/222-22-2222") )
                .setExplicitType(new RyaURI("urn:person"))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Bob")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "57")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                .build();

        // Some Icecream typed objects.
        final Entity chocolateIcecream = Entity.builder()
                .setSubject(new RyaURI("urn:GTIN-14/00012345600012"))
                .setExplicitType(new RyaURI("urn:icecream"))
                .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:brand"), new RyaType(XMLSchema.STRING, "Awesome Icecream")))
                .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:flavor"), new RyaType(XMLSchema.STRING, "Chocolate")))
                .build();

        final Entity vanillaIcecream = Entity.builder()
                .setSubject( new RyaURI("urn:GTIN-14/22356325213432") )
                .setExplicitType(new RyaURI("urn:icecream"))
                .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:brand"), new RyaType(XMLSchema.STRING, "Awesome Icecream")))
                .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:flavor"), new RyaType(XMLSchema.STRING, "Vanilla")))
                .build();


        final Entity strawberryIcecream = Entity.builder()
                .setSubject( new RyaURI("urn:GTIN-14/77544325436721") )
                .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:brand"), new RyaType(XMLSchema.STRING, "Awesome Icecream")))
                .setProperty(new RyaURI("urn:icecream"), new Property(new RyaURI("urn:flavor"), new RyaType(XMLSchema.STRING, "Strawberry")))
                .build();

        // Create the objects in the storage.
        storage.create(alice);
        storage.create(bob);
        storage.create(chocolateIcecream);
        storage.create(vanillaIcecream);
        storage.create(strawberryIcecream);

        // Search for all icecreams.
        final Set<TypedEntity> objects = new HashSet<>();
        try(final ConvertingCursor<TypedEntity> it = storage.search(Optional.empty(), icecreamType, new HashSet<>())) {
            while(it.hasNext()) {
                objects.add(it.next());
            }
        }

        // Verify the expected results were returned.
        final Set<TypedEntity> expected = Sets.newHashSet(
                chocolateIcecream.makeTypedEntity(new RyaURI("urn:icecream")).get(),
                vanillaIcecream.makeTypedEntity(new RyaURI("urn:icecream")).get());
        assertEquals(expected, objects);
    }

    @Test
    public void search_byFields() throws Exception {
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), RYA_INSTANCE_NAME);

        // A Type that defines a Person.
        final Type personType = new Type(new RyaURI("urn:person"),
                ImmutableSet.<RyaURI>builder()
                    .add(new RyaURI("urn:name"))
                    .add(new RyaURI("urn:age"))
                    .add(new RyaURI("urn:eye"))
                    .build());

        // Some Person typed objects.
        final Entity alice = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/111-11-1111") )
                .setExplicitType(new RyaURI("urn:person"))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                .build();

        final Entity bob = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/222-22-2222") )
                .setExplicitType(new RyaURI("urn:person"))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Bob")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "57")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                .build();


        final Entity charlie = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/333-33-3333") )
                .setExplicitType( new RyaURI("urn:person") )
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Charlie")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                .build();

        final Entity david = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/444-44-4444") )
                .setExplicitType( new RyaURI("urn:person") )
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "David")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "brown")))
                .build();

        final Entity eve = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/555-55-5555") )
                .setExplicitType( new RyaURI("urn:person") )
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Eve")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                .build();

        final Entity frank = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/666-66-6666") )
                .setExplicitType( new RyaURI("urn:person") )
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Frank")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                .setProperty(new RyaURI("urn:someOtherType"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                .build();

        final Entity george = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/777-77-7777") )
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "George")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                .build();

        // Create the objects in the storage.
        storage.create(alice);
        storage.create(bob);
        storage.create(charlie);
        storage.create(david);
        storage.create(eve);
        storage.create(frank);
        storage.create(george);

        // Search for all people who are 30 and have blue eyes.
        final Set<TypedEntity> objects = new HashSet<>();

        final Set<Property> searchValues = Sets.newHashSet(
                new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")),
                new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")));

        try(final ConvertingCursor<TypedEntity> it = storage.search(Optional.empty(), personType, searchValues)) {
            while(it.hasNext()) {
                objects.add(it.next());
            }
        }

        // Verify the expected results were returned.
        assertEquals(2, objects.size());
        assertTrue(objects.contains(alice.makeTypedEntity(new RyaURI("urn:person")).get()));
        assertTrue(objects.contains(charlie.makeTypedEntity(new RyaURI("urn:person")).get()));
    }

    @Test
    public void update() throws Exception {
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), RYA_INSTANCE_NAME);

        // Store Alice in the repository.
        final Entity alice = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/111-11-1111") )
                .setExplicitType(new RyaURI("urn:person"))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                .build();

        storage.create(alice);

        // Show Alice was stored.
        Optional<Entity> latest = storage.get(new RyaURI("urn:SSN/111-11-1111"));
        assertEquals(alice, latest.get());

        // Change Alice's eye color to brown.
        final Entity updated = Entity.builder(alice)
                .setVersion(latest.get().getVersion() + 1)
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "brown")))
                .build();

        storage.update(alice, updated);

        // Fetch the Alice object and ensure it has the new value.
        latest = storage.get(new RyaURI("urn:SSN/111-11-1111"));

        assertEquals(updated, latest.get());
    }

    @Test(expected = StaleUpdateException.class)
    public void update_stale() throws Exception {
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), RYA_INSTANCE_NAME);

        // Store Alice in the repository.
        final Entity alice = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/111-11-1111") )
                .setExplicitType(new RyaURI("urn:person"))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:name"), new RyaType(XMLSchema.STRING, "Alice")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "blue")))
                .build();

        storage.create(alice);

        // Show Alice was stored.
        final Optional<Entity> latest = storage.get(new RyaURI("urn:SSN/111-11-1111"));
        assertEquals(alice, latest.get());

        // Create the wrong old state and try to change Alice's eye color to brown.
        final Entity wrongOld = Entity.builder(alice)
                .setVersion(500)
                .build();

        final Entity updated = Entity.builder(alice)
                .setVersion(501)
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType(XMLSchema.STRING, "brown")))
                .build();

        storage.update(wrongOld, updated);
    }

    @Test(expected = EntityStorageException.class)
    public void update_differentSubjects() throws Exception {
        // Two objects that do not have the same Subjects.
        final Entity old = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/111-11-1111") )
                .setExplicitType( new RyaURI("urn:person") )
                .build();

        final Entity updated = Entity.builder()
                .setSubject( new RyaURI("urn:SSN/222-22-2222") )
                .setExplicitType( new RyaURI("urn:person") )
                .build();

        // The update will fail.
        final EntityStorage storage = new MongoEntityStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        storage.update(old, updated);
    }
}