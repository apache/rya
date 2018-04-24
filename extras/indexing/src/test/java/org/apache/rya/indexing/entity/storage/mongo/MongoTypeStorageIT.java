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

import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.storage.TypeStorage;
import org.apache.rya.indexing.entity.storage.TypeStorage.TypeStorageException;
import org.apache.rya.test.mongo.MongoITBase;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Integration tests the methods of {@link MongoTypeStorage}.
 */
public class MongoTypeStorageIT extends MongoITBase {

    private static final String RYA_INSTANCE_NAME = "testInstance";

    @Test
    public void create_and_get() throws TypeStorageException {
        // A Type that will be stored.
        final Type type = new Type(new RyaIRI("urn:icecream"),
                ImmutableSet.<RyaIRI>builder()
                    .add(new RyaIRI("urn:brand"))
                    .add(new RyaIRI("urn:flavor"))
                    .add(new RyaIRI("urn:cost"))
                    .build());

        // Create it.
        final TypeStorage storage = new MongoTypeStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        storage.create(type);

        // Get it.
        final Optional<Type> storedType = storage.get(new RyaIRI("urn:icecream"));

        // Verify the correct value was returned.
        assertEquals(type, storedType.get());
    }

    @Test
    public void can_not_create_with_same_id() throws TypeStorageException {
        // A Type that will be stored.
        final Type type = new Type(new RyaIRI("urn:icecream"),
                ImmutableSet.<RyaIRI>builder()
                    .add(new RyaIRI("urn:brand"))
                    .add(new RyaIRI("urn:flavor"))
                    .add(new RyaIRI("urn:cost"))
                    .build());

        // Create it.
        final TypeStorage storage = new MongoTypeStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        storage.create(type);

        // Try to create it again. This will fail.
        boolean failed = false;
        try {
            storage.create(type);
        } catch(final TypeStorageException e) {
            failed = true;
        }
        assertTrue(failed);
    }

    @Test
    public void get_nonexisting() throws TypeStorageException {
        // Get a Type that hasn't been created.
        final TypeStorage storage = new MongoTypeStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        final Optional<Type> storedType = storage.get(new RyaIRI("urn:icecream"));

        // Verify nothing was returned.
        assertFalse(storedType.isPresent());
    }

    @Test
    public void delete() throws TypeStorageException {
        // An Type that will be stored.
        final Type type = new Type(new RyaIRI("urn:icecream"),
                ImmutableSet.<RyaIRI>builder()
                    .add(new RyaIRI("urn:brand"))
                    .add(new RyaIRI("urn:flavor"))
                    .add(new RyaIRI("urn:cost"))
                    .build());

        // Create it.
        final TypeStorage storage = new MongoTypeStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        storage.create(type);

        // Delete it.
        final boolean deleted = storage.delete( new RyaIRI("urn:icecream") );

        // Verify a document was deleted.
        assertTrue( deleted );
    }

    @Test
    public void delete_nonexisting() throws TypeStorageException {
        // Delete an Type that has not been created.
        final TypeStorage storage = new MongoTypeStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        final boolean deleted = storage.delete( new RyaIRI("urn:icecream") );

        // Verify no document was deleted.
        assertFalse( deleted );
    }

    @Test
    public void search() throws Exception {
        // Add some Types to the storage.
        final Type cat = new Type(new RyaIRI("urn:cat"),
                ImmutableSet.<RyaIRI>builder()
                    .add(new RyaIRI("urn:numLegs"))
                    .add(new RyaIRI("urn:eye"))
                    .add(new RyaIRI("urn:species"))
                    .build());

        final Type dog = new Type(new RyaIRI("urn:dog"),
                ImmutableSet.<RyaIRI>builder()
                    .add(new RyaIRI("urn:numLegs"))
                    .add(new RyaIRI("urn:eye"))
                    .add(new RyaIRI("urn:species"))
                    .build());

        final Type icecream = new Type(new RyaIRI("urn:icecream"),
                ImmutableSet.<RyaIRI>builder()
                    .add(new RyaIRI("urn:brand"))
                    .add(new RyaIRI("urn:flavor"))
                    .add(new RyaIRI("urn:cost"))
                    .build());

        final TypeStorage storage = new MongoTypeStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        storage.create(cat);
        storage.create(dog);
        storage.create(icecream);

        // Search for all Types that have the 'urn:eye' property.
        final ConvertingCursor<Type> typeIt = storage.search(new RyaIRI("urn:eye"));

        final Set<Type> types = new HashSet<>();
        while(typeIt.hasNext()) {
            types.add( typeIt.next() );
        }

        // Verify the correct types were returned.
        final Set<Type> expected = Sets.newHashSet(cat, dog);
        assertEquals(expected, types);
    }
}