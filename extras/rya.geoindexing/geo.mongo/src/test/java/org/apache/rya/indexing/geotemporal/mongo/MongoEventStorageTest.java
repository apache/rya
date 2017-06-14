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
package org.apache.rya.indexing.geotemporal.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.apache.rya.indexing.geotemporal.storage.EventStorage.EventAlreadyExistsException;
import org.apache.rya.indexing.geotemporal.storage.EventStorage.EventStorageException;
import org.joda.time.DateTime;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * Integration tests the methods of {@link MongoEventStorage}.
 */
public class MongoEventStorageTest extends MongoITBase {

    private static final String RYA_INSTANCE_NAME = "testInstance";
    private static final GeometryFactory GF = new GeometryFactory(new PrecisionModel(), 4326);

    @Test
    public void create_and_get() throws Exception {
        final Geometry geo = GF.createPoint(new Coordinate(10, 10));
        final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.now());

        // An Event that will be stored.
        final Event event = Event.builder()
                .setSubject(new RyaURI("urn:event/001"))
                .setGeometry(geo)
                .setTemporalInstant(instant)
                .build();

        // Create it.
        final EventStorage storage = new MongoEventStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        storage.create(event);

        // Get it.
        final Optional<Event> storedEvent = storage.get(new RyaURI("urn:event/001"));

        // Verify the correct value was returned.
        assertEquals(event, storedEvent.get());
    }

    @Test
    public void can_not_create_with_same_subject() throws Exception {
        final Geometry geo = GF.createPoint(new Coordinate(10, 10));
        final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.now());

        // An Event that will be stored.
        final Event event = Event.builder()
                .setSubject(new RyaURI("urn:event/001"))
                .setGeometry(geo)
                .setTemporalInstant(instant)
                .build();

        // Create it.
        final EventStorage storage = new MongoEventStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        storage.create(event);

        // Try to create it again. This will fail.
        boolean failed = false;
        try {
            storage.create(event);
        } catch(final EventAlreadyExistsException e) {
            failed = true;
        }
        assertTrue(failed);
    }

    @Test
    public void get_noneExisting() throws Exception {
        // Get a Type that hasn't been created.
        final EventStorage storage = new MongoEventStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        final Optional<Event> storedEvent = storage.get(new RyaURI("urn:event/000"));

        // Verify nothing was returned.
        assertFalse(storedEvent.isPresent());
    }

    @Test
    public void delete() throws Exception {
        final Geometry geo = GF.createPoint(new Coordinate(10, 10));
        final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.now());

        // An Event that will be stored.
        final Event event = Event.builder()
                .setSubject(new RyaURI("urn:event/002"))
                .setGeometry(geo)
                .setTemporalInstant(instant)
                .build();

        // Create it.
        final EventStorage storage = new MongoEventStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        storage.create(event);

        // Delete it.
        final boolean deleted = storage.delete( new RyaURI("urn:event/002") );

        // Verify a document was deleted.
        assertTrue( deleted );
    }

    @Test
    public void delete_nonExisting() throws Exception {
        // Delete an Event that has not been created.
        final EventStorage storage = new MongoEventStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        final boolean deleted = storage.delete( new RyaURI("urn:event/003") );

        // Verify no document was deleted.
        assertFalse( deleted );
    }

    @Test
    public void update() throws Exception {
        final EventStorage storage = new MongoEventStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        final Geometry geo = GF.createPoint(new Coordinate(10, 10));
        TemporalInstant instant = new TemporalInstantRfc3339(DateTime.now());

        // An Event that will be stored.
        final Event event = Event.builder()
                .setSubject(new RyaURI("urn:event/004"))
                .setGeometry(geo)
                .setTemporalInstant(instant)
                .build();

        storage.create(event);

        // Show Alice was stored.
        Optional<Event> latest = storage.get(new RyaURI("urn:event/004"));
        assertEquals(event, latest.get());

        instant = new TemporalInstantRfc3339(DateTime.now());
        // Change Alice's eye color to brown.
        final Event updated = Event.builder(event)
                .setTemporalInstant(instant)
                .build();

        storage.update(event, updated);

        // Fetch the Alice object and ensure it has the new value.
        latest = storage.get(new RyaURI("urn:event/004"));

        assertEquals(updated, latest.get());
    }

    @Test(expected = EventStorageException.class)
    public void update_differentSubjects() throws Exception {
        final EventStorage storage = new MongoEventStorage(super.getMongoClient(), RYA_INSTANCE_NAME);
        final Geometry geo = GF.createPoint(new Coordinate(10, 10));
        final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.now());

        // Two objects that do not have the same Subjects.
        final Event old = Event.builder()
                .setSubject(new RyaURI("urn:event/001"))
                .setGeometry(geo)
                .setTemporalInstant(instant)
                .build();

        final Event updated = Event.builder()
                .setSubject(new RyaURI("urn:event/002"))
                .setGeometry(geo)
                .setTemporalInstant(instant)
                .build();

        // The update will fail.
        storage.update(old, updated);
    }
}