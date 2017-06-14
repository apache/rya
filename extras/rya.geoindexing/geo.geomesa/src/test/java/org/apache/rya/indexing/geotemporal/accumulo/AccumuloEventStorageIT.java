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
package org.apache.rya.indexing.geotemporal.accumulo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.OptionalConfigUtils;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.apache.rya.indexing.geotemporal.storage.EventStorage.EventAlreadyExistsException;
import org.apache.rya.indexing.geotemporal.storage.EventStorage.EventStorageException;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.sail.SailException;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * Integration tests the methods of {@link AccumuloEventStorage}. This is
 * independent of the Rya client, so need to set that up. No SPO,POS,OSP
 */
public class AccumuloEventStorageIT extends AccumuloITBase {

	private static final String RYA_INSTANCE_NAME = "testInstance";
	private static final GeometryFactory GF = new GeometryFactory(new PrecisionModel(), 4326);
	//private Sail sail;
	private AccumuloRdfConfiguration ryaConf;

	@Before
	public void setup() throws Exception {
		ryaConf = new AccumuloRdfConfiguration();
		ryaConf.setTablePrefix(RYA_INSTANCE_NAME);
		ryaConf.set(ConfigUtils.CLOUDBASE_USER, super.getUsername());
		ryaConf.set(ConfigUtils.CLOUDBASE_PASSWORD, super.getPassword());
		ryaConf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, super.getZookeepers());
		ryaConf.set(ConfigUtils.CLOUDBASE_INSTANCE, super.getInstanceName());
		ryaConf.set(OptionalConfigUtils.USE_GEO, "false");
		ryaConf.set(OptionalConfigUtils.USE_GEOTEMPORAL, "true");
		//this.sail = GeoRyaSailFactory.getInstance(ryaConf);
	}
	@After
	public void shutdown() throws SailException {
		//this.sail.shutDown();
	}
	/**
	 * create an event then get it back by subject. (works!)
	 * @throws Exception
	 */
	@Test
	public void create_and_get() throws Exception {
		// setup();

		final Geometry geo = GF.createPoint(new Coordinate(10, 10));
		final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.now());
		// An Event that will be stored.
		final Event event = Event.builder().setSubject(new RyaURI("urn:event/001")).setGeometry(geo)
				.setTemporalInstant(instant).build();

		// Create it.
		final AccumuloEventStorage storage = new AccumuloEventStorage(); 
		storage.init(ryaConf);
		storage.create(event);

		// Get it.
		final Optional<Event> storedEvent = storage.get(new RyaURI("urn:event/001"));

		// Verify the correct value was returned.
		assertTrue("Should find a value.", storedEvent.isPresent());
		assertEquals("Should find this value.", event, storedEvent.get());
	}
	/**
	 * Verifies error is thrown when duplicate key is attempted to be created.
	 * @throws Exception
	 */
	@Test(expected=EventAlreadyExistsException.class)
	public void can_not_create_with_same_subject() throws Exception {
		final AccumuloEventStorage storage = new AccumuloEventStorage(); 
		storage.init(ryaConf);

		final Geometry geo = GF.createPoint(new Coordinate(10, 10));
		final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.now());

		// An Event that will be stored.
		final Event event = Event.builder().setSubject(new RyaURI("urn:event/001")).setGeometry(geo)
				.setTemporalInstant(instant).build();

		// Create it.
		// final EventStorage storage = new
		storage.create(event);
		// this one will fail.
		storage.create(event);
	}

	@Test
	public void get_noneExisting() throws Exception {
		final AccumuloEventStorage storage = new AccumuloEventStorage(); 
		storage.init(ryaConf);
		
		// Get an event that hasn't been created.
		final Optional<Event> storedEvent = storage.get(new RyaURI("urn:event/000"));

		// Verify nothing was returned.
		assertFalse(storedEvent.isPresent());
	}

	@Test
	public void delete() throws Exception {
		final AccumuloEventStorage storage = new AccumuloEventStorage(); 
		storage.init(ryaConf);

		final Geometry geo = GF.createPoint(new Coordinate(10, 10));
		final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.now());
		// An Event that will be stored.
		final Event event = Event.builder().setSubject(new RyaURI("urn:event/002")).setGeometry(geo)
				.setTemporalInstant(instant).build();

		// Create it.
		storage.create(event);

		// Delete it.
		final boolean deleted = storage.delete(new RyaURI("urn:event/002"));

		// Verify a document was deleted.
		assertTrue(deleted);
		// Get it.
		final Optional<Event> storedEvent = storage.get(new RyaURI("urn:event/002"));

		// Verify the value is gone.
		assertFalse("Should be no value.", storedEvent.isPresent());
	}

	@Test
	public void delete_nonExisting() throws Exception {
		final AccumuloEventStorage storage = new AccumuloEventStorage(); 
		storage.init(ryaConf);

		// Delete an Event that has not been created.
		final boolean deleted = storage.delete(new RyaURI("urn:event/003"));

		// Verify no document was deleted.
		assertFalse(deleted);
	}

	@Test
	public void update() throws Exception {
		final AccumuloEventStorage storage = new AccumuloEventStorage(); 
		storage.init(ryaConf);

		final Geometry geo = GF.createPoint(new Coordinate(10, 10));
		TemporalInstant instant = new TemporalInstantRfc3339(DateTime.now());

		// An Event that will be stored.
		final Event event = Event.builder().setSubject(new RyaURI("urn:event/004")).setGeometry(geo)
				.setTemporalInstant(instant).build();

		storage.create(event);

		// Show Alice was stored.
		Optional<Event> latest = storage.get(new RyaURI("urn:event/004"));
		assertEquals(event, latest.get());

		instant = new TemporalInstantRfc3339(DateTime.now());
		// Change time of the event.
		final Event updated = Event.builder(event).setTemporalInstant(instant).build();

		storage.update(event, updated);

		// Fetch the Alice object and ensure it has the new value.
		latest = storage.get(new RyaURI("urn:event/004"));

		assertEquals(updated, latest.get());
	}

	@Test(expected = EventStorageException.class)
	public void update_differentSubjects() throws Exception {
		final AccumuloEventStorage storage = new AccumuloEventStorage(); 
		storage.init(ryaConf);

		final Geometry geo = GF.createPoint(new Coordinate(10, 10));
		final TemporalInstant instant = new TemporalInstantRfc3339(DateTime.now());

		// Two objects that do not have the same Subjects.
		final Event old = Event.builder().setSubject(new RyaURI("urn:event/001")).setGeometry(geo)
				.setTemporalInstant(instant).build();

		final Event updated = Event.builder().setSubject(new RyaURI("urn:event/002")).setGeometry(geo)
				.setTemporalInstant(instant).build();

		// The update will fail.
		storage.update(old, updated);
	}
}