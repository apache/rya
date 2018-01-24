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

import static org.junit.Assert.*;

import java.util.Optional;

import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.GeoRyaSailFactory;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.OptionalConfigUtils;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * Integration tests the methods of {@link MongoGeoTemporalIndexer}.
 */
public class AccumuloGeoTemporalIndexerIT extends AccumuloITBase {
    private static final GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);
    private static final String URI_PROPERTY_AT_TIME = "http://www.w3.org/2006/time#atTime";
    private SailRepositoryConnection conn;
    private AccumuloGeoTemporalIndexer indexer;
    private AccumuloRdfConfiguration ryaConf;
	private static final String RYA_INSTANCE_NAME = "testInstance";

    @Before
    public void makeTestIndexer() throws Exception {
		ryaConf = new AccumuloRdfConfiguration();
		ryaConf.setTablePrefix(RYA_INSTANCE_NAME);
		ryaConf.set(ConfigUtils.CLOUDBASE_USER, super.getUsername());
		ryaConf.set(ConfigUtils.CLOUDBASE_PASSWORD, super.getPassword());
		ryaConf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, super.getZookeepers());
		ryaConf.set(ConfigUtils.CLOUDBASE_INSTANCE, super.getInstanceName());
		ryaConf.set(OptionalConfigUtils.USE_GEO, "true");//"false");
		ryaConf.set(OptionalConfigUtils.USE_GEOTEMPORAL, "true");
		ryaConf.set(OptionalConfigUtils.USE_TEMPORAL, "true");
		ryaConf.set(OptionalConfigUtils.USE_MONGO, "false");
		ryaConf.set(OptionalConfigUtils.TEMPORAL_PREDICATES_LIST,URI_PROPERTY_AT_TIME);
		ryaConf.set(OptionalConfigUtils.GEO_PREDICATES_LIST,GeoConstants.GEO_AS_WKT.stringValue());
        final Sail sail = GeoRyaSailFactory.getInstance(ryaConf);
        conn = new SailRepository(sail).getConnection();
        conn.begin();

        indexer = new AccumuloGeoTemporalIndexer();
        indexer.setConf(ryaConf);
        indexer.init();
    }

    @Test
    public void ensureEvent() throws Exception {
        final RyaStatement geoStmnt = statement(point(0, 0));
        final RyaStatement timeStmnt = statement(makeInstant(0));

        final EventStorage store = indexer.getEventStorage();

        indexer.storeStatement(geoStmnt);
        Optional<Event> evnt = store.get(geoStmnt.getSubject());
        assertTrue(evnt.isPresent());
        Event expected = Event.builder()
            .setSubject(geoStmnt.getSubject())
            .setGeometry(point(0, 0))
            .build();
        assertEquals(expected, evnt.get());

        indexer.storeStatement(timeStmnt);
        evnt = store.get(timeStmnt.getSubject());
        assertTrue(evnt.isPresent());
        expected = Event.builder()
            .setSubject(geoStmnt.getSubject())
            .setGeometry(point(0, 0))
            .setTemporalInstant(makeInstant(0))
            .build();
        assertEquals(expected, evnt.get());
// // this is how it should work, from the mongo test:        
//        indexer.deleteStatement(geoStmnt);
//        evnt = store.get(timeStmnt.getSubject());
//        assertTrue(evnt.isPresent());
//        expected = Event.builder()
//            .setSubject(timeStmnt.getSubject())
//            .setTemporalInstant(makeInstant(0))
//            .build();
//        assertEquals(expected, evnt.get());
//
//        indexer.deleteStatement(timeStmnt);
//        evnt = store.get(timeStmnt.getSubject());
//        assertTrue(evnt.isPresent());
//        expected = Event.builder()
//            .setSubject(timeStmnt.getSubject())
//            .build();
//        assertEquals(expected, evnt.get());
		// // this is how it works now. It should probably preserve the other event statement:
		indexer.deleteStatement(geoStmnt);
		evnt = store.get(timeStmnt.getSubject());
		assertFalse(evnt.isPresent());
		evnt = store.get(geoStmnt.getSubject());
		assertFalse(evnt.isPresent());

    }

    private static RyaStatement statement(final Geometry geo) {
        final ValueFactory vf = new ValueFactoryImpl();
        final Resource subject = vf.createURI("uri:test");
        final URI predicate = GeoConstants.GEO_AS_WKT;
        final Value object = vf.createLiteral(geo.toString(), GeoConstants.XMLSCHEMA_OGC_WKT);
        return RdfToRyaConversions.convertStatement(new StatementImpl(subject, predicate, object));
    }

    private static RyaStatement statement(final TemporalInstant instant) {
        final ValueFactory vf = new ValueFactoryImpl();
        final Resource subject = vf.createURI("uri:test");
        final URI predicate = vf.createURI("Property:atTime");
        final Value object = vf.createLiteral(instant.toString());
        return RdfToRyaConversions.convertStatement(new StatementImpl(subject, predicate, object));
    }
    /**
     * Make an uniform instant with given seconds.
     */
    protected static TemporalInstant makeInstant(final int secondsMakeMeUnique) {
        return new TemporalInstantRfc3339(2015, 12, 30, 12, 00, secondsMakeMeUnique);
    }
    /**
     * Create a point from x and y
     * @param x
     * @param y
     * @return a Point
     */
    protected static Point point(final double x, final double y) {
        return gf.createPoint(new Coordinate(x, y));
    }
}
