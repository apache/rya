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
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.geotemporal.mongo.MongoGeoTemporalIndexer;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.apache.rya.mongodb.MockMongoFactory;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.mongodb.MongoClient;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Integration tests the methods of {@link MongoGeoTemporalIndexer}.
 */
public class MongoGeoTemporalIndexerIT extends MongoITBase {

    private MongoGeoTemporalIndexer indexer;

    @Before
    public void makeTestIndexer() throws Exception {
        final MongoClient client = MockMongoFactory.newFactory().newMongoClient();
        indexer = new MongoGeoTemporalIndexer();
        conf.setMongoDBName("GEO_TEMPORAL_INDEXER_TEST");
        conf.set(MongoDBRdfConfiguration.MONGO_DB_NAME, "GEO_TEMPORAL_INDEXER_TEST");
        conf.setMongoClient(client);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");
        indexer.setConf(conf);
        indexer.init();
    }

    @Test
    public void ensureEvent() throws Exception {
        final RyaStatement geoStmnt = statement(point(0, 0));
        final RyaStatement timeStmnt = statement(makeInstant(0));

        final EventStorage store = indexer.getEventStorage(conf);

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

        indexer.deleteStatement(geoStmnt);
        evnt = store.get(timeStmnt.getSubject());
        assertTrue(evnt.isPresent());
        expected = Event.builder()
            .setSubject(timeStmnt.getSubject())
            .setTemporalInstant(makeInstant(0))
            .build();
        assertEquals(expected, evnt.get());

        indexer.deleteStatement(timeStmnt);
        evnt = store.get(timeStmnt.getSubject());
        assertTrue(evnt.isPresent());
        expected = Event.builder()
            .setSubject(timeStmnt.getSubject())
            .build();
        assertEquals(expected, evnt.get());
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
}