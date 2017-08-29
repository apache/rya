/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.accumulo.geo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.indexing.GeoIndexerType;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.DelegatingFeatureReader;
import org.geotools.data.FeatureReader;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.feature.SchemaException;
import org.geotools.feature.visitor.MaxVisitor;
import org.geotools.feature.visitor.MinVisitor;
import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

import mil.nga.giat.geowave.adapter.vector.plugin.GeoWaveFeatureReader;
import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;

/**
 * Tests the {@link FeatureReader} capabilities within the
 * {@link GeoWaveGeoIndexer).
 */
public class GeoWaveFeatureReaderTest {
    private DataStore dataStore;
    private SimpleFeatureType type;
    private final GeometryFactory factory = new GeometryFactory(new PrecisionModel(PrecisionModel.FIXED));
    private Query query = null;
    private final List<String> fids = new ArrayList<>();
    private final List<String> pids = new ArrayList<>();
    private Date stime, etime;

    private AccumuloRdfConfiguration conf;

    private void setupConf() throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix("triplestore_");
        final String tableName = GeoWaveGeoIndexer.getTableName(conf);
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        conf.set(ConfigUtils.CLOUDBASE_USER, "USERNAME");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "PASS");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "INSTANCE");
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, "localhost");
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "U");
        conf.set(OptionalConfigUtils.USE_GEO, "true");
        conf.set(OptionalConfigUtils.GEO_INDEXER_TYPE, GeoIndexerType.GEO_WAVE.toString());

        final TableOperations tops = ConfigUtils.getConnector(conf).tableOperations();
        // get all of the table names with the prefix
        final Set<String> toDel = Sets.newHashSet();
        for (final String t : tops.list()) {
            if (t.startsWith(tableName)) {
                toDel.add(t);
            }
        }
        for (final String t : toDel) {
            tops.delete(t);
        }
    }

    @Before
    public void setup() throws SchemaException, CQLException, Exception {
        setupConf();
        try (final GeoWaveGeoIndexer indexer = new GeoWaveGeoIndexer()) {
            indexer.setConf(conf);
            dataStore = indexer.getGeoToolsDataStore();
            // Clear old data
            indexer.purge(conf);

            type = DataUtilities.createType(
                    "GeoWaveFeatureReaderTest",
                    "geometry:Geometry:srid=4326,start:Date,end:Date,pop:java.lang.Long,pid:String");

            dataStore.createSchema(type);

            stime = DateUtilities.parseISO("2005-05-15T20:32:56Z");
            etime = DateUtilities.parseISO("2005-05-20T20:32:56Z");

            final Transaction transaction1 = new DefaultTransaction();
            final FeatureWriter<SimpleFeatureType, SimpleFeature> writer = dataStore.getFeatureWriter(
                    type.getTypeName(),
                    transaction1);
            assertFalse(writer.hasNext());
            SimpleFeature newFeature = writer.next();
            newFeature.setAttribute(
                    "pop",
                    Long.valueOf(100));
            newFeature.setAttribute(
                    "pid",
                    "a" + UUID.randomUUID().toString());
            newFeature.setAttribute(
                    "start",
                    stime);
            newFeature.setAttribute(
                    "end",
                    etime);
            newFeature.setAttribute(
                    "geometry",
                    factory.createPoint(new Coordinate(27.25, 41.25)));
            fids.add(newFeature.getID());
            pids.add(newFeature.getAttribute("pid").toString());
            writer.write();
            newFeature = writer.next();
            newFeature.setAttribute(
                    "pop",
                    Long.valueOf(101));
            newFeature.setAttribute(
                    "pid",
                    "b" + UUID.randomUUID().toString());
            newFeature.setAttribute(
                    "start",
                    etime);
            newFeature.setAttribute(
                    "geometry",
                    factory.createPoint(new Coordinate(28.25, 41.25)));
            fids.add(newFeature.getID());
            pids.add(newFeature.getAttribute("pid").toString());
            writer.write();
            writer.close();
            transaction1.commit();
            transaction1.close();

            query = new Query(
                    "GeoWaveFeatureReaderTest",
                    ECQL.toFilter("IN ('" + fids.get(0) + "')"),
                    new String[] {
                        "geometry",
                        "pid"
                    });
        }
    }

    @After
    public void tearDown() {
        dataStore.dispose();
    }

    @Test
    public void testFID() throws IllegalArgumentException, NoSuchElementException, IOException, CQLException {
        final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
            dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        int count = 0;
        while (reader.hasNext()) {
            final SimpleFeature feature = reader.next();
            assertTrue(fids.contains(feature.getID()));
            count++;
        }
        assertTrue(count > 0);
    }

    @Test
    public void testFidFilterQuery() throws IllegalArgumentException, NoSuchElementException, IOException, CQLException {
        final String fidsString = fids.stream().collect(Collectors.joining("','", "'", "'"));
        final Filter filter = ECQL.toFilter("IN (" + fidsString + ")");
        final Query query = new Query(
                "GeoWaveFeatureReaderTest",
                filter,
                new String[] {
                    "geometry",
                    "pid"
                });
        final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
            dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        int count = 0;
        while (reader.hasNext()) {
            final SimpleFeature feature = reader.next();
            assertTrue(fids.contains(feature.getID()));
            count++;
        }
        assertTrue(count == fids.size());
    }

    @Test
    public void testPidFilterQuery() throws IllegalArgumentException, NoSuchElementException, IOException, CQLException {
        // Filter it so that it only queries for everything but the first pid.
        // There's only 2 pids total so it should just return the second one.
        final String pidsString = pids.subList(1, pids.size()).stream().collect(Collectors.joining("','", "'", "'"));
        final Filter filter = ECQL.toFilter("pid IN (" + pidsString + ")");
        final Query query = new Query(
                "GeoWaveFeatureReaderTest",
                filter,
                new String[] {
                    "geometry",
                    "pid"
                });
        final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
            dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        int count = 0;
        while (reader.hasNext()) {
            final SimpleFeature feature = reader.next();
            assertTrue(fids.contains(feature.getID()));
            count++;
        }
        assertTrue(count == pids.size() - 1);
    }


    @Test
    public void testBBOX() throws IllegalArgumentException, NoSuchElementException, IOException {
        final FilterFactoryImpl factory = new FilterFactoryImpl();
        final Query query = new Query(
                "GeoWaveFeatureReaderTest",
                factory.bbox(
                        "",
                        -180,
                        -90,
                        180,
                        90,
                        "EPSG:4326"),
                new String[] {
                    "geometry",
                    "pid"
                });

        final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
            dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        int count = 0;
        while (reader.hasNext()) {
            final SimpleFeature feature = reader.next();
            assertTrue(fids.contains(feature.getID()));
            count++;
        }
        assertTrue(count > 0);
    }

    @Test
    public void testRangeIndex() throws IllegalArgumentException, NoSuchElementException, IOException {
        final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
            dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        int count = 0;
        while (reader.hasNext()) {
            final SimpleFeature feature = reader.next();
            assertTrue(fids.contains(feature.getID()));
            count++;
        }
        assertEquals(1, count);
    }

    @Test
    public void testLike() throws IllegalArgumentException, NoSuchElementException, IOException, CQLException {
        final Query query = new Query(
                "GeoWaveFeatureReaderTest",
                ECQL.toFilter("pid like '" + pids.get(
                        0).substring(
                        0,
                        1) + "%'"),
                new String[] {
                    "geometry",
                    "pid"
                });
        final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
            dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        int count = 0;
        while (reader.hasNext()) {
            final SimpleFeature feature = reader.next();
            assertTrue(fids.contains(feature.getID()));
            count++;
        }
        assertEquals(1, count);
    }

    @Test
    public void testRemoveFeature() throws IllegalArgumentException, NoSuchElementException, IOException, CQLException {
        final Query query = new Query(
                "GeoWaveFeatureReaderTest",
                ECQL.toFilter("pid like '" + pids.get(
                        0).substring(
                        0,
                        1) + "%'"),
                new String[] {
                    "geometry",
                    "pid"
                });
        final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
            dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        int count = 0;
        while (reader.hasNext()) {
            final SimpleFeature feature = reader.next();
            assertTrue(fids.contains(feature.getID()));
            count++;
        }
        assertEquals(1, count);

        // Remove
        final FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
            dataStore.getFeatureWriter(type.getTypeName(), Transaction.AUTO_COMMIT);
        try {
            while (writer.hasNext()) {
                writer.next();
                writer.remove();
            }
        } finally {
            writer.close();
        }

        // Re-query
        final FeatureReader<SimpleFeatureType, SimpleFeature> reader2 =
                dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        int recount = 0;
        while (reader2.hasNext()) {
            reader2.next();
            recount++;
        }
        assertEquals(0, recount);
    }

    @Test
    public void testMax() throws IllegalArgumentException, NoSuchElementException, IOException {
        final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
            dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        final MaxVisitor visitor = new MaxVisitor("start", type);
        unwrapDelegatingFeatureReader(reader).getFeatureCollection().accepts(visitor, null);
        assertTrue(visitor.getMax().equals(etime));
    }

    @Test
    public void testMin() throws IllegalArgumentException, NoSuchElementException, IOException {
        final FeatureReader<SimpleFeatureType, SimpleFeature> reader =
            dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
        final MinVisitor visitor = new MinVisitor("start", type);
        unwrapDelegatingFeatureReader(reader).getFeatureCollection().accepts(visitor, null);
        assertTrue(visitor.getMin().equals(stime));
    }

    private GeoWaveFeatureReader unwrapDelegatingFeatureReader(final FeatureReader<SimpleFeatureType, SimpleFeature> reader ) {
        // GeoTools uses decorator pattern to wrap FeatureReaders
        // we need to get down to the inner GeoWaveFeatureReader
        FeatureReader<SimpleFeatureType, SimpleFeature> currReader = reader;
        while (!(currReader instanceof GeoWaveFeatureReader)) {
            currReader = ((DelegatingFeatureReader<SimpleFeatureType, SimpleFeature>) currReader).getDelegate();
        }
        return (GeoWaveFeatureReader) currReader;
    }
}