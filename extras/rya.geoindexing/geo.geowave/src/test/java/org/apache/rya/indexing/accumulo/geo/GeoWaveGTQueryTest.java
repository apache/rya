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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.query.cql.CQLQuery;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.minicluster.MiniAccumuloClusterFactory;

/**
 * This class is intended to provide a self-contained, easy-to-follow example of
 * a few GeoTools queries against GeoWave. For simplicity, a MiniAccumuloCluster
 * is spun up and a few points from the DC area are ingested (Washington
 * Monument, White House, FedEx Field). Two queries are executed against this
 * data set.
 */
public class GeoWaveGTQueryTest {
    private static File tempAccumuloDir;
    private static MiniAccumuloClusterImpl accumulo;
    private static DataStore dataStore;

    private static final PrimaryIndex INDEX = new SpatialDimensionalityTypeProvider().createPrimaryIndex();

    // Points (to be ingested into GeoWave Data Store)
    private static final Coordinate WASHINGTON_MONUMENT = new Coordinate(-77.0352, 38.8895);
    private static final Coordinate WHITE_HOUSE = new Coordinate(-77.0366, 38.8977);
    private static final Coordinate FEDEX_FIELD = new Coordinate(-76.8644, 38.9078);

    // cities used to construct Geometries for queries
    private static final Coordinate BALTIMORE = new Coordinate(-76.6167, 39.2833);
    private static final Coordinate RICHMOND = new Coordinate(-77.4667, 37.5333);
    private static final Coordinate HARRISONBURG = new Coordinate(-78.8689, 38.4496);

    private static final Map<String, Coordinate> CANNED_DATA = ImmutableMap.of(
        "Washington Monument", WASHINGTON_MONUMENT,
        "White House", WHITE_HOUSE,
        "FedEx Field", FEDEX_FIELD
    );

    private static final FeatureDataAdapter ADAPTER = new FeatureDataAdapter(getPointSimpleFeatureType());

    private static final String ACCUMULO_USER = "root";
    private static final String ACCUMULO_PASSWORD = "password";
    private static final String TABLE_NAMESPACE = "";

    @BeforeClass
    public static void setup() throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException {
        tempAccumuloDir = Files.createTempDir();

        accumulo = MiniAccumuloClusterFactory.newAccumuloCluster(
                new MiniAccumuloConfigImpl(tempAccumuloDir, ACCUMULO_PASSWORD),
                GeoWaveGTQueryTest.class);

        accumulo.start();

        dataStore = new AccumuloDataStore(
                new BasicAccumuloOperations(
                        accumulo.getZooKeepers(),
                        accumulo.getInstanceName(),
                        ACCUMULO_USER,
                        ACCUMULO_PASSWORD,
                        TABLE_NAMESPACE));

        ingestCannedData();
    }

    private static void ingestCannedData() throws IOException {
        final List<SimpleFeature> points = new ArrayList<>();

        System.out.println("Building SimpleFeatures from canned data set...");

        for (final Entry<String, Coordinate> entry : CANNED_DATA.entrySet()) {
            System.out.println("Added point: " + entry.getKey());
            points.add(buildSimpleFeature(entry.getKey(), entry.getValue()));
        }

        System.out.println("Ingesting canned data...");

        try (final IndexWriter<SimpleFeature> indexWriter = dataStore.createWriter(ADAPTER, INDEX)) {
            for (final SimpleFeature sf : points) {
                indexWriter.write(sf);
            }
        }

        System.out.println("Ingest complete.");
    }

    @Test
    public void executeCQLQueryTest() throws IOException, CQLException {
        System.out.println("Executing query, expecting to match two points...");

        final Filter cqlFilter = ECQL.toFilter("BBOX(geometry,-77.6167,38.6833,-76.6,38.9200) and locationName like 'W%'");

        final QueryOptions queryOptions = new QueryOptions(ADAPTER, INDEX);
        final CQLQuery cqlQuery = new CQLQuery(null, cqlFilter, ADAPTER);

        try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(queryOptions, cqlQuery)) {
            int count = 0;
            while (iterator.hasNext()) {
                System.out.println("Query match: " + iterator.next().getID());
                count++;
            }
            System.out.println("executeCQLQueryTest count: " + count);
            // Should match "Washington Monument" and "White House"
            assertEquals(2, count);
        }
    }

    @Test
    public void executeBoundingBoxQueryTest() throws IOException {
        System.out.println("Constructing bounding box for the area contained by [Baltimore, MD and Richmond, VA.");

        final Geometry boundingBox = GeometryUtils.GEOMETRY_FACTORY.toGeometry(new Envelope(
                BALTIMORE,
                RICHMOND));

        System.out.println("Executing query, expecting to match ALL points...");

        final QueryOptions queryOptions = new QueryOptions(ADAPTER, INDEX);
        final SpatialQuery spatialQuery = new SpatialQuery(boundingBox);

        try (final CloseableIterator<SimpleFeature> iterator = dataStore.query(queryOptions, spatialQuery)) {
            int count = 0;
            while (iterator.hasNext()) {
                System.out.println("Query match: " + iterator.next().getID());
                count++;
            }
            System.out.println("executeBoundingBoxQueryTest count: " + count);
            // Should match "FedEx Field", "Washington Monument", and "White House"
            assertEquals(3, count);
        }
    }

    @Test
    public void executePolygonQueryTest() throws IOException {
        System.out.println("Constructing polygon for the area contained by [Baltimore, MD; Richmond, VA; Harrisonburg, VA].");

        final Polygon polygon = GeometryUtils.GEOMETRY_FACTORY.createPolygon(new Coordinate[] {
            BALTIMORE,
            RICHMOND,
            HARRISONBURG,
            BALTIMORE
        });

        System.out.println("Executing query, expecting to match ALL points...");

        final QueryOptions queryOptions = new QueryOptions(ADAPTER, INDEX);
        final SpatialQuery spatialQuery = new SpatialQuery(polygon);

        /*
         * NOTICE: In this query, the adapter is added to the query options. If
         * an index has data from more than one adapter, the data associated
         * with a specific adapter can be selected.
         */
        try (final CloseableIterator<SimpleFeature> closableIterator = dataStore.query(queryOptions, spatialQuery)) {
            int count = 0;
            while (closableIterator.hasNext()) {
                System.out.println("Query match: " + closableIterator.next().getID());
                count++;
            }
            System.out.println("executePolygonQueryTest count: " + count);
            // Should match "FedEx Field", "Washington Monument", and "White House"
            assertEquals(3, count);
        }
    }

    @AfterClass
    public static void cleanup() throws IOException, InterruptedException {
        try {
            accumulo.stop();
        } finally {
            FileUtils.deleteDirectory(tempAccumuloDir);
        }
    }

    private static SimpleFeatureType getPointSimpleFeatureType() {
        final String name = "PointSimpleFeatureType";
        final SimpleFeatureTypeBuilder sftBuilder = new SimpleFeatureTypeBuilder();
        final AttributeTypeBuilder atBuilder = new AttributeTypeBuilder();
        sftBuilder.setName(name);
        sftBuilder.add(atBuilder.binding(String.class).nillable(false)
            .buildDescriptor("locationName"));
        sftBuilder.add(atBuilder.binding(Geometry.class).nillable(false)
            .buildDescriptor("geometry"));

        return sftBuilder.buildFeatureType();
    }

    private static SimpleFeature buildSimpleFeature(final String locationName, final Coordinate coordinate) {
        final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getPointSimpleFeatureType());
        builder.set("locationName", locationName);
        builder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate));

        return builder.buildFeature(locationName);
    }
}