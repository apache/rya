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

import static org.apache.rya.api.resolver.RdfToRyaConversions.convertStatement;
import static org.apache.rya.indexing.GeoIndexingTestUtils.getSet;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.GeoIndexerType;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;

import mil.nga.giat.geowave.datastore.accumulo.minicluster.MiniAccumuloClusterFactory;

/**
 * Tests  higher level functioning of the geoindexer parse WKT, predicate list,
 * prime and anti meridian, delete, search, context, search with Statement Constraints.
 */
public class GeoWaveIndexerTest {

    private static final StatementConstraints EMPTY_CONSTRAINTS = new StatementConstraints();

    private AccumuloRdfConfiguration conf;
    private final GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);

    private static File tempAccumuloDir;
    private static MiniAccumuloClusterImpl accumulo;

    private static final boolean IS_MOCK = true;

    private static final String ACCUMULO_USER = IS_MOCK ? "username" : "root";
    private static final String ACCUMULO_PASSWORD = "password";

    @BeforeClass
    public static void setup() throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException {
        if (!IS_MOCK) {
            tempAccumuloDir = Files.createTempDir();

            accumulo = MiniAccumuloClusterFactory.newAccumuloCluster(
                    new MiniAccumuloConfigImpl(tempAccumuloDir, ACCUMULO_PASSWORD),
                    GeoWaveIndexerTest.class);

            accumulo.start();
        }
    }

    @AfterClass
    public static void cleanup() throws IOException, InterruptedException {
        if (!IS_MOCK) {
            try {
                accumulo.stop();
            } finally {
                FileUtils.deleteDirectory(tempAccumuloDir);
            }
        }
    }

    @Before
    public void before() throws Exception {
        conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix("triplestore_");
        final String tableName = GeoWaveGeoIndexer.getTableName(conf);
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, IS_MOCK);
        conf.set(ConfigUtils.CLOUDBASE_USER, ACCUMULO_USER);
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, ACCUMULO_PASSWORD);
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, IS_MOCK ? "INSTANCE" : accumulo.getInstanceName());
        conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, IS_MOCK ? "localhost" : accumulo.getZooKeepers());
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "U");
        conf.set(OptionalConfigUtils.USE_GEO, "true");
        conf.set(OptionalConfigUtils.GEO_INDEXER_TYPE, GeoIndexerType.GEO_WAVE.toString());

        final TableOperations tops = ConfigUtils.getConnector(conf).tableOperations();
        // get all of the table names with the prefix
        final Set<String> toDel = Sets.newHashSet();
        for (final String t : tops.list()){
            if (t.startsWith(tableName)){
                toDel.add(t);
            }
        }
        for (final String t : toDel) {
            tops.delete(t);
        }
    }

    @Test
    public void testRestrictPredicatesSearch() throws Exception {
        conf.setStrings(ConfigUtils.GEO_PREDICATES_LIST, "pred:1,pred:2");
        try (final GeoWaveGeoIndexer f = new GeoWaveGeoIndexer()) {
            f.setConf(conf);
            f.purge(conf);

            final ValueFactory vf = SimpleValueFactory.getInstance();

            final Point point = gf.createPoint(new Coordinate(10, 10));
            final Value pointValue = vf.createLiteral("Point(10 10)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final IRI invalidPredicate = GeoConstants.GEO_AS_WKT;

            // These should not be stored because they are not in the predicate list
            f.storeStatement(convertStatement(vf.createStatement(vf.createIRI("foo:subj1"), invalidPredicate, pointValue)));
            f.storeStatement(convertStatement(vf.createStatement(vf.createIRI("foo:subj2"), invalidPredicate, pointValue)));

            final IRI pred1 = vf.createIRI("pred:1");
            final IRI pred2 = vf.createIRI("pred:2");

            // These should be stored because they are in the predicate list
            final Statement s3 = vf.createStatement(vf.createIRI("foo:subj3"), pred1, pointValue);
            final Statement s4 = vf.createStatement(vf.createIRI("foo:subj4"), pred2, pointValue);
            f.storeStatement(convertStatement(s3));
            f.storeStatement(convertStatement(s4));

            // This should not be stored because the object is not valid wkt
            f.storeStatement(convertStatement(vf.createStatement(vf.createIRI("foo:subj5"), pred1, vf.createLiteral("soint(10 10)"))));

            // This should not be stored because the object is not a literal
            f.storeStatement(convertStatement(vf.createStatement(vf.createIRI("foo:subj6"), pred1, vf.createIRI("p:Point(10 10)"))));

            f.flush();

            final Set<Statement> actual = getSet(f.queryEquals(point, EMPTY_CONSTRAINTS));
            Assert.assertEquals(2, actual.size());
            Assert.assertTrue(actual.contains(s3));
            Assert.assertTrue(actual.contains(s4));
        }
    }

    @Test
    public void testPrimeMeridianSearch() throws Exception {
        try (final GeoWaveGeoIndexer f = new GeoWaveGeoIndexer()) {
            f.setConf(conf);
            f.purge(conf);

            final ValueFactory vf = SimpleValueFactory.getInstance();
            final Resource subject = vf.createIRI("foo:subj");
            final IRI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createIRI("foo:context");

            final Statement statement = vf.createStatement(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            final double[] ONE = { 1, 1, -1, 1, -1, -1, 1, -1, 1, 1 };
            final double[] TWO = { 2, 2, -2, 2, -2, -2, 2, -2, 2, 2 };
            final double[] THREE = { 3, 3, -3, 3, -3, -3, 3, -3, 3, 3 };

            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(ONE, 2));
            final LinearRing r2 = gf.createLinearRing(new PackedCoordinateSequence.Double(TWO, 2));
            final LinearRing r3 = gf.createLinearRing(new PackedCoordinateSequence.Double(THREE, 2));

            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
            final Polygon p2 = gf.createPolygon(r2, new LinearRing[] {});
            final Polygon p3 = gf.createPolygon(r3, new LinearRing[] {});

            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p1, EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p2, EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p3, EMPTY_CONSTRAINTS)));

            // Test a ring with a hole in it
            final Polygon p3m2 = gf.createPolygon(r3, new LinearRing[] { r2 });
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(p3m2, EMPTY_CONSTRAINTS)));

            // test a ring outside the point
            final double[] OUT = { 3, 3, 1, 3, 1, 1, 3, 1, 3, 3 };
            final LinearRing rOut = gf.createLinearRing(new PackedCoordinateSequence.Double(OUT, 2));
            final Polygon pOut = gf.createPolygon(rOut, new LinearRing[] {});
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(pOut, EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testDcSearch() throws Exception {
        // test a ring around dc
        try (final GeoWaveGeoIndexer f = new GeoWaveGeoIndexer()) {
            f.setConf(conf);
            f.purge(conf);

            final ValueFactory vf = SimpleValueFactory.getInstance();
            final Resource subject = vf.createIRI("foo:subj");
            final IRI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createIRI("foo:context");

            final Statement statement = vf.createStatement(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            final double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p1, EMPTY_CONSTRAINTS)));

            // test a ring outside the point
            final double[] OUT = { -77, 39, -76, 39, -76, 38, -77, 38, -77, 39 };
            final LinearRing rOut = gf.createLinearRing(new PackedCoordinateSequence.Double(OUT, 2));
            final Polygon pOut = gf.createPolygon(rOut, new LinearRing[] {});
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(pOut, EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testDeleteSearch() throws Exception {
        // test a ring around dc
        try (final GeoWaveGeoIndexer f = new GeoWaveGeoIndexer()) {
            f.setConf(conf);
            f.purge(conf);

            final ValueFactory vf = SimpleValueFactory.getInstance();
            final Resource subject = vf.createIRI("foo:subj");
            final IRI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createIRI("foo:context");

            final Statement statement = vf.createStatement(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            f.deleteStatement(convertStatement(statement));

            // test a ring that the point would be inside of if not deleted
            final double[] in = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(in, 2));
            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(p1, EMPTY_CONSTRAINTS)));

            // test a ring that the point would be outside of if not deleted
            final double[] out = { -77, 39, -76, 39, -76, 38, -77, 38, -77, 39 };
            final LinearRing rOut = gf.createLinearRing(new PackedCoordinateSequence.Double(out, 2));
            final Polygon pOut = gf.createPolygon(rOut, new LinearRing[] {});
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(pOut, EMPTY_CONSTRAINTS)));

            // test a ring for the whole world and make sure the point is gone
            final double[] world = { -180, 90, 180, 90, 180, -90, -180, -90, -180, 90 };
            final LinearRing rWorld = gf.createLinearRing(new PackedCoordinateSequence.Double(world, 2));
            final Polygon pWorld = gf.createPolygon(rWorld, new LinearRing[] {});
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(pWorld, EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testDcSearchWithContext() throws Exception {
        // test a ring around dc
        try (final GeoWaveGeoIndexer f = new GeoWaveGeoIndexer()) {
            f.setConf(conf);
            f.purge(conf);

            final ValueFactory vf = SimpleValueFactory.getInstance();
            final Resource subject = vf.createIRI("foo:subj");
            final IRI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createIRI("foo:context");

            final Statement statement = vf.createStatement(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            final double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            // query with correct context
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p1, new StatementConstraints().setContext(context))));

            // query with wrong context
            Assert.assertEquals(Sets.newHashSet(),
                    getSet(f.queryWithin(p1, new StatementConstraints().setContext(vf.createIRI("foo:context2")))));
        }
    }

    @Test
    public void testDcSearchWithSubject() throws Exception {
        // test a ring around dc
        try (final GeoWaveGeoIndexer f = new GeoWaveGeoIndexer()) {
            f.setConf(conf);
            f.purge(conf);

            final ValueFactory vf = SimpleValueFactory.getInstance();
            final Resource subject = vf.createIRI("foo:subj");
            final IRI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createIRI("foo:context");

            final Statement statement = vf.createStatement(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            final double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            // query with correct subject
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p1, new StatementConstraints().setSubject(subject))));

            // query with wrong subject
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(p1, new StatementConstraints().setSubject(vf.createIRI("foo:subj2")))));
        }
    }

    @Test
    public void testDcSearchWithSubjectAndContext() throws Exception {
        // test a ring around dc
        try (final GeoWaveGeoIndexer f = new GeoWaveGeoIndexer()) {
            f.setConf(conf);
            f.purge(conf);

            final ValueFactory vf = SimpleValueFactory.getInstance();
            final Resource subject = vf.createIRI("foo:subj");
            final IRI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createIRI("foo:context");

            final Statement statement = vf.createStatement(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            final double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            // query with correct context subject
            Assert.assertEquals(Sets.newHashSet(statement),
                    getSet(f.queryWithin(p1, new StatementConstraints().setContext(context).setSubject(subject))));

            // query with wrong context
            Assert.assertEquals(Sets.newHashSet(),
                    getSet(f.queryWithin(p1, new StatementConstraints().setContext(vf.createIRI("foo:context2")))));

            // query with wrong subject
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(p1, new StatementConstraints().setSubject(vf.createIRI("foo:subj2")))));
        }
    }

    @Test
    public void testDcSearchWithPredicate() throws Exception {
        // test a ring around dc
        try (final GeoWaveGeoIndexer f = new GeoWaveGeoIndexer()) {
            f.setConf(conf);
            f.purge(conf);

            final ValueFactory vf = SimpleValueFactory.getInstance();
            final Resource subject = vf.createIRI("foo:subj");
            final IRI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createIRI("foo:context");

            final Statement statement = vf.createStatement(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            final double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            // query with correct Predicate
            Assert.assertEquals(Sets.newHashSet(statement),
                    getSet(f.queryWithin(p1, new StatementConstraints().setPredicates(Collections.singleton(predicate)))));

            // query with wrong predicate
            Assert.assertEquals(Sets.newHashSet(),
                    getSet(f.queryWithin(p1, new StatementConstraints().setPredicates(Collections.singleton(vf.createIRI("other:pred"))))));
        }
    }

    // @Test
    public void testAntiMeridianSearch() throws Exception {
        // verify that a search works if the bounding box crosses the anti meridian
        try (final GeoWaveGeoIndexer f = new GeoWaveGeoIndexer()) {
            f.setConf(conf);
            f.purge(conf);

            final ValueFactory vf = SimpleValueFactory.getInstance();
            final Resource context = vf.createIRI("foo:context");

            final Resource subjectEast = vf.createIRI("foo:subj:east");
            final IRI predicateEast = GeoConstants.GEO_AS_WKT;
            final Value objectEast = vf.createLiteral("Point(179 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Statement statementEast = vf.createStatement(subjectEast, predicateEast, objectEast, context);
            f.storeStatement(convertStatement(statementEast));

            final Resource subjectWest = vf.createIRI("foo:subj:west");
            final IRI predicateWest = GeoConstants.GEO_AS_WKT;
            final Value objectWest = vf.createLiteral("Point(-179 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Statement statementWest = vf.createStatement(subjectWest, predicateWest, objectWest, context);
            f.storeStatement(convertStatement(statementWest));

            f.flush();

            final double[] ONE = { 178.1, 1, -178, 1, -178, -1, 178.1, -1, 178.1, 1 };

            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(ONE, 2));

            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            Assert.assertEquals(Sets.newHashSet(statementEast, statementWest), getSet(f.queryWithin(p1, EMPTY_CONSTRAINTS)));
        }
    }
}
