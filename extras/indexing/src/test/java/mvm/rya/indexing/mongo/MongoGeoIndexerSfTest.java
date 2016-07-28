package mvm.rya.indexing.mongo;
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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;

import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;
import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.indexing.StatementConstraints;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.accumulo.geo.GeoConstants;
import mvm.rya.indexing.mongodb.geo.MongoGeoIndexer;
import mvm.rya.mongodb.MongoDBRdfConfiguration;

/**
 * Tests all of the "simple functions" of the geoindexer.
 */
public class MongoGeoIndexerSfTest {
    private MongoDBRdfConfiguration conf;
    private static GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);
    private static MongoGeoIndexer g;

    private static final StatementConstraints EMPTY_CONSTRAINTS = new StatementConstraints();

    // Here is the landscape:
    /**
     * <pre>
     * 	 +---+---+---+---+---+---+---+
     * 	 |        F          |       |
     * 	 +  A    +           +   C   +
     * 	 |                   |       |
     * 	 +---+---+    E      +---+---+
     * 	 |       |   /       |
     * 	 +   B   +  /+---+---+
     * 	 |       | / |       |
     * 	 +---+---+/--+---+---+
     * 	         /   |     D |
     * 	        /    +---+---+
     * </pre>
     **/

    private static final Polygon A = poly(bbox(0, 1, 4, 5));
    private static final Polygon B = poly(bbox(0, 1, 2, 3));
    private static final Polygon C = poly(bbox(4, 3, 6, 5));
    private static final Polygon D = poly(bbox(3, 0, 5, 2));

    private static final Point F = point(2, 4);

    private static final LineString E = line(2, 0, 3, 3);

    private static final Map<Geometry, String> names = Maps.newHashMap();
    static {
        names.put(A, "A");
        names.put(B, "B");
        names.put(C, "C");
        names.put(D, "D");
        names.put(E, "E");
        names.put(F, "F");
    }

    @Before
    public void before() throws Exception {
        System.out.println(UUID.randomUUID().toString());
        conf = new MongoDBRdfConfiguration();
        conf.set(ConfigUtils.USE_MONGO, "true");
        conf.set(MongoDBRdfConfiguration.USE_TEST_MONGO, "true");
        conf.set(MongoDBRdfConfiguration.MONGO_DB_NAME, "test");
        conf.set(MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX, "rya_");
        conf.set(ConfigUtils.GEO_PREDICATES_LIST, "http://www.opengis.net/ont/geosparql#asWKT");
        conf.set(ConfigUtils.USE_GEO, "true");
        conf.setTablePrefix("rya_");

        final MongodForTestsFactory testsFactory = MongodForTestsFactory.with(Version.Main.PRODUCTION);
        final MongoClient mongoClient = testsFactory.newMongo();
        g = new MongoGeoIndexer(mongoClient);
        g.setConf(conf);
        g.storeStatement(statement(A));
        g.storeStatement(statement(B));
        g.storeStatement(statement(C));
        g.storeStatement(statement(D));
        g.storeStatement(statement(F));
        g.storeStatement(statement(E));
    }

    private static RyaStatement statement(final Geometry geo) {
        final ValueFactory vf = new ValueFactoryImpl();
        final Resource subject = vf.createURI("uri:" + names.get(geo));
        final URI predicate = GeoConstants.GEO_AS_WKT;
        final Value object = vf.createLiteral(geo.toString(), GeoConstants.XMLSCHEMA_OGC_WKT);
        return RdfToRyaConversions.convertStatement(new StatementImpl(subject, predicate, object));

    }

    private static Point point(final double x, final double y) {
        return gf.createPoint(new Coordinate(x, y));
    }

    private static LineString line(final double x1, final double y1, final double x2, final double y2) {
        return new LineString(new PackedCoordinateSequence.Double(new double[] { x1, y1, x2, y2 }, 2), gf);
    }

    private static Polygon poly(final double[] arr) {
        final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(arr, 2));
        final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
        return p1;
    }

    private static double[] bbox(final double x1, final double y1, final double x2, final double y2) {
        return new double[] { x1, y1, x1, y2, x2, y2, x2, y1, x1, y1 };
    }

    public void compare(final CloseableIteration<Statement, ?> actual, final Geometry... expected) throws Exception {
        final Set<Statement> expectedSet = Sets.newHashSet();
        for (final Geometry geo : expected) {
            expectedSet.add(RyaToRdfConversions.convertStatement(statement(geo)));
        }

        Assert.assertEquals(expectedSet, getSet(actual));
    }

    private static <X> Set<X> getSet(final CloseableIteration<X, ?> iter) throws Exception {
        final Set<X> set = new HashSet<X>();
        while (iter.hasNext()) {
            set.add(iter.next());
        }
        return set;
    }

    private static Geometry[] EMPTY_RESULTS = {};

    @Test
    public void testEquals() throws Exception {
        // point
        compare(g.queryEquals(F, EMPTY_CONSTRAINTS), F);
        compare(g.queryEquals(point(2, 2), EMPTY_CONSTRAINTS), EMPTY_RESULTS);

        // line
        compare(g.queryEquals(E, EMPTY_CONSTRAINTS), E);
        compare(g.queryEquals(line(2, 2, 3, 3), EMPTY_CONSTRAINTS), EMPTY_RESULTS);

        // poly
        compare(g.queryEquals(A, EMPTY_CONSTRAINTS), A);
        compare(g.queryEquals(poly(bbox(1, 1, 4, 5)), EMPTY_CONSTRAINTS), EMPTY_RESULTS);

    }

//    @Test
//    public void testDisjoint() throws Exception {
//        // point
//        compare(g.queryDisjoint(F, EMPTY_CONSTRAINTS), B, C, D, E);
//
//        // line
//        compare(g.queryDisjoint(E, EMPTY_CONSTRAINTS), B, C, D, F);
//
//        // poly
//        compare(g.queryDisjoint(A, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
//        compare(g.queryDisjoint(B, EMPTY_CONSTRAINTS), C, D, F, E);
//    }

    @Test
    public void testIntersectsPoint() throws Exception {
        // This seems like a bug
        // compare(g.queryIntersects(F, EMPTY_CONSTRAINTS), A, F);
        // compare(g.queryIntersects(F, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
    }

    @Test
    public void testIntersectsLine() throws Exception {
        // This seems like a bug
        // compare(g.queryIntersects(E, EMPTY_CONSTRAINTS), A, E);
        // compare(g.queryIntersects(E, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
    }

//    @Test
//    public void testIntersectsPoly() throws Exception {
//        compare(g.queryIntersects(A, EMPTY_CONSTRAINTS), A, B, C, D, F, E);
//    }

//    @Test
//    public void testTouchesPoint() throws Exception {
//        compare(g.queryTouches(F, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
//    }
//
//    @Test
//    public void testTouchesLine() throws Exception {
//        compare(g.queryTouches(E, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
//    }

//    @Test
//    public void testTouchesPoly() throws Exception {
//        compare(g.queryTouches(A, EMPTY_CONSTRAINTS), C);
//    }

//    @Test
//    public void testCrossesPoint() throws Exception {
//        compare(g.queryCrosses(F, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
//    }

    @Test
    public void testCrossesLine() throws Exception {
        // compare(g.queryCrosses(E, EMPTY_CONSTRAINTS), A);
    }

//    @Test
//    public void testCrossesPoly() throws Exception {
//        compare(g.queryCrosses(A, EMPTY_CONSTRAINTS), E);
//    }

//    @Test
//    public void testWithin() throws Exception {
//        // point
//  //      compare(g.queryWithin(F, EMPTY_CONSTRAINTS), F);
//
//        // line
////        compare(g.queryWithin(E, EMPTY_CONSTRAINTS), E);
//
//        // poly
//        compare(g.queryWithin(A, EMPTY_CONSTRAINTS), A, B, F);
//    }

//    @Test
//    public void testContainsPoint() throws Exception {
//        compare(g.queryContains(F, EMPTY_CONSTRAINTS), A, F);
//    }

    @Test
    public void testContainsLine() throws Exception {
        // compare(g.queryContains(E, EMPTY_CONSTRAINTS), E);
    }

//    @Test
//    public void testContainsPoly() throws Exception {
//        compare(g.queryContains(A, EMPTY_CONSTRAINTS), A);
//        compare(g.queryContains(B, EMPTY_CONSTRAINTS), A, B);
//    }

    @Test
    public void testOverlapsPoint() throws Exception {
        // compare(g.queryOverlaps(F, EMPTY_CONSTRAINTS), F);
        // You cannot have overlapping points
        // compare(g.queryOverlaps(F, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
    }

    @Test
    public void testOverlapsLine() throws Exception {
        // compare(g.queryOverlaps(E, EMPTY_CONSTRAINTS), A, E);
        // You cannot have overlapping lines
        // compare(g.queryOverlaps(E, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
    }

//    @Test
//    public void testOverlapsPoly() throws Exception {
//        compare(g.queryOverlaps(A, EMPTY_CONSTRAINTS), D);
//    }

}
