/**
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
package org.apache.rya.indexing.mongo;

import static org.apache.rya.api.resolver.RdfToRyaConversions.convertStatement;
import static org.apache.rya.indexing.GeoIndexingTestUtils.getSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Set;

import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.geo.OptionalConfigUtils;
import org.apache.rya.indexing.mongodb.geo.MongoGeoIndexer;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoTestBase;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;

public class MongoGeoIndexerTest extends MongoTestBase {
    private static final StatementConstraints EMPTY_CONSTRAINTS = new StatementConstraints();
    GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);

    @Override
    public void updateConfiguration(final MongoDBRdfConfiguration conf) {
        conf.set(ConfigUtils.GEO_PREDICATES_LIST, "http://www.opengis.net/ont/geosparql#asWKT");
        conf.set(OptionalConfigUtils.USE_GEO, "true");
    }

    @Test
    public void testRestrictPredicatesSearch() throws Exception {
        try (final MongoGeoIndexer f = new MongoGeoIndexer()) {
            conf.setStrings(ConfigUtils.GEO_PREDICATES_LIST, "pred:1,pred:2");
            f.setConf(conf);
            f.init();

            final ValueFactory vf = new ValueFactoryImpl();

            final Point point = gf.createPoint(new Coordinate(10, 10));
            final Value pointValue = vf.createLiteral("Point(10 10)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final URI invalidPredicate = GeoConstants.GEO_AS_WKT;

            // These should not be stored because they are not in the predicate list
            f.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj1"), invalidPredicate, pointValue)));
            f.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj2"), invalidPredicate, pointValue)));

            final URI pred1 = vf.createURI("pred:1");
            final URI pred2 = vf.createURI("pred:2");

            // These should be stored because they are in the predicate list
            final Statement s3 = new StatementImpl(vf.createURI("foo:subj3"), pred1, pointValue);
            final Statement s4 = new StatementImpl(vf.createURI("foo:subj4"), pred2, pointValue);
            f.storeStatement(convertStatement(s3));
            f.storeStatement(convertStatement(s4));

            // This should not be stored because the object is not valid wkt
            f.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj5"), pred1, vf.createLiteral("soint(10 10)"))));

            // This should not be stored because the object is not a literal
            f.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj6"), pred1, vf.createURI("p:Point(10 10)"))));

            f.flush();

            final Set<Statement> actual = getSet(f.queryEquals(point, EMPTY_CONSTRAINTS));
            assertEquals(2, actual.size());
            assertTrue(actual.contains(s3));
            assertTrue(actual.contains(s4));
        }
    }

    @Test
    public void testPrimeMeridianSearch() throws Exception {
        try (final MongoGeoIndexer f = new MongoGeoIndexer()) {
            f.setConf(conf);
            f.init();

            final ValueFactory vf = new ValueFactoryImpl();
            final Resource subject = vf.createURI("foo:subj");
            final URI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createURI("foo:context");

            final Statement statement = new ContextStatementImpl(subject, predicate, object, context);
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

            assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p1, EMPTY_CONSTRAINTS)));
            assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p2, EMPTY_CONSTRAINTS)));
            assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p3, EMPTY_CONSTRAINTS)));

            // Test a ring with a hole in it
            final Polygon p3m2 = gf.createPolygon(r3, new LinearRing[] { r2 });
            assertEquals(Sets.newHashSet(), getSet(f.queryWithin(p3m2, EMPTY_CONSTRAINTS)));

            // test a ring outside the point
            final double[] OUT = { 3, 3, 1, 3, 1, 1, 3, 1, 3, 3 };
            final LinearRing rOut = gf.createLinearRing(new PackedCoordinateSequence.Double(OUT, 2));
            final Polygon pOut = gf.createPolygon(rOut, new LinearRing[] {});
            assertEquals(Sets.newHashSet(), getSet(f.queryWithin(pOut, EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testDcSearch() throws Exception {
        // test a ring around dc
        try (final MongoGeoIndexer f = new MongoGeoIndexer()) {
            f.setConf(conf);
            f.init();

            final ValueFactory vf = new ValueFactoryImpl();
            final Resource subject = vf.createURI("foo:subj");
            final URI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createURI("foo:context");

            final Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            final double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
            assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p1, EMPTY_CONSTRAINTS)));

            // test a ring outside the point
            final double[] OUT = { -77, 39, -76, 39, -76, 38, -77, 38, -77, 39 };
            final LinearRing rOut = gf.createLinearRing(new PackedCoordinateSequence.Double(OUT, 2));
            final Polygon pOut = gf.createPolygon(rOut, new LinearRing[] {});
            assertEquals(Sets.newHashSet(), getSet(f.queryWithin(pOut, EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testDeleteSearch() throws Exception {
        // test a ring around dc
        try (final MongoGeoIndexer f = new MongoGeoIndexer()) {
            f.setConf(conf);
            f.init();

            final ValueFactory vf = new ValueFactoryImpl();
            final Resource subject = vf.createURI("foo:subj");
            final URI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createURI("foo:context");

            final Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            f.deleteStatement(convertStatement(statement));

            // test a ring that the point would be inside of if not deleted
            final double[] in = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(in, 2));
            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
            assertEquals(Sets.newHashSet(), getSet(f.queryWithin(p1, EMPTY_CONSTRAINTS)));

            // test a ring that the point would be outside of if not deleted
            final double[] out = { -77, 39, -76, 39, -76, 38, -77, 38, -77, 39 };
            final LinearRing rOut = gf.createLinearRing(new PackedCoordinateSequence.Double(out, 2));
            final Polygon pOut = gf.createPolygon(rOut, new LinearRing[] {});
            assertEquals(Sets.newHashSet(), getSet(f.queryWithin(pOut, EMPTY_CONSTRAINTS)));

            // test a ring for the whole world and make sure the point is gone
            // Geomesa is a little sensitive around lon 180, so we only go to 179
            final double[] world = { -180, 90, 179, 90, 179, -90, -180, -90, -180, 90 };
            final LinearRing rWorld = gf.createLinearRing(new PackedCoordinateSequence.Double(world, 2));
            final Polygon pWorld = gf.createPolygon(rWorld, new LinearRing[] {});
            assertEquals(Sets.newHashSet(), getSet(f.queryWithin(pWorld, EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testDcSearchWithContext() throws Exception {
        // test a ring around dc
        try (final MongoGeoIndexer f = new MongoGeoIndexer()) {
            f.setConf(conf);
            f.init();

            final ValueFactory vf = new ValueFactoryImpl();
            final Resource subject = vf.createURI("foo:subj");
            final URI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createURI("foo:context");

            final Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            final double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            // query with correct context
            assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p1, new StatementConstraints().setContext(context))));

            // query with wrong context
            assertEquals(Sets.newHashSet(),
                    getSet(f.queryWithin(p1, new StatementConstraints().setContext(vf.createURI("foo:context2")))));
        }
    }

    @Test
    public void testDcSearchWithSubject() throws Exception {
        // test a ring around dc
        try (final MongoGeoIndexer f = new MongoGeoIndexer()) {
            f.setConf(conf);
            f.init();

            final ValueFactory vf = new ValueFactoryImpl();
            final Resource subject = vf.createURI("foo:subj");
            final URI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createURI("foo:context");

            final Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            final double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            // query with correct subject
            assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p1, new StatementConstraints().setSubject(subject))));

            // query with wrong subject
            assertEquals(Sets.newHashSet(), getSet(f.queryWithin(p1, new StatementConstraints().setSubject(vf.createURI("foo:subj2")))));
        }
    }

    @Test
    public void testDcSearchWithSubjectAndContext() throws Exception {
        // test a ring around dc
        try (final MongoGeoIndexer f = new MongoGeoIndexer()) {
            f.setConf(conf);
            f.init();

            final ValueFactory vf = new ValueFactoryImpl();
            final Resource subject = vf.createURI("foo:subj");
            final URI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createURI("foo:context");

            final Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            final double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            // query with correct context subject
            assertEquals(Sets.newHashSet(statement),
                    getSet(f.queryWithin(p1, new StatementConstraints().setContext(context).setSubject(subject))));

            // query with wrong context
            assertEquals(Sets.newHashSet(),
                    getSet(f.queryWithin(p1, new StatementConstraints().setContext(vf.createURI("foo:context2")))));

            // query with wrong subject
            assertEquals(Sets.newHashSet(), getSet(f.queryWithin(p1, new StatementConstraints().setSubject(vf.createURI("foo:subj2")))));
        }
    }

    @Test
    public void testDcSearchWithPredicate() throws Exception {
        // test a ring around dc
        try (final MongoGeoIndexer f = new MongoGeoIndexer()) {
            f.setConf(conf);
            f.init();

            final ValueFactory vf = new ValueFactoryImpl();
            final Resource subject = vf.createURI("foo:subj");
            final URI predicate = GeoConstants.GEO_AS_WKT;
            final Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Resource context = vf.createURI("foo:context");

            final Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            final double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            // query with correct Predicate
            assertEquals(Sets.newHashSet(statement),
                    getSet(f.queryWithin(p1, new StatementConstraints().setPredicates(Collections.singleton(predicate)))));

            // query with wrong predicate
            assertEquals(Sets.newHashSet(),
                    getSet(f.queryWithin(p1, new StatementConstraints().setPredicates(Collections.singleton(vf.createURI("other:pred"))))));
        }
    }

    // @Test
    public void testAntiMeridianSearch() throws Exception {
        // verify that a search works if the bounding box crosses the anti meridian
        try (final MongoGeoIndexer f = new MongoGeoIndexer()) {
            f.setConf(conf);
            f.init();

            final ValueFactory vf = new ValueFactoryImpl();
            final Resource context = vf.createURI("foo:context");

            final Resource subjectEast = vf.createURI("foo:subj:east");
            final URI predicateEast = GeoConstants.GEO_AS_WKT;
            final Value objectEast = vf.createLiteral("Point(179 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Statement statementEast = new ContextStatementImpl(subjectEast, predicateEast, objectEast, context);
            f.storeStatement(convertStatement(statementEast));

            final Resource subjectWest = vf.createURI("foo:subj:west");
            final URI predicateWest = GeoConstants.GEO_AS_WKT;
            final Value objectWest = vf.createLiteral("Point(-179 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
            final Statement statementWest = new ContextStatementImpl(subjectWest, predicateWest, objectWest, context);
            f.storeStatement(convertStatement(statementWest));

            f.flush();

            final double[] ONE = { 178.1, 1, -178, 1, -178, -1, 178.1, -1, 178.1, 1 };

            final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(ONE, 2));

            final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            assertEquals(Sets.newHashSet(statementEast, statementWest), getSet(f.queryWithin(p1, EMPTY_CONSTRAINTS)));
        }
    }
}
