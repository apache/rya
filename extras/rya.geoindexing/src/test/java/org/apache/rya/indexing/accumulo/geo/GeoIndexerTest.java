package org.apache.rya.indexing.accumulo.geo;

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

import static org.apache.rya.api.resolver.RdfToRyaConversions.convertStatement;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.junit.Assert;
import org.junit.Before;
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

import info.aduna.iteration.CloseableIteration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.accumulo.ConfigUtils;

/**
 * Tests  higher level functioning of the geoindexer parse WKT, predicate list, 
 * prime and anti meridian, delete, search, context, search with Statement Constraints.
 */
public class GeoIndexerTest {

    private static final StatementConstraints EMPTY_CONSTRAINTS = new StatementConstraints();

    private AccumuloRdfConfiguration conf;
    GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);

    @Before
    public void before() throws Exception {
        conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix("triplestore_");
        String tableName = GeoMesaGeoIndexer.getTableName(conf);
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        conf.set(ConfigUtils.CLOUDBASE_USER, "USERNAME");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "PASS");
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "U");

        TableOperations tops = ConfigUtils.getConnector(conf).tableOperations();
        // get all of the table names with the prefix
        Set<String> toDel = Sets.newHashSet();
        for (String t : tops.list()){
            if (t.startsWith(tableName)){
                toDel.add(t);
            }
        }
        for (String t : toDel) {
            tops.delete(t);
        }
    }

    @Test
    public void testRestrictPredicatesSearch() throws Exception {
        conf.setStrings(ConfigUtils.GEO_PREDICATES_LIST, "pred:1,pred:2");
        try (GeoMesaGeoIndexer f = new GeoMesaGeoIndexer()) {
            f.setConf(conf);

            ValueFactory vf = new ValueFactoryImpl();

            Point point = gf.createPoint(new Coordinate(10, 10));
            Value pointValue = vf.createLiteral("Point(10 10)", GeoConstants.XMLSCHEMA_OGC_WKT);
            URI invalidPredicate = GeoConstants.GEO_AS_WKT;

            // These should not be stored because they are not in the predicate list
            f.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj1"), invalidPredicate, pointValue)));
            f.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj2"), invalidPredicate, pointValue)));

            URI pred1 = vf.createURI("pred:1");
            URI pred2 = vf.createURI("pred:2");

            // These should be stored because they are in the predicate list
            Statement s3 = new StatementImpl(vf.createURI("foo:subj3"), pred1, pointValue);
            Statement s4 = new StatementImpl(vf.createURI("foo:subj4"), pred2, pointValue);
            f.storeStatement(convertStatement(s3));
            f.storeStatement(convertStatement(s4));

            // This should not be stored because the object is not valid wkt
            f.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj5"), pred1, vf.createLiteral("soint(10 10)"))));

            // This should not be stored because the object is not a literal
            f.storeStatement(convertStatement(new StatementImpl(vf.createURI("foo:subj6"), pred1, vf.createURI("p:Point(10 10)"))));

            f.flush();

            Set<Statement> actual = getSet(f.queryEquals(point, EMPTY_CONSTRAINTS));
            Assert.assertEquals(2, actual.size());
            Assert.assertTrue(actual.contains(s3));
            Assert.assertTrue(actual.contains(s4));
        }
    }

    private static <X> Set<X> getSet(CloseableIteration<X, ?> iter) throws Exception {
        Set<X> set = new HashSet<X>();
        while (iter.hasNext()) {
            set.add(iter.next());
        }
        return set;
    }

    @Test
    public void testPrimeMeridianSearch() throws Exception {
        try (GeoMesaGeoIndexer f = new GeoMesaGeoIndexer()) {
            f.setConf(conf);

            ValueFactory vf = new ValueFactoryImpl();
            Resource subject = vf.createURI("foo:subj");
            URI predicate = GeoConstants.GEO_AS_WKT;
            Value object = vf.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
            Resource context = vf.createURI("foo:context");

            Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            double[] ONE = { 1, 1, -1, 1, -1, -1, 1, -1, 1, 1 };
            double[] TWO = { 2, 2, -2, 2, -2, -2, 2, -2, 2, 2 };
            double[] THREE = { 3, 3, -3, 3, -3, -3, 3, -3, 3, 3 };

            LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(ONE, 2));
            LinearRing r2 = gf.createLinearRing(new PackedCoordinateSequence.Double(TWO, 2));
            LinearRing r3 = gf.createLinearRing(new PackedCoordinateSequence.Double(THREE, 2));

            Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
            Polygon p2 = gf.createPolygon(r2, new LinearRing[] {});
            Polygon p3 = gf.createPolygon(r3, new LinearRing[] {});

            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p1, EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p2, EMPTY_CONSTRAINTS)));
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p3, EMPTY_CONSTRAINTS)));

            // Test a ring with a hole in it
            Polygon p3m2 = gf.createPolygon(r3, new LinearRing[] { r2 });
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(p3m2, EMPTY_CONSTRAINTS)));

            // test a ring outside the point
            double[] OUT = { 3, 3, 1, 3, 1, 1, 3, 1, 3, 3 };
            LinearRing rOut = gf.createLinearRing(new PackedCoordinateSequence.Double(OUT, 2));
            Polygon pOut = gf.createPolygon(rOut, new LinearRing[] {});
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(pOut, EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testDcSearch() throws Exception {
        // test a ring around dc
        try (GeoMesaGeoIndexer f = new GeoMesaGeoIndexer()) {
            f.setConf(conf);

            ValueFactory vf = new ValueFactoryImpl();
            Resource subject = vf.createURI("foo:subj");
            URI predicate = GeoConstants.GEO_AS_WKT;
            Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            Resource context = vf.createURI("foo:context");

            Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p1, EMPTY_CONSTRAINTS)));

            // test a ring outside the point
            double[] OUT = { -77, 39, -76, 39, -76, 38, -77, 38, -77, 39 };
            LinearRing rOut = gf.createLinearRing(new PackedCoordinateSequence.Double(OUT, 2));
            Polygon pOut = gf.createPolygon(rOut, new LinearRing[] {});
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(pOut, EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testDeleteSearch() throws Exception {
        // test a ring around dc
        try (GeoMesaGeoIndexer f = new GeoMesaGeoIndexer()) {
            f.setConf(conf);

            ValueFactory vf = new ValueFactoryImpl();
            Resource subject = vf.createURI("foo:subj");
            URI predicate = GeoConstants.GEO_AS_WKT;
            Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            Resource context = vf.createURI("foo:context");

            Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            f.deleteStatement(convertStatement(statement));

            // test a ring that the point would be inside of if not deleted
            double[] in = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(in, 2));
            Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(p1, EMPTY_CONSTRAINTS)));

            // test a ring that the point would be outside of if not deleted
            double[] out = { -77, 39, -76, 39, -76, 38, -77, 38, -77, 39 };
            LinearRing rOut = gf.createLinearRing(new PackedCoordinateSequence.Double(out, 2));
            Polygon pOut = gf.createPolygon(rOut, new LinearRing[] {});
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(pOut, EMPTY_CONSTRAINTS)));

            // test a ring for the whole world and make sure the point is gone
            // Geomesa is a little sensitive around lon 180, so we only go to 179
            double[] world = { -180, 90, 179, 90, 179, -90, -180, -90, -180, 90 };
            LinearRing rWorld = gf.createLinearRing(new PackedCoordinateSequence.Double(world, 2));
            Polygon pWorld = gf.createPolygon(rWorld, new LinearRing[] {});
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(pWorld, EMPTY_CONSTRAINTS)));
        }
    }

    @Test
    public void testDcSearchWithContext() throws Exception {
        // test a ring around dc
        try (GeoMesaGeoIndexer f = new GeoMesaGeoIndexer()) {
            f.setConf(conf);

            ValueFactory vf = new ValueFactoryImpl();
            Resource subject = vf.createURI("foo:subj");
            URI predicate = GeoConstants.GEO_AS_WKT;
            Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            Resource context = vf.createURI("foo:context");

            Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            // query with correct context
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p1, new StatementConstraints().setContext(context))));

            // query with wrong context
            Assert.assertEquals(Sets.newHashSet(),
                    getSet(f.queryWithin(p1, new StatementConstraints().setContext(vf.createURI("foo:context2")))));
        }
    }

    @Test
    public void testDcSearchWithSubject() throws Exception {
        // test a ring around dc
        try (GeoMesaGeoIndexer f = new GeoMesaGeoIndexer()) {
            f.setConf(conf);

            ValueFactory vf = new ValueFactoryImpl();
            Resource subject = vf.createURI("foo:subj");
            URI predicate = GeoConstants.GEO_AS_WKT;
            Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            Resource context = vf.createURI("foo:context");

            Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            // query with correct subject
            Assert.assertEquals(Sets.newHashSet(statement), getSet(f.queryWithin(p1, new StatementConstraints().setSubject(subject))));

            // query with wrong subject
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(p1, new StatementConstraints().setSubject(vf.createURI("foo:subj2")))));
        }
    }

    @Test
    public void testDcSearchWithSubjectAndContext() throws Exception {
        // test a ring around dc
        try (GeoMesaGeoIndexer f = new GeoMesaGeoIndexer()) {
            f.setConf(conf);

            ValueFactory vf = new ValueFactoryImpl();
            Resource subject = vf.createURI("foo:subj");
            URI predicate = GeoConstants.GEO_AS_WKT;
            Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            Resource context = vf.createURI("foo:context");

            Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            // query with correct context subject
            Assert.assertEquals(Sets.newHashSet(statement),
                    getSet(f.queryWithin(p1, new StatementConstraints().setContext(context).setSubject(subject))));

            // query with wrong context
            Assert.assertEquals(Sets.newHashSet(),
                    getSet(f.queryWithin(p1, new StatementConstraints().setContext(vf.createURI("foo:context2")))));

            // query with wrong subject
            Assert.assertEquals(Sets.newHashSet(), getSet(f.queryWithin(p1, new StatementConstraints().setSubject(vf.createURI("foo:subj2")))));
        }
    }

    @Test
    public void testDcSearchWithPredicate() throws Exception {
        // test a ring around dc
        try (GeoMesaGeoIndexer f = new GeoMesaGeoIndexer()) {
            f.setConf(conf);

            ValueFactory vf = new ValueFactoryImpl();
            Resource subject = vf.createURI("foo:subj");
            URI predicate = GeoConstants.GEO_AS_WKT;
            Value object = vf.createLiteral("Point(-77.03524 38.889468)", GeoConstants.XMLSCHEMA_OGC_WKT);
            Resource context = vf.createURI("foo:context");

            Statement statement = new ContextStatementImpl(subject, predicate, object, context);
            f.storeStatement(convertStatement(statement));
            f.flush();

            double[] IN = { -78, 39, -77, 39, -77, 38, -78, 38, -78, 39 };
            LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(IN, 2));
            Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            // query with correct Predicate
            Assert.assertEquals(Sets.newHashSet(statement),
                    getSet(f.queryWithin(p1, new StatementConstraints().setPredicates(Collections.singleton(predicate)))));

            // query with wrong predicate
            Assert.assertEquals(Sets.newHashSet(),
                    getSet(f.queryWithin(p1, new StatementConstraints().setPredicates(Collections.singleton(vf.createURI("other:pred"))))));
        }
    }

    // @Test
    public void testAntiMeridianSearch() throws Exception {
        // verify that a search works if the bounding box crosses the anti meridian
        try (GeoMesaGeoIndexer f = new GeoMesaGeoIndexer()) {
            f.setConf(conf);

            ValueFactory vf = new ValueFactoryImpl();
            Resource context = vf.createURI("foo:context");

            Resource subjectEast = vf.createURI("foo:subj:east");
            URI predicateEast = GeoConstants.GEO_AS_WKT;
            Value objectEast = vf.createLiteral("Point(179 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
            Statement statementEast = new ContextStatementImpl(subjectEast, predicateEast, objectEast, context);
            f.storeStatement(convertStatement(statementEast));

            Resource subjectWest = vf.createURI("foo:subj:west");
            URI predicateWest = GeoConstants.GEO_AS_WKT;
            Value objectWest = vf.createLiteral("Point(-179 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
            Statement statementWest = new ContextStatementImpl(subjectWest, predicateWest, objectWest, context);
            f.storeStatement(convertStatement(statementWest));

            f.flush();

            double[] ONE = { 178.1, 1, -178, 1, -178, -1, 178.1, -1, 178.1, 1 };

            LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(ONE, 2));

            Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});

            Assert.assertEquals(Sets.newHashSet(statementEast, statementWest), getSet(f.queryWithin(p1, EMPTY_CONSTRAINTS)));
        }
    }
}
