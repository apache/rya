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

import static org.apache.rya.indexing.GeoIndexingTestUtils.getSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.commons.io.FileUtils;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.GeoIndexerType;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.geotools.geometry.jts.Geometries;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.gml2.GMLWriter;

import mil.nga.giat.geowave.datastore.accumulo.minicluster.MiniAccumuloClusterFactory;

/**
 * Tests all of the "simple functions" of the geoindexer specific to GML.
 * Parameterized so that each test is run for WKT and for GML.
 */
@RunWith(value = Parameterized.class)
public class GeoWaveIndexerSfTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private static AccumuloRdfConfiguration conf;
    private static GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);
    private static GeoWaveGeoIndexer g;

    private static final StatementConstraints EMPTY_CONSTRAINTS = new StatementConstraints();

    // Here is the landscape:
    /**
     * <pre>
     *   2---+---+---+---+---+---+
     *   |        F      |G      |
     *   1  A    o(-1,1) o   C   |
     *   |               |       |
     *   0---+---+       +---+---+(3,0)
     *   |       |    E  |
     *  -1   B   +   .---+---+
     *   |       |  /|   |   |
     *  -2---+---+-/-+---+   +
     *   ^        /  |     D |
     *  -3  -2  -1   0---1---2   3   4
     * </pre>
     **/
    private static final Polygon A = poly(bbox(-3, -2, 1, 2));
    private static final Polygon B = poly(bbox(-3, -2, -1, 0));
    private static final Polygon C = poly(bbox(1, 0, 3, 2));
    private static final Polygon D = poly(bbox(0, -3, 2, -1));

    private static final Point F = point(-1, 1);
    private static final Point G = point(1, 1);

    private static final LineString E = line(-1, -3, 0, -1);

    private static final Map<Geometry, String> NAMES = ImmutableMap.<Geometry, String>builder()
        .put(A, "A")
        .put(B, "B")
        .put(C, "C")
        .put(D, "D")
        .put(E, "E")
        .put(F, "F")
        .put(G, "G")
        .build();

    private static File tempAccumuloDir;
    private static MiniAccumuloClusterImpl accumulo;

    private static final boolean IS_MOCK = true;

    private static final String ACCUMULO_USER = IS_MOCK ? "username" : "root";
    private static final String ACCUMULO_PASSWORD = "password";

    /**
     * JUnit 4 parameterized iterates thru this list and calls the constructor with each.
     * For each test, Call the constructor three times, for WKT and for GML encoding 1, and GML encoding 2
     */
    private static final IRI USE_JTS_LIB_ENCODING = VF.createIRI("uri:useLib") ;
    private static final IRI USE_ROUGH_ENCODING = VF.createIRI("uri:useRough") ;

    @Parameters
    public static Collection<IRI[]> constructorData() {
        final IRI[][] data = new IRI[][] { { GeoConstants.XMLSCHEMA_OGC_WKT, USE_JTS_LIB_ENCODING }, { GeoConstants.XMLSCHEMA_OGC_GML, USE_JTS_LIB_ENCODING }, { GeoConstants.XMLSCHEMA_OGC_GML, USE_JTS_LIB_ENCODING } };
        return Arrays.asList(data);
    }

    private final IRI schemaToTest;
    private final IRI encodeMethod;

    /**
     * Constructor required by JUnit parameterized runner.  See {@link #constructorData()} for constructor values.
     * @param schemaToTest the schema to test {@link IRI}.
     * @param encodeMethod the encode method {@link IRI}.
     */
    public GeoWaveIndexerSfTest(final IRI schemaToTest, final IRI encodeMethod) {
        this.schemaToTest = schemaToTest;
        this.encodeMethod = encodeMethod;
    }

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

    /**
     * Run before each test method.
     * @throws Exception
     */
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
        for (final String t : tops.list()) {
            if (t.startsWith(tableName)) {
                toDel.add(t);
            }
        }
        for (final String t : toDel) {
            tops.delete(t);
        }

        g = new GeoWaveGeoIndexer();
        g.setConf(conf);
        g.purge(conf);
        // Convert the statements as schema WKT or GML, then GML has two methods to encode.
        g.storeStatement(createRyaStatement(A, schemaToTest, encodeMethod));
        g.storeStatement(createRyaStatement(B, schemaToTest, encodeMethod));
        g.storeStatement(createRyaStatement(C, schemaToTest, encodeMethod));
        g.storeStatement(createRyaStatement(D, schemaToTest, encodeMethod));
        g.storeStatement(createRyaStatement(F, schemaToTest, encodeMethod));
        g.storeStatement(createRyaStatement(E, schemaToTest, encodeMethod));
        g.storeStatement(createRyaStatement(G, schemaToTest, encodeMethod));
    }

    private static RyaStatement createRyaStatement(final Geometry geo, final IRI schema, final IRI encodingMethod) {
        return RdfToRyaConversions.convertStatement(genericStatement(geo,schema,encodingMethod));
    }

    private static Statement genericStatement(final Geometry geo, final IRI schema, final IRI encodingMethod) {
        if (schema.equals(GeoConstants.XMLSCHEMA_OGC_WKT)) {
            return genericStatementWkt(geo);
        } else if (schema.equals(GeoConstants.XMLSCHEMA_OGC_GML)) {
            return genericStatementGml(geo, encodingMethod);
        }
        throw new Error("schema unsupported: "+schema);
    }

    private static Statement genericStatementWkt(final Geometry geo) {
        final Resource subject = VF.createIRI("uri:" + NAMES.get(geo));
        final IRI predicate = GeoConstants.GEO_AS_WKT;
        final Value object = VF.createLiteral(geo.toString(), GeoConstants.XMLSCHEMA_OGC_WKT);
        return VF.createStatement(subject, predicate, object);
    }

    private static Statement genericStatementGml(final Geometry geo, final IRI encodingMethod) {
        final Resource subject = VF.createIRI("uri:" + NAMES.get(geo));
        final IRI predicate = GeoConstants.GEO_AS_GML;

        final String gml ;
        if (encodingMethod == USE_JTS_LIB_ENCODING) {
            gml = geoToGmlUseJtsLib(geo);
        } else if (encodingMethod == USE_ROUGH_ENCODING) {
            gml = geoToGmlRough(geo);
        }
        else {
            throw new Error("invalid encoding method: "+encodingMethod);
        //        System.out.println("===created GML====");
        //        System.out.println(gml);
        //        System.out.println("========== GML====");
        }

        final Value object = VF.createLiteral(gml, GeoConstants.XMLSCHEMA_OGC_GML);
        return VF.createStatement(subject, predicate, object);
    }

    /**
     * JTS library conversion from geometry to GML.
     * @param geo base Geometry gets delegated
     * @return String gml encoding of the geomoetry
     */
    private static String geoToGmlUseJtsLib(final Geometry geo) {
        final int srid = geo.getSRID();
        final GMLWriter gmlWriter = new GMLWriter();
        gmlWriter.setNamespace(false);
        gmlWriter.setPrefix(null);

        if (srid != -1 || srid != 0) {
            gmlWriter.setSrsName("EPSG:" + geo.getSRID());
        }
        final String gml = gmlWriter.write(geo);
        // Hack to replace a gml 2.0 deprecated element in the Polygon.
        // It should tolerate this as it does other depreciated elements like <gml:coordinates>.
        return gml.replace("outerBoundaryIs", "exterior");
    }

    /**
     * Rough conversion from geometry to GML using a template.
     * @param geo base Geometry gets delegated
     * @return String gml encoding of the gemoetry
     */
    private static String geoToGmlRough(final Geometry geo) {
        final Geometries theType = org.geotools.geometry.jts.Geometries.get(geo);
        switch (theType) {
        case POINT:
            return geoToGml((Point)geo);
        case LINESTRING:
            return geoToGml((LineString)geo);
        case POLYGON:
            return geoToGml((Polygon)geo);
        case MULTIPOINT:
        case MULTILINESTRING:
        case MULTIPOLYGON:
        default:
            throw new Error("No code to convert to GML for this type: "+theType);
        }
    }

    private static Point point(final double x, final double y) {
        return gf.createPoint(new Coordinate(x, y));
    }

    private static String geoToGml(final Point point) {
        //CRS:84 long X,lat Y
        //ESPG:4326 lat Y,long X
        return "<Point"//
        + " srsName='CRS:84'"// TODO: point.getSRID()
        + "><pos>"+point.getX()+" "+point.getY()+"</pos>  "// assumes  Y=lat  X=long
        + " </Point>";
    }

    private static LineString line(final double x1, final double y1, final double x2, final double y2) {
        return new LineString(new PackedCoordinateSequence.Double(new double[] { x1, y1, x2, y2 }, 2), gf);
    }

    /**
     * convert a lineString geometry to GML
     * @param line
     * @return String that is XML that is a GMLLiteral of line
     */
    private static String geoToGml(final LineString line) {
        final StringBuilder coordString = new StringBuilder() ;
        for (final Coordinate coor : line.getCoordinates()) {
            coordString.append(" ").append(coor.x).append(" ").append(coor.y); //ESPG:4326 lat/long
        }
        return " <gml:LineString srsName=\"http://www.opengis.net/def/crs/EPSG/0/4326\" xmlns:gml='http://www.opengis.net/gml'>\n"
                + "<gml:posList srsDimension=\"2\">"//
                + coordString //
                + "</gml:posList></gml:LineString >";
    }

    private static Polygon poly(final double[] arr) {
        final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(arr, 2));
        final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
        return p1;
    }

    /**
     * convert a Polygon geometry to GML
     * @param geometry
     * @return String that is XML that is a GMLLiteral of line
     */
    private static String geoToGml(final Polygon poly) {
        final StringBuilder coordString = new StringBuilder() ;
        for (final Coordinate coor : poly.getCoordinates()) {
            coordString.append(" ").append(coor.x).append(" ").append(coor.y); //ESPG:4326 lat/long
            //with commas:  coordString.append(" ").append(coor.x).append(",").append(coor.y);
        }
        return "<gml:Polygon srsName=\"EPSG:4326\"  xmlns:gml='http://www.opengis.net/gml'>\r\n"//
                + "<gml:exterior><gml:LinearRing>\r\n"//
                + "<gml:posList srsDimension='2'>\r\n"
                +  coordString
                + "</gml:posList>\r\n"//
                + "</gml:LinearRing></gml:exterior>\r\n</gml:Polygon>\r\n";
    }

    private static double[] bbox(final double x1, final double y1, final double x2, final double y2) {
        return new double[] { x1, y1, x1, y2, x2, y2, x2, y1, x1, y1 };
    }

    private void compare(final CloseableIteration<Statement, ?> actual, final Geometry... expected) throws Exception {
        final Set<Statement> expectedSet = Sets.newHashSet();
        for (final Geometry geo : expected) {
            expectedSet.add(RyaToRdfConversions.convertStatement(createRyaStatement(geo, this.schemaToTest, encodeMethod)));
        }

        Assert.assertEquals(expectedSet, getSet(actual));
    }

    private static final Geometry[] EMPTY_RESULTS = {};

    @Test
    public void testParsePoly() throws Exception {
        assertParseable(D);
    }

    @Test
    public void testParseLine() throws Exception {
        assertParseable(E);
    }

    @Test
    public void testParsePoint() throws Exception {
        assertParseable(F);
    }

    /**
     * Convert Geometry to Wkt|GML (schemaToTest), parse to Geometry, and compare to original.
     * @param originalGeom the original {@link Geometry}.
     * @throws ParseException
     */
    public void assertParseable(final Geometry originalGeom) throws ParseException {
        final Geometry parsedGeom = GeoParseUtils.getGeometry(genericStatement(originalGeom,schemaToTest, encodeMethod), new GmlParser());
        assertTrue("Parsed should equal original: "+originalGeom+" parsed: "+parsedGeom, originalGeom.equalsNorm(parsedGeom));
        assertEquals( originalGeom, parsedGeom ); //also passes
        assertTrue( originalGeom.equalsExact(parsedGeom) ); //also passes
    }

    @Test
    public void testEquals() throws Exception {
        // point
        compare(g.queryEquals(F, EMPTY_CONSTRAINTS), F);
        compare(g.queryEquals(point(-1, -1), EMPTY_CONSTRAINTS), EMPTY_RESULTS);

        // line
        compare(g.queryEquals(E, EMPTY_CONSTRAINTS), E);
        compare(g.queryEquals(line(-1, -1, 0, 0), EMPTY_CONSTRAINTS), EMPTY_RESULTS);

        // poly
        compare(g.queryEquals(A, EMPTY_CONSTRAINTS), A);
        compare(g.queryEquals(poly(bbox(-2, -2, 1, 2)), EMPTY_CONSTRAINTS), EMPTY_RESULTS);
    }

    @Test
    public void testDisjoint() throws Exception {
        // point
        compare(g.queryDisjoint(F, EMPTY_CONSTRAINTS), B, C, D, E, G);

        // line
        compare(g.queryDisjoint(E, EMPTY_CONSTRAINTS), B, C, F, G);

        // poly
        compare(g.queryDisjoint(A, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
        compare(g.queryDisjoint(B, EMPTY_CONSTRAINTS), C, D, F, E, G);
    }

    @Test
    public void testIntersectsPoint() throws Exception {
        compare(g.queryIntersects(F, EMPTY_CONSTRAINTS), A, F);
    }

    @Test
    public void testIntersectsLine() throws Exception {
        compare(g.queryIntersects(E, EMPTY_CONSTRAINTS), A, E, D);
    }

    @Test
    public void testIntersectsPoly() throws Exception {
        compare(g.queryIntersects(A, EMPTY_CONSTRAINTS), A, B, C, D, F, E, G);
    }

    @Test
    public void testTouchesPoint() throws Exception {
        compare(g.queryTouches(F, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
        compare(g.queryTouches(G, EMPTY_CONSTRAINTS), A, C);
    }

    @Test
    public void testTouchesLine() throws Exception {
        compare(g.queryTouches(E, EMPTY_CONSTRAINTS), D);
    }

    @Test
    public void testTouchesPoly() throws Exception {
        compare(g.queryTouches(A, EMPTY_CONSTRAINTS), C,G);
    }

    @Test
    public void testCrossesPoint() throws Exception {
        compare(g.queryCrosses(F, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
        compare(g.queryCrosses(G, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
        compare(g.queryCrosses(point(2, 0), EMPTY_CONSTRAINTS), EMPTY_RESULTS);
    }

    @Test
    public void testCrossesLine() throws Exception {
        compare(g.queryCrosses(E, EMPTY_CONSTRAINTS), A);
    }

    @Test
    public void testCrossesPoly() throws Exception {
        compare(g.queryCrosses(A, EMPTY_CONSTRAINTS), E);
        compare(g.queryCrosses(poly(bbox(-0.9, -2.9, -0.1, -1.1)), EMPTY_CONSTRAINTS), E);
    }

    @Test
    public void testWithin() throws Exception {
        // point
        compare(g.queryWithin(F, EMPTY_CONSTRAINTS), F);

        // line
        compare(g.queryWithin(E, EMPTY_CONSTRAINTS), E);

        // poly
        compare(g.queryWithin(A, EMPTY_CONSTRAINTS), A, B, F);
    }

    @Test
    public void testContainsPoint() throws Exception {
        compare(g.queryContains(F, EMPTY_CONSTRAINTS), A, F);
    }

    @Test
    public void testContainsLine() throws Exception {
        compare(g.queryContains(E, EMPTY_CONSTRAINTS), E);
    }

    @Test
    public void testContainsPoly() throws Exception {
        compare(g.queryContains(A, EMPTY_CONSTRAINTS), A);
        compare(g.queryContains(B, EMPTY_CONSTRAINTS), A, B);
    }

    @Test
    public void testOverlapsPoint() throws Exception {
        compare(g.queryOverlaps(F, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
    }

    @Test
    public void testOverlapsLine() throws Exception {
        compare(g.queryOverlaps(E, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
    }

    @Test
    public void testOverlapsPoly() throws Exception {
        compare(g.queryOverlaps(A, EMPTY_CONSTRAINTS), D);
    }

}
