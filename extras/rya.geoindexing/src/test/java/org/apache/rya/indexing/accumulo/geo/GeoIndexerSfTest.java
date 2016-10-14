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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.admin.TableOperations;
import org.geotools.geometry.jts.Geometries;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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

import info.aduna.iteration.CloseableIteration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.accumulo.ConfigUtils;

/**
 * Tests all of the "simple functions" of the geoindexer specific to GML.
 * Parameterized so that each test is run for WKT and for GML.
 */
@RunWith(value = Parameterized.class)
public class GeoIndexerSfTest {
    private static AccumuloRdfConfiguration conf;
    private static GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);
    private static GeoMesaGeoIndexer g;

    private static final StatementConstraints EMPTY_CONSTRAINTS = new StatementConstraints();

    // Here is the landscape:
    /**
     * <pre>
     * 	 2---+---+---+---+---+---+
     * 	 |        F      |G      |
     * 	 1  A    o(-1,1) o   C   |
     * 	 |               |       |
     * 	 0---+---+       +---+---+(3,0)
     * 	 |       |    E  |    
     * 	-1   B   +   .---+---+
     * 	 |       |  /|   |   |
     * 	-2---+---+-/-+---+   +
     * 	 ^        /  |     D |
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

    private static final Map<Geometry, String> names = Maps.newHashMap();
    static {
        names.put(A, "A");
        names.put(B, "B");
        names.put(C, "C");
        names.put(D, "D");
        names.put(E, "E");
        names.put(F, "F");
        names.put(G, "G");
    }

    /**
     * JUnit 4 parameterized iterates thru this list and calls the constructor with each. 
     * For each test, Call the constructor three times, for WKT and for GML encoding 1, and GML encoding 2 
     * @return
     */
    final static URI useJtsLibEncoding = new URIImpl("uri:useLib") ;
    final static URI useRoughEncoding = new URIImpl("uri:useRough") ;
    
    @Parameters
    public static Collection<URI[]> constructorData() {
        URI[][] data = new URI[][] { { GeoConstants.XMLSCHEMA_OGC_WKT,useJtsLibEncoding }, { GeoConstants.XMLSCHEMA_OGC_GML,useJtsLibEncoding } , { GeoConstants.XMLSCHEMA_OGC_GML,useRoughEncoding } };
        return Arrays.asList(data);
    }

    private URI schemaToTest;
    private URI encodeMethod;
    /**
     * Constructor required by JUnit parameterized runner.  See data() for constructor values.
     */
    public GeoIndexerSfTest(URI schemaToTest, URI encodeMethod) {
        this.schemaToTest=schemaToTest;
        this.encodeMethod = encodeMethod;
    }
    /**
     * Run before each test method.
     * @throws Exception
     */
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
        for (String t : tops.list()) {
            if (t.startsWith(tableName)) {
                toDel.add(t);
            }
        }
        for (String t : toDel) {
            tops.delete(t);
        }

        g = new GeoMesaGeoIndexer();
        g.setConf(conf);
        // Convert the statements as schema WKT or GML, then GML has two methods to encode.
        g.storeStatement(RyaStatement(A,schemaToTest, encodeMethod));
        g.storeStatement(RyaStatement(B,schemaToTest, encodeMethod));
        g.storeStatement(RyaStatement(C,schemaToTest, encodeMethod));
        g.storeStatement(RyaStatement(D,schemaToTest, encodeMethod));
        g.storeStatement(RyaStatement(F,schemaToTest, encodeMethod));
        g.storeStatement(RyaStatement(E,schemaToTest, encodeMethod));
        g.storeStatement(RyaStatement(G,schemaToTest, encodeMethod));
    }

    private static RyaStatement RyaStatement(Geometry geo, URI schema, URI encodingMethod) {
        return RdfToRyaConversions.convertStatement(genericStatement(geo,schema,encodingMethod));
    }
    private static Statement genericStatement(Geometry geo, URI schema, URI encodingMethod) {
        if (schema.equals(GeoConstants.XMLSCHEMA_OGC_WKT)) {
            return genericStatementWkt(geo);
        } else if (schema.equals(GeoConstants.XMLSCHEMA_OGC_GML)) {
            return genericStatementGml(geo, encodingMethod);
        }
        throw new Error("schema unsupported: "+schema);
    }
    private static Statement genericStatementWkt(Geometry geo) {
        ValueFactory vf = new ValueFactoryImpl();
        Resource subject = vf.createURI("uri:" + names.get(geo));
        URI predicate = GeoConstants.GEO_AS_WKT;
        Value object = vf.createLiteral(geo.toString(), GeoConstants.XMLSCHEMA_OGC_WKT);
        return new StatementImpl(subject, predicate, object);
    }

    private static Statement genericStatementGml(Geometry geo, URI encodingMethod) {
        ValueFactory vf = new ValueFactoryImpl();
        Resource subject = vf.createURI("uri:" + names.get(geo));
        URI predicate = GeoConstants.GEO_AS_GML;
        
        final String gml ;
        if (encodingMethod==useJtsLibEncoding) 
            gml = geoToGmlUseJtsLib(geo);
        else if (encodingMethod==useRoughEncoding)
            gml = geoToGmlRough(geo);
        else
            throw new Error("invalid encoding method: "+encodingMethod);
        //        System.out.println("===created GML====");
        //        System.out.println(gml);
        //        System.out.println("========== GML====");

        Value object = vf.createLiteral(gml, GeoConstants.XMLSCHEMA_OGC_GML);
        return new StatementImpl(subject, predicate, object);
    }

    /**
     * JTS library conversion from geometry to GML.
     * @param geo base Geometry gets delegated
     * @return String gml encoding of the geomoetry
     */
    private static String geoToGmlUseJtsLib(Geometry geo) {
        int srid = geo.getSRID();
        GMLWriter gmlWriter = new GMLWriter();
        gmlWriter.setNamespace(false);
        gmlWriter.setPrefix(null);
        
        if (srid != -1 || srid != 0) {
            gmlWriter.setSrsName("EPSG:" + geo.getSRID());
        }
        String gml = gmlWriter.write(geo);
        // Hack to replace a gml 2.0 deprecated element in the Polygon.  
        // It should tolerate this as it does other depreciated elements like <gml:coordinates>.
        return gml.replace("outerBoundaryIs", "exterior");
    }

    /**
     * Rough conversion from geometry to GML using a template.
     * @param geo base Geometry gets delegated
     * @return String gml encoding of the gemoetry
     */
        private static String geoToGmlRough(Geometry geo) {
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

    private static Point point(double x, double y) {
        return gf.createPoint(new Coordinate(x, y));
    }

    private static String geoToGml(Point point) {
        //CRS:84 long X,lat Y
        //ESPG:4326 lat Y,long X
        return "<Point"//
        + " srsName='CRS:84'"// TODO: point.getSRID()  
        + "><pos>"+point.getX()+" "+point.getY()+"</pos>  "// assumes  Y=lat  X=long 
        + " </Point>";
    }

    private static LineString line(double x1, double y1, double x2, double y2) {
        return new LineString(new PackedCoordinateSequence.Double(new double[] { x1, y1, x2, y2 }, 2), gf);
    }
    /** 
     * convert a lineString geometry to GML
     * @param line
     * @return String that is XML that is a GMLLiteral of line
     */
    private static String geoToGml(LineString line) {
        StringBuilder coordString = new StringBuilder() ;
        for (Coordinate coor : line.getCoordinates()) {
            coordString.append(" ").append(coor.x).append(" ").append(coor.y); //ESPG:4326 lat/long
        }
        return " <gml:LineString srsName=\"http://www.opengis.net/def/crs/EPSG/0/4326\" xmlns:gml='http://www.opengis.net/gml'>\n"  
                + "<gml:posList srsDimension=\"2\">"//
                + coordString //
                + "</gml:posList></gml:LineString >";
    }

    private static Polygon poly(double[] arr) {
        LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(arr, 2));
        Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
        return p1;
    }
    /** 
     * convert a Polygon geometry to GML
     * @param geometry
     * @return String that is XML that is a GMLLiteral of line
     */
    private static String geoToGml(Polygon poly) {
        StringBuilder coordString = new StringBuilder() ;
        for (Coordinate coor : poly.getCoordinates()) {
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

    private static double[] bbox(double x1, double y1, double x2, double y2) {
        return new double[] { x1, y1, x1, y2, x2, y2, x2, y1, x1, y1 };
    }

    public void compare(CloseableIteration<Statement, ?> actual, Geometry... expected) throws Exception {
        Set<Statement> expectedSet = Sets.newHashSet();
        for (Geometry geo : expected) {
            expectedSet.add(RyaToRdfConversions.convertStatement(RyaStatement(geo,this.schemaToTest, encodeMethod)));
        }

        Assert.assertEquals(expectedSet, getSet(actual));
    }

    private static <X> Set<X> getSet(CloseableIteration<X, ?> iter) throws Exception {
        Set<X> set = new HashSet<X>();
        while (iter.hasNext()) {
            set.add(iter.next());
        }
        return set;
    }

    private static Geometry[] EMPTY_RESULTS = {};

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
     * @throws ParseException
     */
    public void assertParseable(Geometry originalGeom) throws ParseException {
        Geometry parsedGeom = GeoParseUtils.getGeometry(genericStatement(originalGeom,schemaToTest, encodeMethod));
        assertTrue("Parsed should equal original: "+originalGeom+" parsed: "+parsedGeom, originalGeom.equalsNorm(parsedGeom));
        // assertEquals( originalGeom, parsedGeom ); //also passes
        // assertTrue( originalGeom.equalsExact(parsedGeom) ); //also passes
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
    @Ignore
    public void testIntersectsPoint() throws Exception {
        // This seems like a bug
        //   scala.MatchError: POINT (2 4) (of class com.vividsolutions.jts.geom.Point)
        //   at org.locationtech.geomesa.filter.FilterHelper$.updateToIDLSafeFilter(FilterHelper.scala:53)
        // compare(g.queryIntersects(F, EMPTY_CONSTRAINTS), A, F);
        // compare(g.queryIntersects(F, EMPTY_CONSTRAINTS), EMPTY_RESULTS);    
    }

    @Ignore
    @Test
    public void testIntersectsLine() throws Exception {
        // This seems like a bug
        // fails with: 
        //     scala.MatchError: LINESTRING (2 0, 3 3) (of class com.vividsolutions.jts.geom.LineString)
        //     at org.locationtech.geomesa.filter.FilterHelper$.updateToIDLSafeFilter(FilterHelper.scala:53)
        //compare(g.queryIntersects(E, EMPTY_CONSTRAINTS), A, E, D);
        //compare(g.queryIntersects(E, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
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
        // bug? java.lang.IllegalStateException:  getX called on empty Point
        //    compare(g.queryCrosses(point(2, 0), EMPTY_CONSTRAINTS), E);
    }

    @Ignore
    @Test
    public void testCrossesLine() throws Exception {
        // fails with:
        //     java.lang.IllegalStateException: getX called on empty Point
        //      at com.vividsolutions.jts.geom.Point.getX(Point.java:124)
        //      at org.locationtech.geomesa.utils.geohash.GeohashUtils$.considerCandidate$1(GeohashUtils.scala:1023)

        // compare(g.queryCrosses(E, EMPTY_CONSTRAINTS), A);
    }

    @Test
    public void testCrossesPoly() throws Exception {
        compare(g.queryCrosses(A, EMPTY_CONSTRAINTS), E);
        compare(g.queryCrosses(poly(bbox(-0.9, -2.9, -0.1, -1.1)), EMPTY_CONSTRAINTS), E);
    }

    @Test
    public void testWithin() throws Exception {
        // point
        // geomesa bug? scala.MatchError: POINT (2 4) (of class com.vividsolutions.jts.geom.Point)
        //    compare(g.queryWithin(F, EMPTY_CONSTRAINTS), F);

        // line
        // geomesa bug? scala.MatchError: LINESTRING (2 0, 3 2) (of class com.vividsolutions.jts.geom.LineString)
        //    compare(g.queryWithin(E, EMPTY_CONSTRAINTS), E);

        // poly
        compare(g.queryWithin(A, EMPTY_CONSTRAINTS), A, B, F);
    }

    @Test
    public void testContainsPoint() throws Exception {
        compare(g.queryContains(F, EMPTY_CONSTRAINTS), A, F);
    }

    @Ignore
    @Test
    public void testContainsLine() throws Exception {
        // compare(g.queryContains(E, EMPTY_CONSTRAINTS), E);
    }

    @Test
    public void testContainsPoly() throws Exception {
        compare(g.queryContains(A, EMPTY_CONSTRAINTS), A);
        compare(g.queryContains(B, EMPTY_CONSTRAINTS), A, B);
    }

    @Ignore
    @Test
    public void testOverlapsPoint() throws Exception {
        // compare(g.queryOverlaps(F, EMPTY_CONSTRAINTS), F);
        // You cannot have overlapping points
        // compare(g.queryOverlaps(F, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
    }

    @Ignore
    @Test
    public void testOverlapsLine() throws Exception {
        // compare(g.queryOverlaps(E, EMPTY_CONSTRAINTS), A, E);
        // You cannot have overlapping lines
        // compare(g.queryOverlaps(E, EMPTY_CONSTRAINTS), EMPTY_RESULTS);
    }

    @Test
    public void testOverlapsPoly() throws Exception {
        compare(g.queryOverlaps(A, EMPTY_CONSTRAINTS), D);
    }

}
