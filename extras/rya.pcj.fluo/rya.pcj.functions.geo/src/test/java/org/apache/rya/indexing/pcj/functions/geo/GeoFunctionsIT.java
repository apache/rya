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
package org.apache.rya.indexing.pcj.functions.geo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.function.Function;
import org.openrdf.query.algebra.evaluation.function.FunctionRegistry;
import org.openrdf.query.impl.BindingImpl;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

/**
 * Performs integration tests PCJ Geospatial functions in SPARQL.
 * Each test starts a Accumulo/Rya/Fluo single node stack and runs a continuous query, checking results.
 */
public class GeoFunctionsIT extends ITBase {

    @Test
    public void verifySpiLoadedGeoFunctions() {
        final String functions[] = { "distance", //
                "convexHull", "boundary", "envelope", "union", "intersection", "symDifference", "difference", //
                "relate", /* "equals", */ "sfDisjoint", "sfIntersects", "sfTouches", "sfCrosses", //
                "sfWithin", "sfContains", "sfOverlaps", "ehDisjoint", "ehMeet", "ehOverlap", //
                "ehCovers", "ehCoveredBy", "ehInside", "ehContains", "rcc8dc", "rcc8ec", //
                "rcc8po", "rcc8tppi", "rcc8tpp", "rcc8ntpp", "rcc8ntppi" }; //
        HashSet<String> functionsCheckList = new HashSet<String>();
        functionsCheckList.addAll(Arrays.asList(functions));
        for (String f : FunctionRegistry.getInstance().getKeys()) {
            String functionShortName = f.replaceFirst("^.*/geosparql/(.*)", "$1");
            // System.out.println("Registered function: " + f + " shortname: " + functionShortName);
            functionsCheckList.remove(functionShortName);
        }
        assertTrue("Missed loading these functions via SPI: " + functionsCheckList, functionsCheckList.isEmpty());
    }

    @Test
    public void withGeoFilters() throws Exception {
        final String geoWithinSelect = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "//
                        + "PREFIX ryageo: <tag:rya.apache.org,2017:function/geo#> "//
                        + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "//
                        + "SELECT ?feature ?point ?wkt " //
                        + "{" //
                        + " ?feature a geo:Feature . "//
                        + " ?feature geo:hasGeometry ?point . "//
                        + " ?point a geo:Point . "//
                        + " ?point geo:asWKT ?wkt . "//
                        + " FILTER(ryageo:ehContains(?wkt, \"POLYGON((-77 39, -76 39, -76 38, -77 38, -77 39))\"^^geo:wktLiteral)) " //
                        + "}";//
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(makeRyaStatement("tag:rya.apache.org,2017:ex#feature", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.opengis.net/ont/geosparql#Feature"), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#feature", "http://www.opengis.net/ont/geosparql#hasGeometry", "tag:rya.apache.org,2017:ex#test_point"), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#test_point", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.opengis.net/ont/geosparql#Point"), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#test_point", "http://www.opengis.net/ont/geosparql#asWKT", new RyaType(new URIImpl("http://www.opengis.net/ont/geosparql#wktLiteral"), "Point(-77.03524 38.889468)")) //
        );

        Function fooFunction = new Function() {
            @Override
            public String getURI() {
                return "tag:rya.apache.org,2017:function/geo#ehContains";
            }

            @Override
            public Value evaluate(ValueFactory valueFactory, Value... args) throws ValueExprEvaluationException {

                if (args.length != 2) {
                    throw new ValueExprEvaluationException(getURI() + " requires exactly 3 arguments, got " + args.length);
                }
                // SpatialContext spatialContext = (new SpatialContextFactory()).newSpatialContext();
                // Shape shape1 = org.eclipse.rdf4j.query.algebra.evaluation.function.geosparql.FunctionArguments() .getShape(this, args[0], spatialContext);
                // Shape shape2 = FunctionArguments.getShape(this, args[1], spatialContext);
                // //https://github.com/eclipse/rdf4j/blob/master/core/queryalgebra/geosparql/src/main/java/org/eclipse/rdf4j/query/algebra/evaluation/function/geosparql/SpatialSupport.java
                // boolean result = SpatialSupport.getSpatialAlgebra().ehContains(shape1, shape2);
                // return valueFactory.createLiteral(result);
                return valueFactory.createLiteral(true);
            }
        };

        // Add our new function to the registry
        FunctionRegistry.getInstance().add(fooFunction);

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(//
                        new BindingImpl("wkt", new LiteralImpl("Point(-77.03524 38.889468)", new URIImpl("http://www.opengis.net/ont/geosparql#wktLiteral"))), //
                        new BindingImpl("feature", new URIImpl("tag:rya.apache.org,2017:ex#feature")), //
                        new BindingImpl("point", new URIImpl("tag:rya.apache.org,2017:ex#test_point"))));

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(geoWithinSelect);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, RYA_INSTANCE_NAME);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String> absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, geoWithinSelect);
        assertEquals(expected, results);
    }

    @Test
    public void GeoDistance() throws Exception {
        String geoCitySelect = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> " //
                        + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " //
                        + "PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/> " //
                        + "SELECT ?cityA ?cityB " //
                        // + "SELECT ?cityA ?cityB ?dist " //
                        + "WHERE { ?cityA geo:asWKT ?coord1 . " //
                        + "        ?cityB geo:asWKT ?coord2 . " //
                        // + " BIND( (geof:distance(?coord1, ?coord2, uom:metre) / 1000) as ?dist) . " // currently not supported
                        + " FILTER ( 500000 > geof:distance(?coord1, ?coord2, uom:metre)  ) . " // from brussels 173km to amsterdam
                        + " FILTER ( !sameTerm (?cityA, ?cityB) ) }"; //

        final URIImpl wktTypeUri = new URIImpl("http://www.opengis.net/ont/geosparql#wktLiteral");
        final String asWKT = "http://www.opengis.net/ont/geosparql#asWKT";
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(//
                        makeRyaStatement("tag:rya.apache.org,2017:ex#dakar", asWKT, new RyaType(wktTypeUri, "Point(-17.45 14.69)")), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#dakar2", asWKT, new RyaType(wktTypeUri, "Point(-17.45 14.69)")), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#canberra", asWKT, new RyaType(wktTypeUri, "Point(149.12 -35.31)")), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#brussels", asWKT, new RyaType(wktTypeUri, "Point(4.35 50.85)")), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#amsterdam", asWKT, new RyaType(wktTypeUri, "Point(4.9 52.37)")) //
        );

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(new BindingImpl("cityA", new URIImpl("tag:rya.apache.org,2017:ex#dakar")), new BindingImpl("cityB", new URIImpl("tag:rya.apache.org,2017:ex#dakar2"))));
        expected.add(makeBindingSet(new BindingImpl("cityA", new URIImpl("tag:rya.apache.org,2017:ex#dakar2")), new BindingImpl("cityB", new URIImpl("tag:rya.apache.org,2017:ex#dakar"))));
        expected.add(makeBindingSet(new BindingImpl("cityA", new URIImpl("tag:rya.apache.org,2017:ex#brussels")), new BindingImpl("cityB", new URIImpl("tag:rya.apache.org,2017:ex#amsterdam"))));
        expected.add(makeBindingSet(new BindingImpl("cityA", new URIImpl("tag:rya.apache.org,2017:ex#amsterdam")), new BindingImpl("cityB", new URIImpl("tag:rya.apache.org,2017:ex#brussels"))));

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(geoCitySelect);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, RYA_INSTANCE_NAME);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String> absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, geoCitySelect);

        results.forEach(res -> {
            // System.out.println(res.getValue("cityA").stringValue() + " - " + res.getValue("cityB").stringValue() + " : "
            // /* + res.getValue("dist").stringValue() + "km" */
            // );
        });

        assertEquals(expected, results);
    }

    /**
     * sfwithin function test. This requires full blown JTS.
     * If you see this error: "Unknown Shape definition [POLYGON" ...
     * Then:
     * (from Solr docs:) the field definition needs the attribute
     * spatialContextFactory="com.spatial4j.core.context.jts.JtsSpatialContextFactory"
     * or (this works) this system property must be set to :
     * SpatialContextFactory=com.spatial4j.core.context.jts.JtsSpatialContextFactory
     * If you see:
     * java.lang.UnsupportedOperationException: Not supported due to licensing issues. Feel free to provide your own implementation by using something like JTS.
     * Then add a bit of code to replace the default one that comes with RDF4J:
     * SpatialSupportInitializer.java
     * Here is one: https://bitbucket.org/pulquero/sesame-geosparql-jts
     *
     * @throws Exception
     */
    // @Ignore("needs JTS initializer, see comments.")
    @Test
    public void withGeoSpatialSupportInitializer() throws Exception {
        final String geoWithinSelect = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "//
                        + "PREFIX ryageo: <tag:rya.apache.org,2017:function/geo#> "//
                        + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "//
                        + "SELECT ?feature ?point ?wkt " //
                        + "{" //
                        + " ?feature a geo:Feature . "//
                        + " ?feature geo:hasGeometry ?point . "//
                        + " ?point a geo:Point . "//
                        + " ?point geo:asWKT ?wkt . "//
                        + " FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -76 39, -76 38, -78 38, -78 39))\"^^geo:wktLiteral)) " //
                        + "}";//
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(//
                        makeRyaStatement("tag:rya.apache.org,2017:ex#feature", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.opengis.net/ont/geosparql#Feature"), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#feature", "http://www.opengis.net/ont/geosparql#hasGeometry", "tag:rya.apache.org,2017:ex#test_point"), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#test_point", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.opengis.net/ont/geosparql#Point"), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#test_point", "http://www.opengis.net/ont/geosparql#asWKT", new RyaType(new URIImpl("http://www.opengis.net/ont/geosparql#wktLiteral") //
                                        , "Point(-77.03524 38.889468)")), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#skip_point", "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "http://www.opengis.net/ont/geosparql#Point"), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#skip_point", "http://www.opengis.net/ont/geosparql#asWKT", new RyaType(new URIImpl("http://www.opengis.net/ont/geosparql#wktLiteral")//
                                        , "Point(-10 10)")) //
        );
        // Register geo functions from RDF4J is done automatically via SPI.
        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(new BindingImpl("wkt", (new LiteralImpl("Point(-77.03524 38.889468)", new URIImpl("http://www.opengis.net/ont/geosparql#wktLiteral")))), new BindingImpl("feature", new URIImpl("tag:rya.apache.org,2017:ex#feature")), new BindingImpl("point", new URIImpl("tag:rya.apache.org,2017:ex#test_point"))));
        // expected.add(makeBindingSet(new BindingImpl("wkt", (new LiteralImpl("Point(-77.03524 38.889468)", new URIImpl("http://www.opengis.net/ont/geosparql#wktLiteral"))))));
        // expected.add(makeBindingSet(new BindingImpl("wkt", new URIImpl("\"Point(-77.03524 38.889468)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>")), new BindingImpl("feature", new
        // URIImpl("tag:rya.apache.org,2017:ex#feature")), new BindingImpl("point", new URIImpl("tag:rya.apache.org,2017:ex#test_point"))));

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(geoWithinSelect);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, RYA_INSTANCE_NAME);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String> absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, geoWithinSelect);
        assertEquals(expected, results);
    }

    /**
     * This test does not rely on geoTools. The default implementation in RDF4J handles point intersections.
     * 
     * @throws Exception
     */
    @Test
    public void withGeoIntersectsPoint() throws Exception {
        String geoCitySelect = "PREFIX geo: <http://www.opengis.net/ont/geosparql#> " //
                        + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " //
                        + "PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/> " //
                        + "SELECT ?cityA ?cityB " //
                        + "WHERE { ?cityA geo:asWKT ?coord1 . " //
                        + "        ?cityB geo:asWKT ?coord2 . " //
                        + " FILTER ( geof:sfIntersects(?coord1, ?coord2) ) " //
                        + " FILTER ( !sameTerm (?cityA, ?cityB) ) }"; //

        final URIImpl wktTypeUri = new URIImpl("http://www.opengis.net/ont/geosparql#wktLiteral");
        final String asWKT = "http://www.opengis.net/ont/geosparql#asWKT";
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(//
                        makeRyaStatement("tag:rya.apache.org,2017:ex#dakar", asWKT, new RyaType(wktTypeUri, "Point(-17.45 14.69)")), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#canberra", asWKT, new RyaType(wktTypeUri, "Point(149.12 -35.31)")), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#brussels", asWKT, new RyaType(wktTypeUri, "Point(4.35 50.85)")), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#amsterdam", asWKT, new RyaType(wktTypeUri, "Point(4.9 52.37)")), //
                        makeRyaStatement("tag:rya.apache.org,2017:ex#amsterdam2", asWKT, new RyaType(wktTypeUri, "Point(4.9 52.37)")) //
        );

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(new BindingImpl("cityA", new URIImpl("tag:rya.apache.org,2017:ex#amsterdam")), new BindingImpl("cityB", new URIImpl("tag:rya.apache.org,2017:ex#amsterdam2"))));
        expected.add(makeBindingSet(new BindingImpl("cityA", new URIImpl("tag:rya.apache.org,2017:ex#amsterdam2")), new BindingImpl("cityB", new URIImpl("tag:rya.apache.org,2017:ex#amsterdam"))));

        // Register geo functions from RDF4J is done automatically via SPI.
        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(geoCitySelect);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, RYA_INSTANCE_NAME);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String> absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, geoCitySelect);

        results.forEach(res -> {
            // System.out.println(res.getValue("cityA").stringValue() + " - " + res.getValue("cityB").stringValue() + " : "
            // /* + res.getValue("dist").stringValue() + "km" */
            // );
        });

        assertEquals(expected, results);
    }

    @Test
    public void withTemporal() throws Exception {
        final String dtPredUri = "http://www.w3.org/2006/time#inXSDDateTime";
        final String xmlDateTime = "http://www.w3.org/2001/XMLSchema#dateTime";
        // Find all stored dates.
        String selectQuery = "PREFIX time: <http://www.w3.org/2006/time#> \n"//
                        + "PREFIX xml: <http://www.w3.org/2001/XMLSchema#> \n" //
                        + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"//
                        + "SELECT ?event ?time \n" //
                        + "WHERE { \n" //
                        + "  ?event time:inXSDDateTime ?time . \n"//
                        // + " FILTER(?time > '2000-01-01T01:00:00Z'^^xml:dateTime) \n"// all
                        // + " FILTER(?time < '2007-01-01T01:01:03-08:00'^^xml:dateTime) \n"// after 2007
                        + " FILTER(?time > '2001-01-01T01:01:03-08:00'^^xml:dateTime) \n"// after 3 seconds
                        + " FILTER('2007-01-01T01:01:01+09:00'^^xml:dateTime > ?time ) \n"// 2006/12/31 include 2006, not 2007,8
                        + "}";//

        // create some resources and literals to make statements out of
        String eventz = "<http://eventz>";
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(//
                        makeRyaStatement(eventz, "http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "<http://www.w3.org/2006/time#Instant>"), //
                        makeRyaStatement(eventz, dtPredUri, new RyaType(new URIImpl(xmlDateTime), "2001-01-01T01:01:01-08:00")), // one second
                        makeRyaStatement(eventz, dtPredUri, new RyaType(new URIImpl(xmlDateTime), "2001-01-01T04:01:02.000-05:00")), // 2 seconds
                        makeRyaStatement(eventz, dtPredUri, new RyaType(new URIImpl(xmlDateTime), "2001-01-01T01:01:03-08:00")), // 3 seconds
                        makeRyaStatement(eventz, dtPredUri, new RyaType(new URIImpl(xmlDateTime), "2001-01-01T01:01:03.999-08:00")), // 4 seconds
                        makeRyaStatement(eventz, dtPredUri, new RyaType(new URIImpl(xmlDateTime), "2001-01-01T09:01:05Z")), // 5 seconds
                        makeRyaStatement(eventz, dtPredUri, new RyaType(new URIImpl(xmlDateTime), "2006-01-01TZ")), //
                        makeRyaStatement(eventz, dtPredUri, new RyaType(new URIImpl(xmlDateTime), "2007-01-01TZ")), //
                        makeRyaStatement(eventz, dtPredUri, new RyaType(new URIImpl(xmlDateTime), "2008-01-01TZ")));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(new BindingImpl("time", new LiteralImpl("2001-01-01T09:01:03.999Z", new URIImpl(xmlDateTime))), new BindingImpl("event", new URIImpl(eventz)))); //
        expected.add(makeBindingSet(new BindingImpl("time", new LiteralImpl("2001-01-01T09:01:05.000Z", new URIImpl(xmlDateTime))), new BindingImpl("event", new URIImpl(eventz)))); //
        expected.add(makeBindingSet(new BindingImpl("time", new LiteralImpl("2006-01-01T00:00:00.000Z", new URIImpl(xmlDateTime))), new BindingImpl("event", new URIImpl(eventz)))); //
        expected.add(makeBindingSet(new BindingImpl("time", new LiteralImpl("2006-01-01T00:00:00.000Z", new URIImpl(xmlDateTime))), new BindingImpl("event", new URIImpl(eventz)))); //
        // expected.add(makeBindingSet(new BindingImpl("event", new URIImpl(eventz)), new BindingImpl("time", new LiteralImpl("2007-01-01T05:00:00.000Z", new URIImpl(xmlDateTime))))); //
        // expected.add(makeBindingSet(new BindingImpl("event", new URIImpl(eventz)), new BindingImpl("time", new LiteralImpl("2008-01-01T05:00:00.000Z", new URIImpl(xmlDateTime)))));

        // Register geo functions from RDF4J is done automatically via SPI.
        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(selectQuery);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, RYA_INSTANCE_NAME);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String> absent());

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();
        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, selectQuery);
        assertEquals(expected, results);
    }

}

