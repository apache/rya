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

import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.xml.datatype.DatatypeFactory;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.junit.Test;
import  org.eclipse.rdf4j.model.Statement;
import  org.eclipse.rdf4j.model.URI;
import  org.eclipse.rdf4j.model.Value;
import  org.eclipse.rdf4j.model.ValueFactory;
import  org.eclipse.rdf4j.model.impl.ValueFactoryImpl;
import  org.eclipse.rdf4j.query.BindingSet;
import  org.eclipse.rdf4j.query.algebra.evaluation.ValueExprEvaluationException;
import  org.eclipse.rdf4j.query.algebra.evaluation.function.Function;
import  org.eclipse.rdf4j.query.algebra.evaluation.function.FunctionRegistry;
import  org.eclipse.rdf4j.query.impl.MapBindingSet;
import  org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;

import com.google.common.collect.Sets;

/**
 * Performs integration tests PCJ Geospatial functions in SPARQL.
 * Each test starts a Accumulo/Rya/Fluo single node stack and runs a continuous query, checking results.
 */
public class GeoFunctionsIT extends RyaExportITBase {

    @Test
    public void verifySpiLoadedGeoFunctions() {
        final String functions[] = { "distance", "convexHull", "boundary", "envelope", "union", "intersection",
                "symDifference", "difference", "relate", "sfDisjoint", "sfIntersects", "sfTouches", "sfCrosses",
                "sfWithin", "sfContains", "sfOverlaps", "ehDisjoint", "ehMeet", "ehOverlap", "ehCovers", "ehCoveredBy",
                "ehInside", "ehContains", "rcc8dc", "rcc8ec", "rcc8po", "rcc8tppi", "rcc8tpp", "rcc8ntpp", "rcc8ntppi" };
        final HashSet<String> functionsCheckList = new HashSet<>();
        functionsCheckList.addAll(Arrays.asList(functions));
        for (final String f : FunctionRegistry.getInstance().getKeys()) {
            final String functionShortName = f.replaceFirst("^.*/geosparql/(.*)", "$1");
            functionsCheckList.remove(functionShortName);
        }
        assertTrue("Missed loading these functions via SPI: " + functionsCheckList, functionsCheckList.isEmpty());
    }

    @Test
    public void withGeoFilters() throws Exception {
        final String sparql =
                "PREFIX geo: <http://www.opengis.net/ont/geosparql#> " +
                "PREFIX ryageo: <tag:rya.apache.org,2017:function/geo#> " +
                "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " +
                "SELECT ?feature ?point ?wkt {" +
                    " ?feature a geo:Feature . " +
                    " ?feature geo:hasGeometry ?point . " +
                    " ?point a geo:Point . " +
                    " ?point geo:asWKT ?wkt . " +
                    " FILTER(ryageo:ehContains(?wkt, \"POLYGON((-77 39, -76 39, -76 38, -77 38, -77 39))\"^^geo:wktLiteral)) " +
                "}";

        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Set<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#feature"), vf.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), vf.createIRI("http://www.opengis.net/ont/geosparql#Feature")),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#feature"), vf.createIRI("http://www.opengis.net/ont/geosparql#hasGeometry"), vf.createIRI("tag:rya.apache.org,2017:ex#test_point")),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#test_point"), vf.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), vf.createIRI("http://www.opengis.net/ont/geosparql#Point")),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#test_point"), vf.createIRI("http://www.opengis.net/ont/geosparql#asWKT"), vf.createLiteral("Point(-77.03524 38.889468)", vf.createIRI("http://www.opengis.net/ont/geosparql#wktLiteral"))));

        // Create a Geo function.
        final Function geoFunction = new Function() {
            @Override
            public String getURI() {
                return "tag:rya.apache.org,2017:function/geo#ehContains";
            }

            @Override
            public Value evaluate(final ValueFactory valueFactory, final Value... args) throws ValueExprEvaluationException {
                if (args.length != 2) {
                    throw new ValueExprEvaluationException(getURI() + " requires exactly 3 arguments, got " + args.length);
                }
                return valueFactory.createLiteral(true);
            }
        };

        // Add our new function to the registry
        FunctionRegistry.getInstance().add(geoFunction);

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("wkt", vf.createLiteral("Point(-77.03524 38.889468)", vf.createIRI("http://www.opengis.net/ont/geosparql#wktLiteral")));
        bs.addBinding("feature", vf.createIRI("tag:rya.apache.org,2017:ex#feature"));
        bs.addBinding("point", vf.createIRI("tag:rya.apache.org,2017:ex#test_point"));
        expectedResults.add(bs);

        runTest(sparql, statements, expectedResults);
    }

    @Test
    public void GeoDistance() throws Exception {
        final String sparql =
                "PREFIX geo: <http://www.opengis.net/ont/geosparql#> " +
                "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " +
                "PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/> " +
                "SELECT ?cityA ?cityB " +
                "WHERE { " +
                    "?cityA geo:asWKT ?coord1 . " +
                    "?cityB geo:asWKT ?coord2 . " +
                    // from brussels 173km to amsterdam
                    " FILTER ( 500000 > geof:distance(?coord1, ?coord2, uom:metre)  ) . " +
                    " FILTER ( !sameTerm (?cityA, ?cityB) ) " +
                "}";

        final ValueFactory vf = SimpleValueFactory.getInstance();
        final URI wktTypeUri = vf.createIRI("http://www.opengis.net/ont/geosparql#wktLiteral");
        final URI asWKT = vf.createIRI("http://www.opengis.net/ont/geosparql#asWKT");
        final Set<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#dakar"), asWKT, vf.createLiteral("Point(-17.45 14.69)", wktTypeUri)),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#dakar2"), asWKT, vf.createLiteral("Point(-17.45 14.69)", wktTypeUri)),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#canberra"), asWKT, vf.createLiteral("Point(149.12 -35.31)", wktTypeUri)),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#brussels"), asWKT, vf.createLiteral("Point(4.35 50.85)", wktTypeUri)),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#amsterdam"), asWKT, vf.createLiteral("Point(4.9 52.37)", wktTypeUri)));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("cityA", vf.createIRI("tag:rya.apache.org,2017:ex#dakar"));
        bs.addBinding("cityB", vf.createIRI("tag:rya.apache.org,2017:ex#dakar2"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("cityA", vf.createIRI("tag:rya.apache.org,2017:ex#dakar2"));
        bs.addBinding("cityB", vf.createIRI("tag:rya.apache.org,2017:ex#dakar"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("cityA", vf.createIRI("tag:rya.apache.org,2017:ex#brussels"));
        bs.addBinding("cityB", vf.createIRI("tag:rya.apache.org,2017:ex#amsterdam"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("cityA", vf.createIRI("tag:rya.apache.org,2017:ex#amsterdam"));
        bs.addBinding("cityB", vf.createIRI("tag:rya.apache.org,2017:ex#brussels"));
        expectedResults.add(bs);

        runTest(sparql, statements, expectedResults);
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
     */
    @Test
    public void withGeoSpatialSupportInitializer() throws Exception {
        final String sparql =
                "PREFIX geo: <http://www.opengis.net/ont/geosparql#> " +
                "PREFIX ryageo: <tag:rya.apache.org,2017:function/geo#> " +
                "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> " +
                "SELECT ?feature ?point ?wkt { " +
                    "?feature a geo:Feature . " +
                    "?feature geo:hasGeometry ?point . " +
                    "?point a geo:Point . " +
                    "?point geo:asWKT ?wkt . " +
                    "FILTER(geof:sfWithin(?wkt, \"POLYGON((-78 39, -76 39, -76 38, -78 38, -78 39))\"^^geo:wktLiteral)) " +
                "}";

        final ValueFactory vf = SimpleValueFactory.getInstance();
        final Set<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#feature"), vf.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), vf.createIRI("http://www.opengis.net/ont/geosparql#Feature")),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#feature"), vf.createIRI("http://www.opengis.net/ont/geosparql#hasGeometry"), vf.createIRI("tag:rya.apache.org,2017:ex#test_point")),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#test_point"), vf.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), vf.createIRI("http://www.opengis.net/ont/geosparql#Point")),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#test_point"), vf.createIRI("http://www.opengis.net/ont/geosparql#asWKT"), vf.createLiteral("Point(-77.03524 38.889468)", vf.createIRI("http://www.opengis.net/ont/geosparql#wktLiteral"))),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#skip_point"), vf.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), vf.createIRI("http://www.opengis.net/ont/geosparql#Point")),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#skip_point"), vf.createIRI("http://www.opengis.net/ont/geosparql#asWKT"), vf.createLiteral("Point(-10 10)", vf.createIRI("http://www.opengis.net/ont/geosparql#wktLiteral"))));

        // Register geo functions from RDF4J is done automatically via SPI.
        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("wkt", vf.createLiteral("Point(-77.03524 38.889468)", vf.createIRI("http://www.opengis.net/ont/geosparql#wktLiteral")));
        bs.addBinding("feature", vf.createIRI("tag:rya.apache.org,2017:ex#feature"));
        bs.addBinding("point", vf.createIRI("tag:rya.apache.org,2017:ex#test_point"));
        expectedResults.add(bs);

        runTest(sparql, statements, expectedResults);
    }

    /**
     * This test does not rely on geoTools. The default implementation in RDF4J handles point intersections.
     */
    @Test
    public void withGeoIntersectsPoint() throws Exception {
        final String sparql =
                "PREFIX geo: <http://www.opengis.net/ont/geosparql#> "  +
                "PREFIX geof: <http://www.opengis.net/def/function/geosparql/> "  +
                "PREFIX uom: <http://www.opengis.net/def/uom/OGC/1.0/> "  +
                "SELECT ?cityA ?cityB { "  +
                    "?cityA geo:asWKT ?coord1 . " +
                    "?cityB geo:asWKT ?coord2 . " +
                    " FILTER ( geof:sfIntersects(?coord1, ?coord2) ) " +
                    " FILTER ( !sameTerm (?cityA, ?cityB) ) " +
                "}";

        final ValueFactory vf = SimpleValueFactory.getInstance();
        final URI wktTypeUri = vf.createIRI("http://www.opengis.net/ont/geosparql#wktLiteral");
        final URI asWKT = vf.createIRI("http://www.opengis.net/ont/geosparql#asWKT");
        final Set<Statement> statements = Sets.newHashSet(
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#dakar"), asWKT, vf.createLiteral("Point(-17.45 14.69)", wktTypeUri)),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#canberra"), asWKT, vf.createLiteral("Point(149.12 -35.31)", wktTypeUri)),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#brussels"), asWKT, vf.createLiteral("Point(4.35 50.85)", wktTypeUri)),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#amsterdam"), asWKT, vf.createLiteral("Point(4.9 52.37)", wktTypeUri)),
                vf.createStatement(vf.createIRI("tag:rya.apache.org,2017:ex#amsterdam2"), asWKT, vf.createLiteral("Point(4.9 52.37)", wktTypeUri)));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("cityA", vf.createIRI("tag:rya.apache.org,2017:ex#amsterdam"));
        bs.addBinding("cityB", vf.createIRI("tag:rya.apache.org,2017:ex#amsterdam2"));
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("cityA", vf.createIRI("tag:rya.apache.org,2017:ex#amsterdam2"));
        bs.addBinding("cityB", vf.createIRI("tag:rya.apache.org,2017:ex#amsterdam"));
        expectedResults.add(bs);

        runTest(sparql, statements, expectedResults);
    }

    @Test
    public void withTemporal() throws Exception {
        // Find all stored dates.
        final String sparql =
                "PREFIX time: <http://www.w3.org/2006/time#> " +
                "PREFIX xml: <http://www.w3.org/2001/XMLSchema#> " +
                "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> " +
                "SELECT ?event ?time { " +
                    "?event time:inXSDDateTime ?time . " +
                    "FILTER(?time > '2001-01-01T01:01:03-08:00'^^xml:dateTime) " + // after 3 seconds
                    "FILTER('2007-01-01T01:01:01+09:00'^^xml:dateTime > ?time ) " + // 2006/12/31 include 2006, not 2007,8
                "}";

        // create some resources and literals to make statements out of
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final DatatypeFactory dtf = DatatypeFactory.newInstance();

        final URI dtPredUri = vf.createIRI("http://www.w3.org/2006/time#inXSDDateTime");
        final URI eventz = vf.createIRI("<http://eventz>");

        final Set<Statement> statements = Sets.newHashSet(
                vf.createStatement(eventz, vf.createIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), vf.createIRI("<http://www.w3.org/2006/time#Instant>")),
                vf.createStatement(eventz, dtPredUri, vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T01:01:01-08:00"))), // 1 second
                vf.createStatement(eventz, dtPredUri, vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T04:01:02.000-05:00"))), // 2 seconds
                vf.createStatement(eventz, dtPredUri, vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T01:01:03-08:00"))), // 3 seconds
                vf.createStatement(eventz, dtPredUri, vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T01:01:03.999-08:00"))), // 4 seconds
                vf.createStatement(eventz, dtPredUri, vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T09:01:05Z"))), // 5 seconds
                vf.createStatement(eventz, dtPredUri, vf.createLiteral(dtf.newXMLGregorianCalendar("2006-01-01T05:00:00.000Z"))),
                vf.createStatement(eventz, dtPredUri, vf.createLiteral(dtf.newXMLGregorianCalendar("2007-01-01T05:00:00.000Z"))),
                vf.createStatement(eventz, dtPredUri, vf.createLiteral(dtf.newXMLGregorianCalendar("2008-01-01T05:00:00.000Z"))));

        final Set<BindingSet> expectedResults = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("time", vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T09:01:05.000Z")));
        bs.addBinding("event", eventz);
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("time", vf.createLiteral(dtf.newXMLGregorianCalendar("2006-01-01T05:00:00.000Z")));
        bs.addBinding("event", eventz);
        expectedResults.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("time", vf.createLiteral(dtf.newXMLGregorianCalendar("2001-01-01T09:01:03.999Z")));
        bs.addBinding("event", eventz);
        expectedResults.add(bs);

        runTest(sparql, statements, expectedResults);
    }

    public void runTest(final String sparql, final Collection<Statement> statements, final Collection<BindingSet> expectedResults) throws Exception {
        requireNonNull(sparql);
        requireNonNull(statements);
        requireNonNull(expectedResults);

        // Register the PCJ with Rya.
        final Instance accInstance = super.getAccumuloConnector().getInstance();
        final Connector accumuloConn = super.getAccumuloConnector();

        final RyaClient ryaClient = AccumuloRyaClientFactory.build(new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                accInstance.getInstanceName(),
                accInstance.getZooKeepers()), accumuloConn);

        ryaClient.getCreatePCJ().createPCJ(getRyaInstanceName(), sparql);

        // Write the data to Rya.
        final SailRepositoryConnection ryaConn = super.getRyaSailRepository().getConnection();
        ryaConn.begin();
        ryaConn.add(statements);
        ryaConn.commit();
        ryaConn.close();

        // Wait for the Fluo application to finish computing the end result.
        super.getMiniFluo().waitForObservers();

        // Fetch the value that is stored within the PCJ table.
        try(final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName())) {
            final String pcjId = pcjStorage.listPcjs().get(0);
            final Set<BindingSet> results = Sets.newHashSet( pcjStorage.listResults(pcjId) );

            // Ensure the result of the query matches the expected result.
            assertEquals(expectedResults, results);
        }
    }
}