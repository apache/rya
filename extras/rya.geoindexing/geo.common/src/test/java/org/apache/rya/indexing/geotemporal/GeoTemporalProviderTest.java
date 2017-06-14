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
package org.apache.rya.indexing.geotemporal;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.apache.rya.indexing.geotemporal.GeoTemporalIndexSetProvider;
import org.apache.rya.indexing.geotemporal.model.EventQueryNode;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

public class GeoTemporalProviderTest extends GeoTemporalTestBase {
    private static final String URI_PROPERTY_AT_TIME = "Property:atTime";
    private GeoTemporalIndexSetProvider provider;
    private EventStorage events;
    @Before
    public void setup() {
        events = mock(EventStorage.class);
        provider = new GeoTemporalIndexSetProvider(events);
    }

    /*
     * Simplest Happy Path test
     */
    @Test
    public void twoPatternsTwoFilters_test() throws Exception {
        final ValueFactory vf = new ValueFactoryImpl();
        final Value geo = vf.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
        final Value temp = vf.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        final URI tempPred = vf.createURI(URI_PROPERTY_AT_TIME);
        final String query =
            "PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
            "PREFIX geos: <http://www.opengis.net/def/function/geosparql/>" +
            "PREFIX time: <tag:rya-rdf.org,2015:temporal#>" +
            "SELECT * WHERE { " +
                "?subj <" + tempPred + "> ?time ."+
                "?subj <" + GeoConstants.GEO_AS_WKT + "> ?loc . " +
                " FILTER(geos:sfContains(?loc, " + geo + ")) . " +
                " FILTER(time:equals(?time, " + temp + ")) . " +
            "}";
        final QuerySegment<EventQueryNode> node = getQueryNode(query);
        final List<EventQueryNode> nodes = provider.getExternalSets(node);
        assertEquals(1, nodes.size());
    }

    @Test
    public void onePatternTwoFilters_test() throws Exception {
        final ValueFactory vf = new ValueFactoryImpl();
        final Value geo = vf.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
        final Value temp = vf.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        final URI tempPred = vf.createURI(URI_PROPERTY_AT_TIME);
        final String query =
            "PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
            "PREFIX geos: <http://www.opengis.net/def/function/geosparql/>" +
            "PREFIX time: <tag:rya-rdf.org,2015:temporal#>" +
            "SELECT * WHERE { " +
                "?subj <" + tempPred + "> ?time ."+
                " FILTER(geos:sfContains(?loc, " + geo + ")) . " +
                " FILTER(time:equals(?time, " + temp + ")) . " +
            "}";
        final QuerySegment<EventQueryNode> node = getQueryNode(query);
        final List<EventQueryNode> nodes = provider.getExternalSets(node);
        assertEquals(0, nodes.size());
    }

    @Test
    public void twoPatternsOneFilter_test() throws Exception {
        final ValueFactory vf = new ValueFactoryImpl();
        final Value geo = vf.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
        final Value temp = vf.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        final URI tempPred = vf.createURI(URI_PROPERTY_AT_TIME);
        final String query =
            "PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
            "PREFIX geos: <http://www.opengis.net/def/function/geosparql/>" +
            "PREFIX time: <tag:rya-rdf.org,2015:temporal#>" +
            "SELECT * WHERE { " +
                "?subj <" + tempPred + "> ?time ."+
                "?subj <" + GeoConstants.GEO_AS_WKT + "> ?loc . " +
                " FILTER(geos:sfContains(?loc, " + geo + ")) . " +
            "}";
        final QuerySegment<EventQueryNode> node = getQueryNode(query);
        final List<EventQueryNode> nodes = provider.getExternalSets(node);
        assertEquals(0, nodes.size());
    }

    @Test
    public void twoPatternsNoFilter_test() throws Exception {
        final ValueFactory vf = new ValueFactoryImpl();
        final URI tempPred = vf.createURI(URI_PROPERTY_AT_TIME);
        final String query =
            "PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
            "PREFIX geos: <http://www.opengis.net/def/function/geosparql/>" +
            "PREFIX time: <tag:rya-rdf.org,2015:temporal#>" +
            "SELECT * WHERE { " +
                "?subj <" + tempPred + "> ?time ."+
                "?subj <" + GeoConstants.GEO_AS_WKT + "> ?loc . " +
            "}";
        final QuerySegment<EventQueryNode> node = getQueryNode(query);
        final List<EventQueryNode> nodes = provider.getExternalSets(node);
        assertEquals(0, nodes.size());
    }

    @Test
    public void twoPatternsTwoFiltersNotValid_test() throws Exception {
        final ValueFactory vf = new ValueFactoryImpl();
        final Value geo = vf.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
        final Value temp = vf.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        final URI tempPred = vf.createURI(URI_PROPERTY_AT_TIME);
        //Only handles geo and temporal filters
        final String query =
            "PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
            "PREFIX geos: <http://www.opengis.net/def/function/geosparql/>" +
            "PREFIX text: <http://rdf.useekm.com/fts#text>" +
            "SELECT * WHERE { " +
                "?subj <" + tempPred + "> ?time ."+
                "?subj <" + GeoConstants.GEO_AS_WKT + "> ?loc . " +
                " FILTER(geos:sfContains(?loc, " + geo + ")) . " +
                " FILTER(text:equals(?time, " + temp + ")) . " +
            "}";
        final QuerySegment<EventQueryNode> node = getQueryNode(query);
        final List<EventQueryNode> nodes = provider.getExternalSets(node);
        assertEquals(0, nodes.size());
    }

    @Test
    public void twoSubjOneFilter_test() throws Exception {
        final ValueFactory vf = new ValueFactoryImpl();
        final Value geo = vf.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
        final Value temp = vf.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        final URI tempPred = vf.createURI(URI_PROPERTY_AT_TIME);
        final String query =
            "PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
            "PREFIX geos: <http://www.opengis.net/def/function/geosparql/>" +
            "PREFIX time: <tag:rya-rdf.org,2015:temporal#>" +
            "SELECT * WHERE { " +
                "?subj <" + tempPred + "> ?time ."+
                "?subj <" + GeoConstants.GEO_AS_WKT + "> ?loc . " +
                "?subj2 <" + tempPred + "> ?time2 ."+
                "?subj2 <" + GeoConstants.GEO_AS_WKT + "> ?loc2 . " +
                " FILTER(geos:sfContains(?loc, " + geo + ")) . " +
                " FILTER(time:equals(?time, " + temp + ")) . " +
            "}";
        final QuerySegment<EventQueryNode> node = getQueryNode(query);
        final List<EventQueryNode> nodes = provider.getExternalSets(node);
        assertEquals(1, nodes.size());
    }

    @Test
    public void twoNode_test() throws Exception {
        final ValueFactory vf = new ValueFactoryImpl();
        final Value geo = vf.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
        final Value temp = vf.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        final URI tempPred = vf.createURI(URI_PROPERTY_AT_TIME);
        final String query =
            "PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
            "PREFIX geos: <http://www.opengis.net/def/function/geosparql/>" +
            "PREFIX time: <tag:rya-rdf.org,2015:temporal#>" +
            "SELECT * WHERE { " +
                "?subj <" + tempPred + "> ?time ."+
                "?subj <" + GeoConstants.GEO_AS_WKT + "> ?loc . " +
                "?subj2 <" + tempPred + "> ?time2 ."+
                "?subj2 <" + GeoConstants.GEO_AS_WKT + "> ?loc2 . " +
                " FILTER(geos:sfContains(?loc, " + geo + ")) . " +
                " FILTER(time:equals(?time, " + temp + ")) . " +
                " FILTER(geos:sfContains(?loc2, " + geo + ")) . " +
                " FILTER(time:equals(?time2, " + temp + ")) . " +
            "}";
        final QuerySegment<EventQueryNode> node = getQueryNode(query);
        final List<EventQueryNode> nodes = provider.getExternalSets(node);
        assertEquals(2, nodes.size());
    }

    @Test
    public void twoSubjectMultiFilter_test() throws Exception {
        final ValueFactory vf = new ValueFactoryImpl();
        final Value geo = vf.createLiteral("Point(0 0)", GeoConstants.XMLSCHEMA_OGC_WKT);
        final Value temp = vf.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString());
        final URI tempPred = vf.createURI(URI_PROPERTY_AT_TIME);
        final String query =
            "PREFIX geo: <http://www.opengis.net/ont/geosparql#>" +
            "PREFIX geos: <http://www.opengis.net/def/function/geosparql/>" +
            "PREFIX time: <tag:rya-rdf.org,2015:temporal#>" +
            "SELECT * WHERE { " +
                "?subj <" + tempPred + "> ?time ."+
                "?subj <" + GeoConstants.GEO_AS_WKT + "> ?loc . " +
                " FILTER(geos:sfContains(?loc, " + geo + ")) . " +
                " FILTER(time:equals(?time, " + temp + ")) . " +
                " FILTER(geos:sfWithin(?loc, " + geo + ")) . " +
                " FILTER(time:before(?time, " + temp + ")) . " +
            "}";
        final QuerySegment<EventQueryNode> node = getQueryNode(query);
        final List<EventQueryNode> nodes = provider.getExternalSets(node);
        assertEquals(1, nodes.size());
    }
}
