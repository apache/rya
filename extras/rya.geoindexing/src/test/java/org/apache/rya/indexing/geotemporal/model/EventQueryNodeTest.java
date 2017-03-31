/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.geotemporal.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.IndexingFunctionRegistry;
import org.apache.rya.indexing.IndexingFunctionRegistry.FUNCTION_TYPE;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.geotemporal.GeoTemporalTestBase;
import org.apache.rya.indexing.geotemporal.mongo.MongoEventStorage;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.apache.rya.mongodb.MockMongoFactory;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.impl.MapBindingSet;

import com.mongodb.MongoClient;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

import info.aduna.iteration.CloseableIteration;

/**
 * Unit tests the methods of {@link EventQueryNode}.
 */
public class EventQueryNodeTest extends GeoTemporalTestBase {
    private static final GeometryFactory GF = new GeometryFactory(new PrecisionModel(), 4326);
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();

    @Test(expected = IllegalStateException.class)
    public void constructor_differentSubjects() throws Exception {
        final Var geoSubj = new Var("point");
        final Var geoPred = new Var("-const-http://www.opengis.net/ont/geosparql#asWKT", ValueFactoryImpl.getInstance().createURI("http://www.opengis.net/ont/geosparql#asWKT"));
        final Var geoObj = new Var("wkt");
        final StatementPattern geoSP = new StatementPattern(geoSubj, geoPred, geoObj);

        final Var timeSubj = new Var("time");
        final Var timePred = new Var("-const-http://www.w3.org/2006/time#inXSDDateTime", ValueFactoryImpl.getInstance().createURI("-const-http://www.w3.org/2006/time#inXSDDateTime"));
        final Var timeObj = new Var("time");
        final StatementPattern timeSP = new StatementPattern(timeSubj, timePred, timeObj);
        // This will fail.
        new EventQueryNode(mock(EventStorage.class), geoSP, timeSP, new ArrayList<IndexingExpr>(), new ArrayList<IndexingExpr>(), new ArrayList<>());
    }

    @Test(expected = IllegalStateException.class)
    public void constructor_variablePredicate() throws Exception {
        // A pattern that has a variable for its predicate.
        final Var geoSubj = new Var("point");
        final Var geoPred = new Var("geo");
        final Var geoObj = new Var("wkt");
        final StatementPattern geoSP = new StatementPattern(geoSubj, geoPred, geoObj);

        final Var timeSubj = new Var("time");
        final Var timePred = new Var("-const-http://www.w3.org/2006/time#inXSDDateTime", ValueFactoryImpl.getInstance().createURI("-const-http://www.w3.org/2006/time#inXSDDateTime"));
        final Var timeObj = new Var("time");
        final StatementPattern timeSP = new StatementPattern(timeSubj, timePred, timeObj);
        // This will fail.
        new EventQueryNode(mock(EventStorage.class), geoSP, timeSP, new ArrayList<IndexingExpr>(), new ArrayList<IndexingExpr>(), new ArrayList<>());
    }

    @Test
    public void evaluate_constantSubject() throws Exception {
        final MongoClient client = MockMongoFactory.newFactory().newMongoClient();
        final EventStorage storage = new MongoEventStorage(client, "testDB");
        RyaURI subject = new RyaURI("urn:event-1111");
        final Geometry geo = GF.createPoint(new Coordinate(1, 1));
        final TemporalInstant temp = new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0);
        final Event event = Event.builder()
            .setSubject(subject)
            .setGeometry(geo)
            .setTemporalInstant(temp)
            .build();

        subject = new RyaURI("urn:event-2222");
        final Event otherEvent = Event.builder()
            .setSubject(subject)
            .setGeometry(geo)
            .setTemporalInstant(temp)
            .build();

        storage.create(event);
        storage.create(otherEvent);

        final String query =
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
              + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
              + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
              + "SELECT ?event ?time ?point ?wkt "
              + "WHERE { "
                + "  <urn:event-1111> time:atTime ?time . "
                + "  <urn:event-1111> geo:asWKT ?wkt . "
                + "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-3 -2, -3 2, 1 2, 1 -2, -3 -2))\"^^geo:wktLiteral)) "
                + "  FILTER(tempo:equals(?time, \"2015-12-30T12:00:00Z\")) "
              + "}";

        final EventQueryNode node = buildNode(storage, query);
        final CloseableIteration<BindingSet, QueryEvaluationException> rez = node.evaluate(new MapBindingSet());
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("wkt", VF.createLiteral("POINT (1 1)"));
        expected.addBinding("time", VF.createLiteral("2015-12-30T07:00:00-05:00"));
        int count = 0;
        assertTrue(rez.hasNext());
        while(rez.hasNext()) {
            assertEquals(expected, rez.next());
            count++;
        }
        assertEquals(1, count);
    }

    @Test
    public void evaluate_variableSubject() throws Exception {
        final MongoClient client = MockMongoFactory.newFactory().newMongoClient();
        final EventStorage storage = new MongoEventStorage(client, "testDB");
        RyaURI subject = new RyaURI("urn:event-1111");
        Geometry geo = GF.createPoint(new Coordinate(1, 1));
        final TemporalInstant temp = new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0);
        final Event event = Event.builder()
            .setSubject(subject)
            .setGeometry(geo)
            .setTemporalInstant(temp)
            .build();

        subject = new RyaURI("urn:event-2222");
        geo = GF.createPoint(new Coordinate(-1, -1));
        final Event otherEvent = Event.builder()
            .setSubject(subject)
            .setGeometry(geo)
            .setTemporalInstant(temp)
            .build();

        storage.create(event);
        storage.create(otherEvent);

        final String query =
                "PREFIX time: <http://www.w3.org/2006/time#> \n"
              + "PREFIX tempo: <tag:rya-rdf.org,2015:temporal#> \n"
              + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>"
              + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>"
              + "SELECT ?event ?time ?point ?wkt "
              + "WHERE { "
                + "  ?event time:atTime ?time . "
                + "  ?event geo:asWKT ?wkt . "
                + "  FILTER(geof:sfWithin(?wkt, \"POLYGON((-3 -2, -3 2, 1 2, 1 -2, -3 -2))\"^^geo:wktLiteral)) "
                + "  FILTER(tempo:equals(?time, \"2015-12-30T12:00:00Z\")) "
              + "}";

        final EventQueryNode node = buildNode(storage, query);
        final CloseableIteration<BindingSet, QueryEvaluationException> rez = node.evaluate(new MapBindingSet());
        final MapBindingSet expected1 = new MapBindingSet();
        expected1.addBinding("wkt", VF.createLiteral("POINT (1 1)"));
        expected1.addBinding("time", VF.createLiteral("2015-12-30T07:00:00-05:00"));

        final MapBindingSet expected2 = new MapBindingSet();
        expected2.addBinding("wkt", VF.createLiteral("POINT (-1 -1)"));
        expected2.addBinding("time", VF.createLiteral("2015-12-30T07:00:00-05:00"));

        final List<BindingSet> actual = new ArrayList<>();
        while(rez.hasNext()) {
            actual.add(rez.next());
        }
        assertEquals(expected1, actual.get(0));
        assertEquals(expected2, actual.get(1));
        assertEquals(2, actual.size());
    }

    private EventQueryNode buildNode(final EventStorage store, final String query) throws Exception {
        final List<IndexingExpr> geoFilters = new ArrayList<>();
        final List<IndexingExpr> temporalFilters = new ArrayList<>();
        final List<StatementPattern> sps = getSps(query);
        final List<FunctionCall> filters = getFilters(query);
        for(final FunctionCall filter : filters) {
            final URI filterURI = new URIImpl(filter.getURI());
            final Var objVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(filterURI, filter.getArgs());
            final IndexingExpr expr = new IndexingExpr(filterURI, sps.get(0), extractArguments(objVar.getName(), filter));
            if(IndexingFunctionRegistry.getFunctionType(filterURI) == FUNCTION_TYPE.GEO) {
                geoFilters.add(expr);
            } else {
                temporalFilters.add(expr);
            }
        }

        final StatementPattern geoPattern = sps.get(1);
        final StatementPattern temporalPattern = sps.get(0);

        return new EventQueryNode(store, geoPattern, temporalPattern, geoFilters, temporalFilters, filters);
    }

    private Value[] extractArguments(final String matchName, final FunctionCall call) {
        final Value args[] = new Value[call.getArgs().size() - 1];
        int argI = 0;
        for (int i = 0; i != call.getArgs().size(); ++i) {
            final ValueExpr arg = call.getArgs().get(i);
            if (argI == i && arg instanceof Var && matchName.equals(((Var)arg).getName())) {
                continue;
            }
            if (arg instanceof ValueConstant) {
                args[argI] = ((ValueConstant)arg).getValue();
            } else if (arg instanceof Var && ((Var)arg).hasValue()) {
                args[argI] = ((Var)arg).getValue();
            } else {
                throw new IllegalArgumentException("Query error: Found " + arg + ", expected a Literal, BNode or URI");
            }
            ++argI;
        }
        return args;
    }
}