/*
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

import static org.apache.rya.api.domain.VarNameUtils.prependConstant;
import static org.apache.rya.indexing.geotemporal.GeoTemporalTestUtils.getFilters;
import static org.apache.rya.indexing.geotemporal.GeoTemporalTestUtils.getSps;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.IndexingFunctionRegistry;
import org.apache.rya.indexing.IndexingFunctionRegistry.FUNCTION_TYPE;
import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.geotemporal.mongo.MongoEventStorage;
import org.apache.rya.indexing.geotemporal.storage.EventStorage;
import org.apache.rya.test.mongo.MongoITBase;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.FunctionCall;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.ValueConstant;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * Integration tests the methods of {@link EventQueryNode}.
 */
public class EventQueryNode2IT extends MongoITBase {
    private static final GeometryFactory GF = new GeometryFactory(new PrecisionModel(), 4326);
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Test(expected = IllegalStateException.class)
    public void constructor_differentSubjects() throws Exception {
        final Var geoSubj = new Var("point");
        final Var geoPred = new Var(prependConstant("http://www.opengis.net/ont/geosparql#asWKT"), VF.createIRI("http://www.opengis.net/ont/geosparql#asWKT"));
        final Var geoObj = new Var("wkt");
        final StatementPattern geoSP = new StatementPattern(geoSubj, geoPred, geoObj);

        final Var timeSubj = new Var("time");
        final Var timePred = new Var(prependConstant("http://www.w3.org/2006/time#inXSDDateTime"), VF.createIRI(prependConstant("http://www.w3.org/2006/time#inXSDDateTime")));
        final Var timeObj = new Var("time");
        final StatementPattern timeSP = new StatementPattern(timeSubj, timePred, timeObj);
        // This will fail.
        new EventQueryNode.EventQueryNodeBuilder()
            .setStorage(mock(EventStorage.class))
            .setGeoPattern(geoSP)
            .setTemporalPattern(timeSP)
            .setGeoFilters(new ArrayList<IndexingExpr>())
            .setTemporalFilters(new ArrayList<IndexingExpr>())
            .setUsedFilters(new ArrayList<>())
            .build();
    }

    @Test(expected = IllegalStateException.class)
    public void constructor_variablePredicate() throws Exception {
        // A pattern that has a variable for its predicate.
        final Var geoSubj = new Var("point");
        final Var geoPred = new Var("geo");
        final Var geoObj = new Var("wkt");
        final StatementPattern geoSP = new StatementPattern(geoSubj, geoPred, geoObj);

        final Var timeSubj = new Var("time");
        final Var timePred = new Var(prependConstant("http://www.w3.org/2006/time#inXSDDateTime"), VF.createIRI(prependConstant("http://www.w3.org/2006/time#inXSDDateTime")));
        final Var timeObj = new Var("time");
        final StatementPattern timeSP = new StatementPattern(timeSubj, timePred, timeObj);
        // This will fail.
        new EventQueryNode.EventQueryNodeBuilder()
        .setStorage(mock(EventStorage.class))
        .setGeoPattern(geoSP)
        .setTemporalPattern(timeSP)
        .setGeoFilters(new ArrayList<IndexingExpr>())
        .setTemporalFilters(new ArrayList<IndexingExpr>())
        .setUsedFilters(new ArrayList<>())
        .build();
    }

    @Test
    public void evaluate_constantSubject() throws Exception {
        final EventStorage storage = new MongoEventStorage(super.getMongoClient(), "testDB");
        RyaIRI subject = new RyaIRI("urn:event-1111");
        final Geometry geo = GF.createPoint(new Coordinate(1, 1));
        final TemporalInstant temp = new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0);
        final Event event = Event.builder()
            .setSubject(subject)
            .setGeometry(geo)
            .setTemporalInstant(temp)
            .build();

        subject = new RyaIRI("urn:event-2222");
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
                + "  FILTER(tempo:equals(?time, \"" + temp.toString() + "\")) "
              + "}";

        final EventQueryNode node = buildNode(storage, query);
        final CloseableIteration<BindingSet, QueryEvaluationException> rez = node.evaluate(new MapBindingSet());
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("wkt", VF.createLiteral("POINT (1 1)"));
        expected.addBinding("time", VF.createLiteral(temp.toString()));
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
        final EventStorage storage = new MongoEventStorage(super.getMongoClient(), "testDB");
        RyaIRI subject = new RyaIRI("urn:event-1111");
        Geometry geo = GF.createPoint(new Coordinate(1, 1));
        final TemporalInstant temp = new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0);
        final Event event = Event.builder()
            .setSubject(subject)
            .setGeometry(geo)
            .setTemporalInstant(temp)
            .build();

        subject = new RyaIRI("urn:event-2222");
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
        expected1.addBinding("time", VF.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString()));
        final MapBindingSet expected2 = new MapBindingSet();
        expected2.addBinding("wkt", VF.createLiteral("POINT (-1 -1)"));
        expected2.addBinding("time", VF.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString()));

        final List<BindingSet> actual = new ArrayList<>();
        while(rez.hasNext()) {
            actual.add(rez.next());
        }
        assertEquals(expected1, actual.get(0));
        assertEquals(expected2, actual.get(1));
        assertEquals(2, actual.size());
    }

    @Test
    public void evaluate_variableSubject_existingBindingset() throws Exception {
        final EventStorage storage = new MongoEventStorage(super.getMongoClient(), "testDB");
        RyaIRI subject = new RyaIRI("urn:event-1111");
        Geometry geo = GF.createPoint(new Coordinate(1, 1));
        final TemporalInstant temp = new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0);
        final Event event = Event.builder()
            .setSubject(subject)
            .setGeometry(geo)
            .setTemporalInstant(temp)
            .build();

        subject = new RyaIRI("urn:event-2222");
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
        final MapBindingSet existingBindings = new MapBindingSet();
        existingBindings.addBinding("event", VF.createIRI("urn:event-2222"));
        final CloseableIteration<BindingSet, QueryEvaluationException> rez = node.evaluate(existingBindings);
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("wkt", VF.createLiteral("POINT (-1 -1)"));
        expected.addBinding("time", VF.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString()));

        final List<BindingSet> actual = new ArrayList<>();
        while(rez.hasNext()) {
            actual.add(rez.next());
        }
        assertEquals(1, actual.size());
        assertEquals(expected, actual.get(0));
    }

    @Test
    public void evaluate_variableSubject_existingBindingsetWrongFilters() throws Exception {
        final EventStorage storage = new MongoEventStorage(super.getMongoClient(), "testDB");
        RyaIRI subject = new RyaIRI("urn:event-1111");
        Geometry geo = GF.createPoint(new Coordinate(1, 1));
        final TemporalInstant temp = new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0);
        final Event event = Event.builder()
            .setSubject(subject)
            .setGeometry(geo)
            .setTemporalInstant(temp)
            .build();

        subject = new RyaIRI("urn:event-2222");
        geo = GF.createPoint(new Coordinate(-10, -10));
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
        final MapBindingSet existingBindings = new MapBindingSet();
        existingBindings.addBinding("event", VF.createIRI("urn:event-2222"));
        final CloseableIteration<BindingSet, QueryEvaluationException> rez = node.evaluate(existingBindings);
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("wkt", VF.createLiteral("POINT (-1 -1)"));
        expected.addBinding("time", VF.createLiteral(new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 0).toString()));

        assertFalse(rez.hasNext());
    }

    private static EventQueryNode buildNode(final EventStorage store, final String query) throws Exception {
        final List<IndexingExpr> geoFilters = new ArrayList<>();
        final List<IndexingExpr> temporalFilters = new ArrayList<>();
        final List<StatementPattern> sps = getSps(query);
        final List<FunctionCall> filters = getFilters(query);
        for(final FunctionCall filter : filters) {
            final IRI filterIRI = VF.createIRI(filter.getURI());
            final Var objVar = IndexingFunctionRegistry.getResultVarFromFunctionCall(filterIRI, filter.getArgs());
            final Value[] arguments = extractArguments(objVar.getName(), filter);
            final IndexingExpr expr = new IndexingExpr(filterIRI, sps.get(0), Arrays.stream(arguments).toArray());
            if(IndexingFunctionRegistry.getFunctionType(filterIRI) == FUNCTION_TYPE.GEO) {
                geoFilters.add(expr);
            } else {
                temporalFilters.add(expr);
            }
        }

        final StatementPattern geoPattern = sps.get(1);
        final StatementPattern temporalPattern = sps.get(0);

        return new EventQueryNode.EventQueryNodeBuilder()
            .setStorage(store)
            .setGeoPattern(geoPattern)
            .setTemporalPattern(temporalPattern)
            .setGeoFilters(geoFilters)
            .setTemporalFilters(temporalFilters)
            .setUsedFilters(filters)
            .build();
    }

    private static Value[] extractArguments(final String matchName, final FunctionCall call) {
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