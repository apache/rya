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

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.apache.rya.indexing.geotemporal.model.EventQueryNode;
import org.junit.ComparisonFailure;
import org.mockito.Mockito;
import org.openrdf.query.algebra.FunctionCall;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;

public class GeoTemporalTestBase {
    private static final GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);

    /**
     * Make an uniform instant with given seconds.
     */
    protected static TemporalInstant makeInstant(final int secondsMakeMeUnique) {
        return new TemporalInstantRfc3339(2015, 12, 30, 12, 00, secondsMakeMeUnique);
    }

    protected static Polygon poly(final double[] arr) {
        final LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(arr, 2));
        final Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
        return p1;
    }

    protected static Point point(final double x, final double y) {
        return gf.createPoint(new Coordinate(x, y));
    }

    protected static LineString line(final double x1, final double y1, final double x2, final double y2) {
        return new LineString(new PackedCoordinateSequence.Double(new double[] { x1, y1, x2, y2 }, 2), gf);
    }

    protected static double[] bbox(final double x1, final double y1, final double x2, final double y2) {
        return new double[] { x1, y1, x1, y2, x2, y2, x2, y1, x1, y1 };
    }

    protected void assertEqualMongo(final Object expected, final Object actual) throws ComparisonFailure {
        try {
            assertEquals(expected, actual);
        } catch(final Throwable e) {
            throw new ComparisonFailure(e.getMessage(), expected.toString(), actual.toString());
        }
    }

    public List<FunctionCall> getFilters(final String query) throws Exception {
        final FunctionCallCollector collector = new FunctionCallCollector();
        new SPARQLParser().parseQuery(query, null).getTupleExpr().visit(collector);
        return collector.getTupleExpr();
    }

    public List<StatementPattern> getSps(final String query) throws Exception {
        final StatementPatternCollector collector = new StatementPatternCollector();
        new SPARQLParser().parseQuery(query, null).getTupleExpr().visit(collector);
        return collector.getStatementPatterns();
    }

    public QuerySegment<EventQueryNode> getQueryNode(final String query) throws Exception {
        final List<QueryModelNode> exprs = getNodes(query);
        final QuerySegment<EventQueryNode> node = Mockito.mock(QuerySegment.class);
        //provider only cares about Ordered nodes.
        Mockito.when(node.getOrderedNodes()).thenReturn(exprs);
        return node;
    }

    private static List<QueryModelNode> getNodes(final String sparql) throws Exception {
        final NodeCollector collector = new NodeCollector();
        new SPARQLParser().parseQuery(sparql, null).getTupleExpr().visit(collector);
        return collector.getTupleExpr();
    }

    private static class NodeCollector extends QueryModelVisitorBase<RuntimeException> {
        private final List<QueryModelNode> stPatterns = new ArrayList<>();

        public List<QueryModelNode> getTupleExpr() {
            return stPatterns;
        }

        @Override
        public void meet(final FunctionCall node) {
            stPatterns.add(node);
        }

        @Override
        public void meet(final StatementPattern node) {
            stPatterns.add(node);
        }
    }

    private static class FunctionCallCollector extends QueryModelVisitorBase<RuntimeException> {
        private final List<FunctionCall> filters = new ArrayList<>();

        public List<FunctionCall> getTupleExpr() {
            return filters;
        }

        @Override
        public void meet(final FunctionCall node) {
            filters.add(node);
        }
    }
}
