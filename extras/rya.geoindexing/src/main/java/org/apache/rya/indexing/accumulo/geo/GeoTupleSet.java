package org.apache.rya.indexing.accumulo.geo;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.GeoConstants;
import org.apache.rya.indexing.GeoIndexer;
import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.IteratorFactory;
import org.apache.rya.indexing.SearchFunction;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

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


import info.aduna.iteration.CloseableIteration;

//Indexing Node for geo expressions to be inserted into execution plan
//to delegate geo portion of query to geo index
public class GeoTupleSet extends ExternalTupleSet {

    private final Configuration conf;
    private final GeoIndexer geoIndexer;
    private final IndexingExpr filterInfo;


    public GeoTupleSet(final IndexingExpr filterInfo, final GeoIndexer geoIndexer) {
        this.filterInfo = filterInfo;
        this.geoIndexer = geoIndexer;
        conf = geoIndexer.getConf();
    }

    @Override
    public Set<String> getBindingNames() {
        return filterInfo.getBindingNames();
    }

    @Override
	public GeoTupleSet clone() {
        return new GeoTupleSet(filterInfo, geoIndexer);
    }

    @Override
    public double cardinality() {
        return 0.0; // No idea how the estimate cardinality here.
    }


    @Override
    public String getSignature() {
        return "(GeoTuple Projection) " + "variables: " + Joiner.on(", ").join(getBindingNames()).replaceAll("\\s+", " ");
    }



    @Override
    public boolean equals(final Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof GeoTupleSet)) {
            return false;
        }
        final GeoTupleSet arg = (GeoTupleSet) other;
        return filterInfo.equals(arg.filterInfo);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31*result + filterInfo.hashCode();

        return result;
    }



    /**
     * Returns an iterator over the result set of the contained IndexingExpr.
     * <p>
     * Should be thread-safe (concurrent invocation {@link OfflineIterable} this
     * method can be expected with some query evaluators.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final BindingSet bindings)
            throws QueryEvaluationException {


        final URI funcURI = filterInfo.getFunction();
        final SearchFunction searchFunction = new GeoSearchFunctionFactory(conf, geoIndexer).getSearchFunction(funcURI);
        if(filterInfo.getArguments().length > 1) {
            throw new IllegalArgumentException("Index functions do not support more than two arguments.");
        }

        final String queryText = filterInfo.getArguments()[0].stringValue();

        return IteratorFactory.getIterator(filterInfo.getSpConstraint(), bindings, queryText, searchFunction);
    }



    //returns appropriate search function for a given URI
    //search functions used in GeoMesaGeoIndexer to access index
    public static class GeoSearchFunctionFactory {

        Configuration conf;

        private final Map<URI, SearchFunction> SEARCH_FUNCTION_MAP = Maps.newHashMap();

        private final GeoIndexer geoIndexer;

        public GeoSearchFunctionFactory(final Configuration conf, final GeoIndexer geoIndexer) {
            this.conf = conf;
            this.geoIndexer = geoIndexer;
        }


        /**
         * Get a {@link GeoSearchFunction} for a given URI.
         *
         * @param searchFunction
         * @return
         */
        public SearchFunction getSearchFunction(final URI searchFunction) {

            SearchFunction geoFunc = null;

            try {
                geoFunc = getSearchFunctionInternal(searchFunction);
            } catch (final QueryEvaluationException e) {
                e.printStackTrace();
            }

            return geoFunc;
        }

        private SearchFunction getSearchFunctionInternal(final URI searchFunction) throws QueryEvaluationException {
            final SearchFunction sf = SEARCH_FUNCTION_MAP.get(searchFunction);

            if (sf != null) {
                return sf;
            } else {
                throw new QueryEvaluationException("Unknown Search Function: " + searchFunction.stringValue());
            }
        }

        private final SearchFunction GEO_EQUALS = new SearchFunction() {

            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String queryText,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                try {
                    final WKTReader reader = new WKTReader();
                    final Geometry geometry = reader.read(queryText);
                    final CloseableIteration<Statement, QueryEvaluationException> statements = geoIndexer.queryWithin(
                            geometry, contraints);
                    return statements;
                } catch (final ParseException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public String toString() {
                return "GEO_EQUALS";
            };
        };

        private final SearchFunction GEO_DISJOINT = new SearchFunction() {

            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String queryText,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                try {
                    final WKTReader reader = new WKTReader();
                    final Geometry geometry = reader.read(queryText);
                    final CloseableIteration<Statement, QueryEvaluationException> statements = geoIndexer.queryWithin(
                            geometry, contraints);
                    return statements;
                } catch (final ParseException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public String toString() {
                return "GEO_DISJOINT";
            };
        };

        private final SearchFunction GEO_INTERSECTS = new SearchFunction() {

            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String queryText,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                try {
                    final WKTReader reader = new WKTReader();
                    final Geometry geometry = reader.read(queryText);
                    final CloseableIteration<Statement, QueryEvaluationException> statements = geoIndexer.queryWithin(
                            geometry, contraints);
                    return statements;
                } catch (final ParseException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public String toString() {
                return "GEO_INTERSECTS";
            };
        };

        private final SearchFunction GEO_TOUCHES = new SearchFunction() {

            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String queryText,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                try {
                    final WKTReader reader = new WKTReader();
                    final Geometry geometry = reader.read(queryText);
                    final CloseableIteration<Statement, QueryEvaluationException> statements = geoIndexer.queryWithin(
                            geometry, contraints);
                    return statements;
                } catch (final ParseException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public String toString() {
                return "GEO_TOUCHES";
            };
        };

        private final SearchFunction GEO_CONTAINS = new SearchFunction() {

            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String queryText,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                try {
                    final WKTReader reader = new WKTReader();
                    final Geometry geometry = reader.read(queryText);
                    final CloseableIteration<Statement, QueryEvaluationException> statements = geoIndexer.queryWithin(
                            geometry, contraints);
                    return statements;
                } catch (final ParseException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public String toString() {
                return "GEO_CONTAINS";
            };
        };

        private final SearchFunction GEO_OVERLAPS = new SearchFunction() {

            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String queryText,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                try {
                    final WKTReader reader = new WKTReader();
                    final Geometry geometry = reader.read(queryText);
                    final CloseableIteration<Statement, QueryEvaluationException> statements = geoIndexer.queryWithin(
                            geometry, contraints);
                    return statements;
                } catch (final ParseException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public String toString() {
                return "GEO_OVERLAPS";
            };
        };

        private final SearchFunction GEO_CROSSES = new SearchFunction() {

            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String queryText,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                try {
                    final WKTReader reader = new WKTReader();
                    final Geometry geometry = reader.read(queryText);
                    final CloseableIteration<Statement, QueryEvaluationException> statements = geoIndexer.queryWithin(
                            geometry, contraints);
                    return statements;
                } catch (final ParseException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public String toString() {
                return "GEO_CROSSES";
            };
        };

        private final SearchFunction GEO_WITHIN = new SearchFunction() {

            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String queryText,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                try {
                    final WKTReader reader = new WKTReader();
                    final Geometry geometry = reader.read(queryText);
                    final CloseableIteration<Statement, QueryEvaluationException> statements = geoIndexer.queryWithin(
                            geometry, contraints);
                    return statements;
                } catch (final ParseException e) {
                    throw new QueryEvaluationException(e);
                }
            }

            @Override
            public String toString() {
                return "GEO_WITHIN";
            };
        };

        {
            SEARCH_FUNCTION_MAP.put(GeoConstants.GEO_SF_EQUALS, GEO_EQUALS);
            SEARCH_FUNCTION_MAP.put(GeoConstants.GEO_SF_DISJOINT, GEO_DISJOINT);
            SEARCH_FUNCTION_MAP.put(GeoConstants.GEO_SF_INTERSECTS, GEO_INTERSECTS);
            SEARCH_FUNCTION_MAP.put(GeoConstants.GEO_SF_TOUCHES, GEO_TOUCHES);
            SEARCH_FUNCTION_MAP.put(GeoConstants.GEO_SF_CONTAINS, GEO_CONTAINS);
            SEARCH_FUNCTION_MAP.put(GeoConstants.GEO_SF_OVERLAPS, GEO_OVERLAPS);
            SEARCH_FUNCTION_MAP.put(GeoConstants.GEO_SF_CROSSES, GEO_CROSSES);
            SEARCH_FUNCTION_MAP.put(GeoConstants.GEO_SF_WITHIN, GEO_WITHIN);
        }

    }


}
