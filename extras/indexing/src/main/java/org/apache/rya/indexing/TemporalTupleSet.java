package org.apache.rya.indexing;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.joda.time.DateTime;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryModelVisitor;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

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

//Indexing Node for temporal expressions to be inserted into execution plan
//to delegate temporal portion of query to temporal index
public class TemporalTupleSet extends ExternalTupleSet {

    private final Configuration conf;
    private final TemporalIndexer temporalIndexer;
    private final IndexingExpr filterInfo;

    public TemporalTupleSet(final IndexingExpr filterInfo, final TemporalIndexer temporalIndexer) {
        this.filterInfo = filterInfo;
        this.temporalIndexer = temporalIndexer;
        conf = temporalIndexer.getConf();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getBindingNames() {
        return filterInfo.getBindingNames();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note that we need a deep copy for everything that (during optimizations)
     * can be altered via {@link #visitChildren(QueryModelVisitor)}
     */
    @Override
    public TemporalTupleSet clone() {
        return new TemporalTupleSet(filterInfo, temporalIndexer);
    }

    @Override
    public double cardinality() {
        return 0.0; // No idea how the estimate cardinality here.
    }

    @Override
    public String getSignature() {

        return "(TemporalTuple Projection) " + "variables: " + Joiner.on(", ").join(getBindingNames()).replaceAll("\\s+", " ");
    }

    @Override
    public boolean equals(final Object other) {
        if (other == this) {
            return true;
        }
        if (!(other instanceof TemporalTupleSet)) {
            return false;
        }
        final TemporalTupleSet arg = (TemporalTupleSet) other;
        return filterInfo.equals(arg.filterInfo);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31*result + filterInfo.hashCode();

        return result;
    }

    /**
     * Returns an iterator over the result set associated with contained IndexingExpr.
     * <p>
     * Should be thread-safe (concurrent invocation {@link OfflineIterable} this
     * method can be expected with some query evaluators.
     */
    @Override
    public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(final BindingSet bindings)
            throws QueryEvaluationException {
        final URI funcURI = filterInfo.getFunction();
        final SearchFunction searchFunction = new TemporalSearchFunctionFactory(conf, temporalIndexer).getSearchFunction(funcURI);

        if(filterInfo.getArguments().length > 1) {
            throw new IllegalArgumentException("Index functions do not support more than two arguments.");
        }

        final String queryText = filterInfo.getArguments()[0].stringValue();
        return IteratorFactory.getIterator(filterInfo.getSpConstraint(), bindings, queryText, searchFunction);
    }

    //returns appropriate search function for a given URI
    //search functions used by TemporalIndexer to query Temporal Index
    public static class TemporalSearchFunctionFactory  {
        private final Map<URI, SearchFunction> SEARCH_FUNCTION_MAP = Maps.newHashMap();
        private final TemporalIndexer temporalIndexer;

        public TemporalSearchFunctionFactory(final Configuration conf, final TemporalIndexer temporalIndexer) {
            this.temporalIndexer = temporalIndexer;
        }

        /**
         * Get a {@link TemporalSearchFunction} for a give URI.
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

        private final SearchFunction TEMPORAL_InstantAfterInstant = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String searchTerms,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                final TemporalInstant queryInstant = new TemporalInstantRfc3339(DateTime.parse(searchTerms));
                return temporalIndexer.queryInstantAfterInstant(queryInstant, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantAfterInstant";
            };
        };
        private final SearchFunction TEMPORAL_InstantBeforeInstant = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String searchTerms,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                final TemporalInstant queryInstant = new TemporalInstantRfc3339(DateTime.parse(searchTerms));
                return temporalIndexer.queryInstantBeforeInstant(queryInstant, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantBeforeInstant";
            };
        };

        private final SearchFunction TEMPORAL_InstantEqualsInstant = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String searchTerms,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                final TemporalInstant queryInstant = new TemporalInstantRfc3339(DateTime.parse(searchTerms));
                return temporalIndexer.queryInstantEqualsInstant(queryInstant, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantEqualsInstant";
            };
        };

        private final SearchFunction TEMPORAL_InstantAfterInterval = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String searchTerms,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                final TemporalInterval queryInterval = TemporalInstantRfc3339.parseInterval(searchTerms);
                return temporalIndexer.queryInstantAfterInterval(queryInterval, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantAfterInterval";
            };
        };

        private final SearchFunction TEMPORAL_InstantBeforeInterval = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String searchTerms,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                final TemporalInterval queryInterval = TemporalInstantRfc3339.parseInterval(searchTerms);
                return temporalIndexer.queryInstantBeforeInterval(queryInterval, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantBeforeInterval";
            };
        };

        private final SearchFunction TEMPORAL_InstantInsideInterval = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String searchTerms,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                final TemporalInterval queryInterval = TemporalInstantRfc3339.parseInterval(searchTerms);
                return temporalIndexer.queryInstantInsideInterval(queryInterval, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantInsideInterval";
            };
        };

        private final SearchFunction TEMPORAL_InstantHasBeginningInterval = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String searchTerms,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                final TemporalInterval queryInterval = TemporalInstantRfc3339.parseInterval(searchTerms);
                return temporalIndexer.queryInstantHasBeginningInterval(queryInterval, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantHasBeginningInterval";
            };
        };

        private final SearchFunction TEMPORAL_InstantHasEndInterval = new SearchFunction() {
            @Override
            public CloseableIteration<Statement, QueryEvaluationException> performSearch(final String searchTerms,
                    final StatementConstraints contraints) throws QueryEvaluationException {
                final TemporalInterval queryInterval = TemporalInstantRfc3339.parseInterval(searchTerms);
                return temporalIndexer.queryInstantHasEndInterval(queryInterval, contraints);
            }

            @Override
            public String toString() {
                return "TEMPORAL_InstantHasEndInterval";
            };
        };

        {
            final String TEMPORAL_NS = "tag:rya-rdf.org,2015:temporal#";

            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"after"), TEMPORAL_InstantAfterInstant);
            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"before"), TEMPORAL_InstantBeforeInstant);
            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"equals"), TEMPORAL_InstantEqualsInstant);

            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"beforeInterval"), TEMPORAL_InstantBeforeInterval);
            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"afterInterval"), TEMPORAL_InstantAfterInterval);
            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"insideInterval"), TEMPORAL_InstantInsideInterval);
            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"hasBeginningInterval"),
                    TEMPORAL_InstantHasBeginningInterval);
            SEARCH_FUNCTION_MAP.put(new URIImpl(TEMPORAL_NS+"hasEndInterval"), TEMPORAL_InstantHasEndInterval);
        }
    }
}