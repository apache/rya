package org.apache.rya.indexing.pcj.matching;
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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.matching.AbstractExternalSetOptimizer;
import org.apache.rya.indexing.external.matching.BasicRater;
import org.apache.rya.indexing.external.matching.ExternalSetMatcher;
import org.apache.rya.indexing.external.matching.ExternalSetProvider;
import org.apache.rya.indexing.external.matching.QueryNodeListRater;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.apache.rya.indexing.external.matching.TopOfQueryFilterRelocator;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.mongodb.pcj.MongoPcjIndexSetProvider;
import org.apache.rya.indexing.pcj.matching.provider.AbstractPcjIndexSetProvider;
import org.apache.rya.indexing.pcj.matching.provider.AccumuloIndexSetProvider;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;

import com.google.common.base.Optional;;


/**
 * {@link QueryOptimizer} which matches {@link TupleExpr}s associated with
 * pre-computed queries to sub-queries of a given query. Each matched sub-query
 * is replaced by an {@link ExternalTupleSet} node to delegate that portion of
 * the query to the pre-computed query index.
 * <p>
 *
 * A query is be broken up into {@link QuerySegment}s. Pre-computed query
 * indices, or {@link ExternalTupleset} objects, are compared against the
 * {@link QueryModelNode}s in each QuerySegment. If an ExternalTupleSets nodes
 * match a subset of the given QuerySegments nodes, those nodes are replaced by
 * the ExternalTupleSet in the QuerySegment.
 *
 */
public class PCJOptimizer extends AbstractExternalSetOptimizer<ExternalTupleSet>implements Configurable {
    private static final PCJExternalSetMatcherFactory factory = new PCJExternalSetMatcherFactory();
    private AbstractPcjIndexSetProvider provider;
    private Configuration conf;
    private boolean init = false;

    public PCJOptimizer() {}

    public PCJOptimizer(final Configuration conf) {
        setConf(conf);
    }

    /**
     * This constructor is designed to be used for testing.  A more typical use
     * pattern is for a user to specify Accumulo connection details in a Configuration
     * file so that PCJs can be retrieved by an AccumuloIndexSetProvider.
     *
     * @param indices - user specified PCJs to match to query. (not null)
     * @param useOptimalPcj - optimize PCJ combos for matching
     * @param provider - The provider to use in this optimizer. (not null)
     */
    public PCJOptimizer(final List<ExternalTupleSet> indices, final boolean useOptimalPcj,
            final AbstractPcjIndexSetProvider provider) {
        checkNotNull(indices);
        checkNotNull(provider);
        conf = new Configuration();
        useOptimal = useOptimalPcj;
        this.provider = provider;
        init = true;
    }

    @Override
    public final void setConf(final Configuration conf) {
        checkNotNull(conf);
        if (!init) {
            try {
                this.conf = conf;
                useOptimal = ConfigUtils.getUseOptimalPCJ(conf);
                if (conf instanceof StatefulMongoDBRdfConfiguration) {
                    final StatefulMongoDBRdfConfiguration mongoConf = (StatefulMongoDBRdfConfiguration) conf;
                    provider = new MongoPcjIndexSetProvider(mongoConf);
                } else {
                    provider = new AccumuloIndexSetProvider(conf);
                }
            } catch (final Exception e) {
                throw new Error(e);
            }
            init = true;
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    /**
     * This method optimizes a specified query by matching subsets of it with
     * PCJ queries.
     *
     * @param tupleExpr - the query to be optimized
     * @param dataset - this value is ignored
     * @param bindings - this value is ignored
     */
    @Override
    public void optimize(TupleExpr tupleExpr, final Dataset dataset, final BindingSet bindings) {
        checkNotNull(tupleExpr);
        // first standardize query by pulling all filters to top of query if
        // they exist using TopOfQueryFilterRelocator
        tupleExpr = TopOfQueryFilterRelocator.moveFiltersToTop(tupleExpr);
        try {
            if (provider.size() > 0) {
                super.optimize(tupleExpr, null, null);
            } else {
                return;
            }
        } catch (final Exception e) {
            throw new RuntimeException("Could not populate Index Cache.", e);
        }
    }


    @Override
    protected ExternalSetMatcher<ExternalTupleSet> getMatcher(final QuerySegment<ExternalTupleSet> segment) {
        return factory.getMatcher(segment);
    }

    @Override
    protected ExternalSetProvider<ExternalTupleSet> getProvider() {
        return provider;
    }

    @Override
    protected Optional<QueryNodeListRater> getNodeListRater(final QuerySegment<ExternalTupleSet> segment) {
        return Optional.of(new BasicRater(segment.getOrderedNodes()));
    }
}
