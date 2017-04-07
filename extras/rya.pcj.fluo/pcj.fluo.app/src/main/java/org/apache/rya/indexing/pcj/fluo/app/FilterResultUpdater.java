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
package org.apache.rya.indexing.pcj.fluo.app;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.query.FilterMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.util.BindingSetUtil;
import org.apache.rya.indexing.pcj.fluo.app.util.RowKeyUtil;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.util.QueryEvaluationUtil;

import com.google.common.base.Optional;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import info.aduna.iteration.CloseableIteration;

/**
 * Updates the results of a Filter node when its child has added a new Binding
 * Set to its results.
 */
@DefaultAnnotation(NonNull.class)
public class FilterResultUpdater {

    private static final Logger log = Logger.getLogger(FilterResultUpdater.class);

    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();

    /**
     * A utility class used to search SPARQL queries for Filters.
     */
    private static final FilterFinder filterFinder = new FilterFinder();

    /**
     * Is used to evaluate the conditions of a {@link Filter}.
     */
    private static final EvaluationStrategyImpl evaluator = new EvaluationStrategyImpl(
            new TripleSource() {
                private final ValueFactory valueFactory = new ValueFactoryImpl();

                @Override
                public ValueFactory getValueFactory() {
                    return valueFactory;
                }

                @Override
                public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(
                        final Resource arg0,
                        final URI arg1,
                        final Value arg2,
                        final Resource... arg3) throws QueryEvaluationException {
                    throw new UnsupportedOperationException();
                }
            });

    /**
     * Updates the results of a Filter node when one of its child has added a
     * new Binding Set to its results.
     *
     * @param tx - The transaction all Fluo queries will use. (not null)
     * @param childBindingSet - A binding set that the query's child node has emitted. (not null)
     * @param filterMetadata - The metadata of the Filter whose results will be updated. (not null)
     * @throws Exception Something caused the update to fail.
     */
    public void updateFilterResults(
            final TransactionBase tx,
            final VisibilityBindingSet childBindingSet,
            final FilterMetadata filterMetadata) throws Exception {
        checkNotNull(tx);
        checkNotNull(childBindingSet);
        checkNotNull(filterMetadata);

        log.trace(
                "Transaction ID: " + tx.getStartTimestamp() + "\n" +
                "Filter Node ID: " + filterMetadata.getNodeId() + "\n" +
                "Binding Set:\n" + childBindingSet + "\n");

        // Parse the original query and find the Filter that represents filterId.
        final String sparql = filterMetadata.getOriginalSparql();
        final int indexWithinQuery = filterMetadata.getFilterIndexWithinSparql();
        final Optional<Filter> filter = filterFinder.findFilter(sparql, indexWithinQuery);

        // Evaluate whether the child BindingSet satisfies the filter's condition.
        final ValueExpr condition = filter.get().getCondition();
        if (isTrue(condition, childBindingSet)) {
            // Create the Filter's binding set from the child's.
            final VariableOrder filterVarOrder = filterMetadata.getVariableOrder();
            final BindingSet filterBindingSet = BindingSetUtil.keepBindings(filterVarOrder, childBindingSet);

            // Create the Row Key for the emitted binding set. It does not contain visibilities.
            final Bytes resultRow = RowKeyUtil.makeRowKey(filterMetadata.getNodeId(), filterVarOrder, filterBindingSet);

            // If this is a new binding set, then emit it.
            if(tx.get(resultRow, FluoQueryColumns.FILTER_BINDING_SET) == null) {
                final VisibilityBindingSet visBindingSet = new VisibilityBindingSet(filterBindingSet, childBindingSet.getVisibility());
                final Bytes nodeValueBytes = BS_SERDE.serialize(visBindingSet);

                log.trace(
                        "Transaction ID: " + tx.getStartTimestamp() + "\n" +
                        "New Binding Set: " + visBindingSet + "\n");

                tx.set(resultRow, FluoQueryColumns.FILTER_BINDING_SET, nodeValueBytes);
            }
        }
    }

    /**
     * Evaluate a {@link BindingSet} to see if it is accepted by a filter's condition.
     *
     * @param condition - The filter condition. (not null)
     * @param bindings - The binding set to evaluate. (not null)
     * @return {@code true} if the binding set is accepted by the filter; otherwise {@code false}.
     * @throws QueryEvaluationException The condition couldn't be evaluated.
     */
    private static boolean isTrue(final ValueExpr condition, final BindingSet bindings) throws QueryEvaluationException {
        try {
            final Value value = evaluator.evaluate(condition, bindings);
            return QueryEvaluationUtil.getEffectiveBooleanValue(value);
        } catch (final ValueExprEvaluationException e) {
            // XXX Hack: If filtering a statement that does not have the right bindings, return true.
            //           When would this ever come up? Should we actually return true?
            return true;
        }
    }
}