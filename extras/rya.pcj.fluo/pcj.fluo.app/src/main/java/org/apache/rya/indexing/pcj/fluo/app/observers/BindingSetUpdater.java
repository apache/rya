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
package org.apache.rya.indexing.pcj.fluo.app.observers;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.log4j.Logger;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.fluo.app.AggregationResultUpdater;
import org.apache.rya.indexing.pcj.fluo.app.ConstructQueryResultUpdater;
import org.apache.rya.indexing.pcj.fluo.app.FilterResultUpdater;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.PeriodicQueryUpdater;
import org.apache.rya.indexing.pcj.fluo.app.ProjectionResultUpdater;
import org.apache.rya.indexing.pcj.fluo.app.QueryResultUpdater;
import org.apache.rya.indexing.pcj.fluo.app.query.AggregationMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.ConstructQueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FilterMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataCache;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.MetadataCacheSupplier;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicQueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.ProjectionMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Notified when the results of a node have been updated to include a new Binding
 * Set. This observer updates its parent if the new Binding Set effects the parent's
 * results.
 */
@DefaultAnnotation(NonNull.class)
public abstract class BindingSetUpdater extends AbstractObserver {
    private static final Logger log = Logger.getLogger(BindingSetUpdater.class);
    // DAO
    protected final FluoQueryMetadataCache queryDao = MetadataCacheSupplier.getOrCreateCache();

    // Updaters
    private final JoinResultUpdater joinUpdater = new JoinResultUpdater();
    private final FilterResultUpdater filterUpdater = new FilterResultUpdater();
    private final QueryResultUpdater queryUpdater = new QueryResultUpdater();
    private final AggregationResultUpdater aggregationUpdater = new AggregationResultUpdater();
    private final ConstructQueryResultUpdater constructUpdater = new ConstructQueryResultUpdater();
    private final ProjectionResultUpdater projectionUpdater = new ProjectionResultUpdater();
    private final PeriodicQueryUpdater periodicQueryUpdater = new PeriodicQueryUpdater();

    @Override
    public abstract ObservedColumn getObservedColumn();

    /**
     * Create an {@link Observation} that defines the work that needs to be done.
     *
     * @param tx - The Fluo transaction being used for the observer notification. (not null)
     * @param row - The row that triggered the notification. (not null)
     * @return An {@link Observation} that defines the work that needs to be done.
     * @throws Exception A problem caused this method to fail.
     */
    public abstract Observation parseObservation(TransactionBase tx, Bytes row) throws Exception;

    @Override
    public final void process(final TransactionBase tx, final Bytes row, final Column col) {
        checkNotNull(tx);
        checkNotNull(row);
        checkNotNull(col);

        final Observation observation;
        try {
            observation = parseObservation(tx, row);
        } catch (final Exception e) {
            log.error("Unable to parse an Observation from a Row and Column pair, so this notification will be skipped. " +
                    "Row: " + row + " Column: " + col, e);
            return;
        }

        final String observedNodeId = observation.getObservedNodeId();
        final VisibilityBindingSet observedBindingSet = observation.getObservedBindingSet();
        final String parentNodeId = observation.getParentId();

        // Figure out which node needs to handle the new metadata.
        final NodeType parentNodeType = NodeType.fromNodeId(parentNodeId).get();
        switch(parentNodeType) {
            case QUERY:
                final QueryMetadata parentQuery = queryDao.readQueryMetadata(tx, parentNodeId);
                try {
                    queryUpdater.updateQueryResults(tx, observedBindingSet, parentQuery);
                } catch (final Exception e) {
                    throw new RuntimeException("Could not process a Query node.", e);
                }
                break;

            case PROJECTION:
                final ProjectionMetadata projectionQuery = queryDao.readProjectionMetadata(tx, parentNodeId);
                try {
                    projectionUpdater.updateProjectionResults(tx, observedBindingSet, projectionQuery);
                } catch (final Exception e) {
                    throw new RuntimeException("Could not process a Query node.", e);
                }
                break;

            case CONSTRUCT:
                final ConstructQueryMetadata constructQuery = queryDao.readConstructQueryMetadata(tx, parentNodeId);
                try{
                    constructUpdater.updateConstructQueryResults(tx, observedBindingSet, constructQuery);
                } catch (final Exception e) {
                    throw new RuntimeException("Could not process a Query node.", e);
                }
                break;

            case FILTER:
                final FilterMetadata parentFilter = queryDao.readFilterMetadata(tx, parentNodeId);
                try {
                    filterUpdater.updateFilterResults(tx, observedBindingSet, parentFilter);
                } catch (final Exception e) {
                    throw new RuntimeException("Could not process a Filter node.", e);
                }
                break;

            case JOIN:
                final JoinMetadata parentJoin = queryDao.readJoinMetadata(tx, parentNodeId);
                try {
                    joinUpdater.updateJoinResults(tx, observedNodeId, observedBindingSet, parentJoin);
                } catch (final Exception e) {
                    throw new RuntimeException("Could not process a Join node.", e);
                }
                break;

            case PERIODIC_QUERY:
                final PeriodicQueryMetadata parentPeriodicQuery = queryDao.readPeriodicQueryMetadata(tx, parentNodeId);
                try{
                    periodicQueryUpdater.updatePeriodicBinResults(tx, observedBindingSet, parentPeriodicQuery);
                } catch(Exception e) {
                    throw new RuntimeException("Could not process PeriodicBin node.", e);
                }
                break;

            case AGGREGATION:
                final AggregationMetadata parentAggregation = queryDao.readAggregationMetadata(tx, parentNodeId);
                try {
                    aggregationUpdater.updateAggregateResults(tx, observedBindingSet, parentAggregation);
                } catch (final Exception e) {
                    throw new RuntimeException("Could not process an Aggregation node.", e);
                }
                break;


            default:
                throw new IllegalArgumentException("The parent node's NodeType must be of type Aggregation, Projection, ConstructQuery, Filter, Join, PeriodicBin or Query, but was " + parentNodeType);
        }
    }

    /**
     * Defines who just emitted a new Binding Set result, the Binding Set itself,
     * and which node must now handle it.
     */
    public static final class Observation {

        private final String observedNodeId;
        private final VisibilityBindingSet observedBindingSet;
        private final String parentNodeId;

        /**
         * Creates an instance of {@link Observation}.
         *
         * @param observedNodeId - The Node ID that just emitted a new Binding Set. (not null)
         * @param observedBindingSet - A Binding Set that was just emitted. (not null)
         * @param parentNodeId - The Node ID of the node that must handle the new Binding Set input. (not null)
         */
        public Observation(
                final String observedNodeId,
                final VisibilityBindingSet observedBindingSet,
                final String parentNodeId) {
            this.observedNodeId = checkNotNull(observedNodeId);
            this.observedBindingSet = checkNotNull(observedBindingSet);
            this.parentNodeId = checkNotNull(parentNodeId);
        }

        /**
         * @return The Node ID that just emitted a new Binding Set.
         */
        public String getObservedNodeId() {
            return observedNodeId;
        }

        /**
         * @return A Binding Set that was just emitted.
         */
        public VisibilityBindingSet getObservedBindingSet() {
            return observedBindingSet;
        }

        /**
         * @return The Node ID of the node that must handle the new Binding Set input.
         */
        public String getParentId() {
            return parentNodeId;
        }
    }
}