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

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.FilterResultUpdater;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.QueryResultUpdater;
import org.apache.rya.indexing.pcj.fluo.app.query.FilterMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;

/**
 * Notified when the results of a node have been updated to include a new Binding
 * Set. This observer updates its parent if the new Binding Set effects the parent's
 * results.
 */
@ParametersAreNonnullByDefault
public abstract class BindingSetUpdater extends AbstractObserver {

    // DAO
    private final FluoQueryMetadataDAO queryDao = new FluoQueryMetadataDAO();

    // Updaters
    private final JoinResultUpdater joinUpdater = new JoinResultUpdater();
    private final FilterResultUpdater filterUpdater = new FilterResultUpdater();
    private final QueryResultUpdater queryUpdater = new QueryResultUpdater();

    @Override
    public abstract ObservedColumn getObservedColumn();

    /**
     * Create an {@link Observation} that defines the work that needs to be done.
     *
     * @param tx - The Fluo transaction being used for the observer notification. (not null)
     * @param parsedRow - The RowID parsed into a Binding Set and Node ID. (not null)
     * @return An {@link Observation} that defines the work that needs to be done.
     */
    public abstract Observation parseObservation(TransactionBase tx, final BindingSetRow parsedRow);

    @Override
    public final void process(final TransactionBase tx, final Bytes row, final Column col) {
        checkNotNull(tx);
        checkNotNull(row);
        checkNotNull(col);

        final String bindingSetString = tx.get(row, col).toString();
        final Observation observation = parseObservation( tx, new BindingSetRow(BindingSetRow.make(row).getNodeId(), bindingSetString) );
        final String observedNodeId = observation.getObservedNodeId();
        final VisibilityBindingSet observedBindingSet = observation.getObservedBindingSet();
        final String parentNodeId = observation.getParentId();

        // Figure out which node needs to handle the new metadata.
        final NodeType parentNodeType = NodeType.fromNodeId(parentNodeId).get();
        switch(parentNodeType) {
            case QUERY:
                final QueryMetadata parentQuery = queryDao.readQueryMetadata(tx, parentNodeId);
                queryUpdater.updateQueryResults(tx, observedBindingSet, parentQuery);
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
                } catch (final BindingSetConversionException e) {
                    throw new RuntimeException("Could not process a Join node.", e);
                }
                break;

            default:
                throw new IllegalArgumentException("The parent node's NodeType must be of type Filter, Join, or Query, but was " + parentNodeType);
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