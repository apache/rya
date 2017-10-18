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

import static java.util.Objects.requireNonNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.AGGREGATION_PREFIX;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.rya.api.function.aggregation.AggregationState;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.fluo.app.AggregationResultUpdater.AggregationStateSerDe;
import org.apache.rya.indexing.pcj.fluo.app.AggregationResultUpdater.ObjectSerializationAggregationStateSerDe;
import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.query.AggregationMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.eclipse.rdf4j.query.BindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Notified when the results of an Aggregation have been updated to include a new
 * {@link BindingSet} value. This observer updates its parent if the new Binding Set
 * effects the parent's results.
 */
@DefaultAnnotation(NonNull.class)
public class AggregationObserver extends BindingSetUpdater {

    private static final AggregationStateSerDe STATE_SERDE = new ObjectSerializationAggregationStateSerDe();

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.AGGREGATION_BINDING_SET, NotificationType.STRONG);
    }

    @Override
    public Observation parseObservation(final TransactionBase tx, final Bytes row) {
        requireNonNull(tx);
        requireNonNull(row);

        // Make nodeId and fetch the Aggregation node's metadata.
        final String nodeId = BindingSetRow.makeFromShardedRow(Bytes.of(AGGREGATION_PREFIX), row).getNodeId();
        final AggregationMetadata metadata = queryDao.readAggregationMetadata(tx, nodeId);

        // Read the Visibility Binding Set from the value.
        final Bytes stateBytes = tx.get(row, FluoQueryColumns.AGGREGATION_BINDING_SET);
        final AggregationState state = STATE_SERDE.deserialize( stateBytes.toArray() );
        final VisibilityBindingSet aggBindingSet = new VisibilityBindingSet(state.getBindingSet(), state.getVisibility());

        // Figure out which node needs to handle the new metadata.
        final String parentNodeId = metadata.getParentNodeId();

        return new Observation(nodeId, aggBindingSet, parentNodeId);
    }
}
