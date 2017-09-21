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
package org.apache.rya.indexing.pcj.fluo.app.util;

import java.util.Optional;
import java.util.Set;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.fluo.app.AggregationResultUpdater.AggregationStateSerDe;
import org.apache.rya.indexing.pcj.fluo.app.AggregationResultUpdater.ObjectSerializationAggregationStateSerDe;
import org.apache.rya.indexing.pcj.fluo.app.query.CommonNodeMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.CommonNodeMetadataImpl;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.StateNodeMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.query.BindingSet;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

/**
 * Utility class to check if a BindingSet is consistent with a node's aggregation state before writing.
 */
public class AggregationStateUtil {

    private static final AggregationStateSerDe AGG_STATE_SERDE = new ObjectSerializationAggregationStateSerDe();

    /**
     * Determines if the provided {@link VisibilityBindingSet} matches the aggregation state provided by the
     * {@link UnaryNodeMetadata}.
     * 
     * @param childBindingSet - BindingSet whose aggregation state is compared to state of unary node
     * @param metadata - Metadata containing the current aggregation state for the node
     * @return - true if childBindingSet matches aggregation state and false otherwise
     */
    public static boolean checkAggregationState(final TransactionBase tx, final VisibilityBindingSet childBindingSet,
            final StateNodeMetadata metadata) {
        Optional<CommonNodeMetadataImpl> aggregationState = metadata.getStateMetadata();
        if (aggregationState.isPresent()) {

            CommonNodeMetadata stateMetadata = aggregationState.get();
            Optional<BindingSet> bindingSetState = fetchAggregationState(tx, stateMetadata.getNodeId(), stateMetadata.getVariableOrder(), childBindingSet);

            if (bindingSetState.isPresent()) {
                return checkAggregationState(bindingSetState.get(), childBindingSet);
            }

        }
        return true;
    }

    /**
     * Compares the aggregation state of a given BindingSet with the aggregation BindingSet
     * @param aggregationStateBs - BindingSet indicating the aggregation state
     * @param childBindingSet - BindingSet that represents a candidate result
     * @return - true if the candidate BindingSet is consistent with the aggregation state BindingSet and false otherwise
     */
    public static boolean checkAggregationState(BindingSet aggregationStateBs, BindingSet childBindingSet) {
        Set<String> commonVars = Sets.intersection(aggregationStateBs.getBindingNames(), childBindingSet.getBindingNames());
        for (String var: commonVars) {
            if (!Objects.equal(aggregationStateBs.getBinding(var), childBindingSet.getBinding(var))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Retrieves the BindingSet that indicates the aggregation state
     * @param tx - transaction for interacting with Fluo    
     * @param nodeId - id of the aggregation node whose state will be retrieved
     * @param varOrder - VariableOrder of the aggregation node for creating the rowId to perform the state lookup
     * @param childBindingSet - candidate result - needed to build the rowId to performt the state lookup
     * @return - Optional containing the aggregation state BindingSet if it exists
     */
    public static Optional<BindingSet> fetchAggregationState(TransactionBase tx, String nodeId, VariableOrder varOrder,
            BindingSet childBindingSet) {
        final Bytes rowId = RowKeyUtil.makeRowKey(nodeId, varOrder, childBindingSet);
        Optional<Bytes> stateBytes = Optional.ofNullable(tx.get(rowId, FluoQueryColumns.AGGREGATION_BINDING_SET));

        if (stateBytes.isPresent()) {
            return Optional.of(AGG_STATE_SERDE.deserialize(stateBytes.get().toArray()).getBindingSet());
        }

        return Optional.empty();
    }

}
