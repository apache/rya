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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;
import java.util.concurrent.Callable;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.fluo.app.query.CommonNodeMetadataImpl;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.openrdf.query.BindingSet;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Class that performs aggregation state lookups while maintaining a cache
 * of old lookups.  This class uses a single transaction to perform a number
 * of aggregation state lookups.
 */
public class AggregationStateManager {

    private String nodeId;
    private VariableOrder varOrder;
    private TransactionBase tx;
    private Cache<Bytes, BindingSet> aggregationStateCache;


    /**
     * Creates a new instance of AggregationStateManager
     * @param tx - Transaction for interacting with Fluo
     * @param aggregationStateMeta - aggregation state metadata, indicating the aggregation nodeId and VariableOrder
     */
    public AggregationStateManager(TransactionBase tx, CommonNodeMetadataImpl aggregationStateMeta) {
        checkNotNull(tx);
        checkNotNull(aggregationStateMeta);
        this.tx = tx;
        this.nodeId = checkNotNull(aggregationStateMeta.getNodeId());
        this.varOrder = checkNotNull(aggregationStateMeta.getVariableOrder());
        this.aggregationStateCache = CacheBuilder.newBuilder().maximumSize(1000).build();
    }

    /**
     * Fetches the aggregation state.
     * @param childBindingSet - BindingSet used to build rowId for aggregation state lookup
     * @param useCache - indicates whether to used cached aggregation states for lookup
     * @return - Optional BindingSet that represents the aggregation state (if present)
     */
    public Optional<BindingSet> fetchAggregationStateBs(VisibilityBindingSet childBindingSet) {
        checkNotNull(childBindingSet);
        Bytes rowId = RowKeyUtil.makeRowKey(nodeId, varOrder, childBindingSet);
        try {
            BindingSet bs = aggregationStateCache.get(rowId, new Callable<BindingSet>() {
                @Override
                public BindingSet call() throws Exception {
                    Optional<BindingSet> stateBs = AggregationStateUtil.fetchAggregationState(tx, nodeId, varOrder, childBindingSet);
                    if (stateBs.isPresent()) {
                        return stateBs.get();
                    } else {
                        throw new Exception("Unable to find aggregation state for row key: " + rowId);
                    }
                }
            });
            return Optional.of(bs);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /**
     * Verifies that the childBindingSet matches the aggregation state
     * @param childBindingSet - BindingSet whose aggregation state will be verified
     * @param useCache - indicates whether to use cached aggregation states
     * @return - true if the indicated BindingSet has a valid aggregation state and false otherwise
     */
    public boolean checkAggregationState(VisibilityBindingSet childBindingSet) {
        checkNotNull(childBindingSet);

        Optional<BindingSet> stateBs = fetchAggregationStateBs(childBindingSet);
        if(stateBs.isPresent()) {
            return AggregationStateUtil.checkAggregationState(stateBs.get(), childBindingSet);
        }
        return true;
    }

}
