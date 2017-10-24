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
package org.apache.rya.indexing.pcj.fluo.app.query;

import java.util.Optional;

import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Objects;

/**
 * Metadata class that is meant to be extended by any metadata node that could have Aggregation State.
 *
 */
public class StateNodeMetadata extends CommonNodeMetadata {

    private Optional<CommonNodeMetadataImpl> stateMetadata;

    /**
     * Creates a StateNodeMetadata object with empty aggregation metadata.
     * @param nodeId
     * @param varOrder
     */
    public StateNodeMetadata(String nodeId, VariableOrder varOrder) {
        this(nodeId, varOrder, Optional.empty());
    }

    /**
     * Creates a StateNodeMetadata object with indicated aggregation metadata.
     * @param nodeId
     * @param varOrder
     * @param aggregationStateMetadata - aggregation metadata to look up aggregation state
     */
    public StateNodeMetadata(String nodeId, VariableOrder varOrder, Optional<CommonNodeMetadataImpl> aggregationStateMetadata) {
        super(nodeId, varOrder);
        this.stateMetadata = aggregationStateMetadata;
    }

    /**
     * @return - Optional containing aggregation metadata to look up aggregation state (if it exists)
     */
    public Optional<CommonNodeMetadataImpl> getStateMetadata() {
        return stateMetadata;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), stateMetadata);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof StateNodeMetadata) {
            if (super.equals(o)) {
                final StateNodeMetadata metadata = (StateNodeMetadata) o;
                return Objects.equal(this.stateMetadata, metadata.stateMetadata);
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return new StringBuilder().append("State Metadata {\n").append("    Node ID: " + super.getNodeId() + "\n")
                .append("    Variable Order: " + super.getVariableOrder() + "\n").append("    State Metadata: " + stateMetadata + "\n")
                .append("}").toString();
    }

}
