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
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Metadata that is required for periodic queries in the Rya Fluo Application.
 * If a periodic query is registered with the Rya Fluo application, the BindingSets
 * are placed into temporal bins according to whether they occur within the window of
 * a period's ending time.  This Metadata is used to create a Bin Id, which is equivalent
 * to the period's ending time, to be inserted into each BindingSet that occurs within that
 * bin.  This is to allow the AggregationUpdater to aggregate the bins by grouping on the
 * Bin Id.
 *
 */
public class PeriodicQueryMetadata extends StateNodeMetadata {

    private String parentNodeId;
    private String childNodeId;
    private long windowSize;
    private long period;
    private TimeUnit unit;
    private String temporalVariable;

    /**
     * Constructs an instance of PeriodicQueryMetadata
     * @param nodeId - id of periodic query node
     * @param varOrder - variable order indicating the order the BindingSet results are written in
     * @param stateMetadata - Optional containing information about the aggregation state that this node depends on
     * @param parentNodeId - id of parent node
     * @param childNodeId - id of child node
     * @param windowSize - size of window used for filtering
     * @param period - period size that indicates frequency of notifications
     * @param unit - TimeUnit corresponding to window and period
     * @param temporalVariable - temporal variable that periodic conditions are applied to
     */
    public PeriodicQueryMetadata(String nodeId, VariableOrder varOrder, Optional<CommonNodeMetadataImpl> stateMetadata, String parentNodeId, String childNodeId, long windowSize, long period,
            TimeUnit unit, String temporalVariable) {
        super(nodeId, varOrder, stateMetadata);
        this.parentNodeId = Preconditions.checkNotNull(parentNodeId);
        this.childNodeId = Preconditions.checkNotNull(childNodeId);
        this.temporalVariable = Preconditions.checkNotNull(temporalVariable);
        this.unit = Preconditions.checkNotNull(unit);
        Preconditions.checkArgument(period > 0);
        Preconditions.checkArgument(windowSize >= period);

        this.windowSize = windowSize;
        this.period = period;
    }

    /**
     * @return id of parent for navigating query
     */
    public String getParentNodeId() {
        return parentNodeId;
    }

    /**
     *
     * @return id of child for navigating query
     */
    public String getChildNodeId() {
        return childNodeId;
    }

    /**
     *
     * @return temporal variable used for filtering events
     */
    public String getTemporalVariable() {
        return temporalVariable;
    }

    /**
     * @return window duration in millis
     */
    public long getWindowSize() {
        return windowSize;
    }

    /**
     * @return period duration in millis
     */
    public long getPeriod() {
        return period;
    }

    /**
     * @return {@link TimeUnit} for window duration and period duration
     */
    public TimeUnit getUnit() {
        return unit;
    }


    /**
     * @return {@link Builder} for chaining method calls to construct an instance of PeriodicQueryMetadata.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.getNodeId(), super.getVariableOrder(), super.getStateMetadata(), childNodeId, parentNodeId, temporalVariable, period, windowSize, unit);
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }

        if (o instanceof PeriodicQueryMetadata) {
            if (super.equals(o)) {
                PeriodicQueryMetadata metadata = (PeriodicQueryMetadata) o;
                return new EqualsBuilder().append(childNodeId, metadata.childNodeId).append(parentNodeId, metadata.parentNodeId)
                        .append(windowSize, metadata.windowSize).append(period, metadata.period)
                        .append(unit, metadata.unit).append(temporalVariable, metadata.temporalVariable).isEquals();
            }
            return false;
        }

        return false;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("PeriodicQueryMetadata {\n")
                .append("    Node ID: " + super.getNodeId() + "\n")
                .append("    Variable Order: " + super.getVariableOrder() + "\n")
                .append("    State Metadata: " + super.getStateMetadata() + "\n")
                .append("    Parent Node ID: " + parentNodeId + "\n")
                .append("    Child Node ID: " + childNodeId + "\n")
                .append("    Period: " + period + "\n")
                .append("    Window Size: " + windowSize + "\n")
                .append("    Time Unit: " + unit + "\n")
                .append("    Temporal Variable: " + temporalVariable + "\n")
                .append("}")
                .toString();
    }


    /**
     * Builder for chaining method calls to construct an instance of PeriodicQueryMetadata.
     */
    public static class Builder implements CommonNodeMetadata.Builder {

        private String nodeId;
        private VariableOrder varOrder;
        private CommonNodeMetadataImpl state;
        private String parentNodeId;
        private String childNodeId;
        private long windowSize;
        private long period;
        private TimeUnit unit;
        public String temporalVariable;

        public Builder setNodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        /**
         *
         * @return id of of this node
         */
        @Override
        public String getNodeId() {
            return nodeId;
        }

        /**
         * Set the {@link VariableOrder}
         * @param varOrder to indicate order that results will be written in
         * @return Builder for chaining methods calls
         */
        public Builder setVarOrder(VariableOrder varOrder) {
            this.varOrder = varOrder;
            return this;
        }


        /**
         * Returns {@link VariableOrder}
         * @return VariableOrder that indicates order that results are written in
         */
        @Override
        public VariableOrder getVariableOrder() {
            return varOrder;
        }

        /**
         * Sets id of parent node
         * @param parentNodeId
         * @return Builder for chaining methods calls
         */
        public Builder setParentNodeId(String parentNodeId) {
            this.parentNodeId = parentNodeId;
            return this;
        }

        /**
         * @return id of parent node
         */
        public String getParentNodeId() {
            return parentNodeId;
        }

        /**
         * Set id of child node
         * @param childNodeId
         * @return Builder for chaining methods calls
         */
        public Builder setChildNodeId(String childNodeId) {
            this.childNodeId = childNodeId;
            return this;
        }

        public String getChildNodeId() {
            return childNodeId;
        }

        /**
         * Sets window size for periodic query
         * @param windowSize
         * @return Builder for chaining methods calls
         */
        public Builder setWindowSize(long windowSize) {
            this.windowSize = windowSize;
            return this;
        }

        /**
         * Sets period for periodic query
         * @param period
         * @return Builder for chaining methods calls
         */
        public Builder setPeriod(long period) {
            this.period = period;
            return this;
        }

        /**
         * Sets time unit of window and period for periodic query
         * @param unit
         * @return Builder for chaining methods calls
         */
        public Builder setUnit(TimeUnit unit) {
            this.unit = unit;
            return this;
        }

        /**
         * Sets the Aggregation State.
         * @param state - Aggregation State indicating current value of Aggregation
         * @return This builder so that method invocations may be chained.
         */
        public Builder setStateMetadata(CommonNodeMetadataImpl state) {
            this.state = state;
            return this;
        }

        /**
         * Returns the aggregation state metadata for this node if it exists
         * @return - Optional containing the aggregation station
         */
        public Optional<CommonNodeMetadataImpl> getStateMetadata() {
            return Optional.ofNullable(state);
        }

        /**
         * Indicate which variable in BindingSet results is the temporal variable that periodic
         * Conditions should be applied to
         * @param temporalVariable
         * @return Builder for chaining methods calls
         */
        public Builder setTemporalVariable(String temporalVariable) {
            this.temporalVariable = temporalVariable;
            return this;
        }

        /**
         * @return PeriodicQueryMetadata constructed from parameters passed to this Builder
         */
        public PeriodicQueryMetadata build() {
            return new PeriodicQueryMetadata(nodeId, varOrder, Optional.ofNullable(state), parentNodeId, childNodeId, windowSize, period, unit, temporalVariable);
        }
    }

}
