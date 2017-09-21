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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Optional;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Objects;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

/**
 * Metadata that is specific to a Projection.
 */
@Immutable
@DefaultAnnotation(NonNull.class)
public class ProjectionMetadata extends StateNodeMetadata {

    private final String childNodeId;
    private final String parentNodeId;
    private final VariableOrder projectedVars;

    /**
     * Constructs an instance of {@link ProjectionMetadata}.
     *
     * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
     * @param varOrder - The order in which binding values are written in the row to identify this result. (not null)
     * @param stateMetadata - Optional containing information about the aggregation state that this node depends on. (not null)
     * @param childNodeId - The node whose results are projected to the query's SELECT variables. (not null)
     * @param parentNodeId - The parent node of this projection (not null)
     * @param projectedVars - The variables that the results are projected onto (not null)
     */
    public ProjectionMetadata(
            final String nodeId,
            final VariableOrder varOrder,
            final Optional<CommonNodeMetadataImpl> stateMetadata,
            final String childNodeId,
            final String parentNodeId,
            final VariableOrder projectedVars) {
        super(nodeId, varOrder, stateMetadata);
        this.childNodeId = checkNotNull(childNodeId);
        this.parentNodeId = checkNotNull(parentNodeId);
        this.projectedVars = checkNotNull(projectedVars);
    }

    /**
     * @return The node whose results are projected to the query's SELECT variables.l
     */
    public String getChildNodeId() {
        return childNodeId;
    }
    
    /**
     * @return The parent node of this projection node
     */
    public String getParentNodeId() {
        return parentNodeId;
    }
    
    /**
     * @return The variables that results are projected onto
     */
    public VariableOrder getProjectedVars() {
        return projectedVars;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                super.getNodeId(),
                super.getVariableOrder(),
                super.getStateMetadata(),
                projectedVars,
                childNodeId,
                parentNodeId);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }

        if(o instanceof ProjectionMetadata) {
            if(super.equals(o)) {
                final ProjectionMetadata projectionMetadata = (ProjectionMetadata)o;
                return new EqualsBuilder()
                        .append(childNodeId, projectionMetadata.childNodeId)
                        .append(parentNodeId, projectionMetadata.parentNodeId)
                        .append(projectedVars, projectionMetadata.projectedVars)
                        .isEquals();
            }
            return false;
        }

        return false;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("ProjectionMetadata {\n")
                .append("    Node ID: " + super.getNodeId() + "\n")
                .append("    Projection Variables: " + projectedVars + "\n")
                .append("    Variable Order: " + super.getVariableOrder() + "\n")
                .append("    State Metadata: " + super.getStateMetadata() + "\n")
                .append("    Child Node ID: " + childNodeId + "\n")
                .append("    Parent Node ID: " + parentNodeId + "\n")
                .append("}")
                .toString();
    }

    /**
     * Creates a new {@link Builder} for this class.
     *
     * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
     * @return A new {@link Builder} for this class.
     */
    public static Builder builder(final String nodeId) {
        return new Builder(nodeId);
    }

    /**
     * Builds instances of {@link ProjectionMetadata}.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class Builder implements CommonNodeMetadata.Builder {

        private String nodeId;
        private VariableOrder varOrder;
        private CommonNodeMetadataImpl state;
        private String childNodeId;
        private String parentNodeId;
        private VariableOrder projectedVars;

        /**
         * Constructs an instance of {@link Builder}.
         *
         * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
         */
        public Builder(final String nodeId) {
            this.nodeId = checkNotNull(nodeId);
        }
        
        public String getNodeId() {
            return nodeId;
        }
        
        /**
         * Set the variable order of binding sets that are emitted by this node.
         *
         * @param varOrder - The order in which result values are written to the row to identify this result
         * @return This builder so that method invocations may be chained.
         */
        public Builder setVarOrder(@Nullable final VariableOrder varOrder) {
            this.varOrder = varOrder;
            return this;
        }
        
        /**
         * @return the variable order of binding sets that are emitted by this node
         */
        public VariableOrder getVariableOrder() {
            return varOrder;
        }

        /**
         * Set the node whose results are projected to the query's SELECT variables.
         *
         * @param childNodeId - The node whose results are projected to the query's SELECT variables.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setChildNodeId(@Nullable final String childNodeId) {
            this.childNodeId = childNodeId;
            return this;
        }
        
        public String getChildNodeId() {
            return childNodeId;
        }
        
        /**
         * Set the the parent node of this projection node.
         *
         * @param parentNodeId - The parent node of this projection node
         * @return This builder so that method invocations may be chained.
         */
        public Builder setParentNodeId(@Nullable final String parentNodeId) {
            this.parentNodeId = parentNodeId;
            return this;
        }
        
        public String getParentNodeId() {
            return parentNodeId;
        }
        
        /**
         * @param varOrder - Variables that results are projected onto
         * @return This builder so that method invocations may be chained.
         */
        public Builder setProjectedVars(VariableOrder projectedVars) {
            this.projectedVars = projectedVars;
            return this;
        }
        
        /**
         * @return The variables that results are projected onto
         */
        public VariableOrder getProjectionVars() {
            return projectedVars;
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
        
        public Optional<CommonNodeMetadataImpl> getStateMetadata() {
            return Optional.ofNullable(state);
        }

        /**
         * @return An instance of {@link ProjectionMetadata} built using this builder's values.
         */
        public ProjectionMetadata build() {
            return new ProjectionMetadata(nodeId, varOrder, Optional.ofNullable(state), childNodeId, parentNodeId, projectedVars);
        }
    }
}
