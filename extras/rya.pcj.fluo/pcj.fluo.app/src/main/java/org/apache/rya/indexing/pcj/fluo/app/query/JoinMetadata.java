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

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Objects;

/**
 * Metadata that is specific to Join nodes.
 */
@Immutable
@ParametersAreNonnullByDefault
public class JoinMetadata extends CommonNodeMetadata {

    /**
     * The different types of Join algorithms that this join may perform.
     */
    public static enum JoinType {
        NATURAL_JOIN,
        LEFT_OUTER_JOIN;
    }

    private final JoinType joinType;
    private final String parentNodeId;
    private final String leftChildNodeId;
    private final String rightChildNodeId;

    /**
     * Constructs an instance of {@link JoinMetadata}.
     *
     * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
     * @param varOrder - The variable order of binding sets that are emitted by this node. (not null)
     * @param joinType - Defines which join algorithm the join will use.
     * @param parentNodeId - The node id of this node's parent. (not null)
     * @param leftChildNodeId - One of the nodes whose results are being joined. (not null)
     * @param rightChildNodeId - The other node whose results are being joined. (not null)
     */
    public JoinMetadata(
            final String nodeId,
            final VariableOrder varOrder,
            final JoinType joinType,
            final String parentNodeId,
            final String leftChildNodeId,
            final String rightChildNodeId) {
        super(nodeId, varOrder);
        this.joinType = checkNotNull(joinType);
        this.parentNodeId = checkNotNull(parentNodeId);
        this.leftChildNodeId = checkNotNull(leftChildNodeId);
        this.rightChildNodeId = checkNotNull(rightChildNodeId);
    }

    /**
     * @return Defines which join algorithm the join will use.
     */
    public JoinType getJoinType() {
        return joinType;
    }

    /**
     * @return The node id of this node's parent.
     */
    public String getParentNodeId() {
        return parentNodeId;
    }

    /**
     * @return One of the nodes whose results are being joined.
     */
    public String getLeftChildNodeId() {
        return leftChildNodeId;
    }

    /**
     * @return The other node whose results are being joined.
     */
    public String getRightChildNodeId() {
        return rightChildNodeId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                super.getNodeId(),
                super.getVariableOrder(),
                joinType,
                parentNodeId,
                leftChildNodeId,
                rightChildNodeId);
    }

    @Override
    public boolean equals(final Object o) {
        if(o == this) {
            return true;
        }

        if(o instanceof JoinMetadata) {
            if(super.equals(o)) {
                final JoinMetadata joinMetadata = (JoinMetadata)o;
                return new EqualsBuilder()
                        .append(joinType, joinMetadata.joinType)
                        .append(parentNodeId, joinMetadata.parentNodeId)
                        .append(leftChildNodeId, joinMetadata.leftChildNodeId)
                        .append(rightChildNodeId, joinMetadata.rightChildNodeId)
                        .isEquals();
            }
            return false;
        }

        return false;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Join Metadata {\n")
                .append("    Node ID: " + super.getNodeId() + "\n")
                .append("    Variable Order: " + super.getVariableOrder() + "\n")
                .append("    Join Type: " + joinType + "\n")
                .append("    Parent Node ID: " + parentNodeId + "\n")
                .append("    Left Child Node ID: " + leftChildNodeId + "\n")
                .append("    Right Child Node ID: " + rightChildNodeId + "\n")
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
     * Builds instances of {@link JoinMetadata}.
     */
    @ParametersAreNonnullByDefault
    public static final class Builder {

        private final String nodeId;
        private VariableOrder varOrder;
        private JoinType joinType;
        private String parentNodeId;
        private String leftChildNodeId;
        private String rightChildNodeId;

        /**
         * Constructs an instance of {@link Builder}.
         *
         * @param nodeId - The node ID associated with the Join node this builder makes. (not null)
         */
        public Builder(final String nodeId) {
            this.nodeId = checkNotNull(nodeId);
        }

        /**
         * @return The node ID associated with the Join node this builder makes.
         */
        public String getNodeId() {
            return nodeId;
        }

        /**
         * Sets the variable order of the binding sets that are emitted by this node.
         *
         * @param varOrder - The variable order of the binding sets that are emitted by this node.
         * @return This builder so that method invocation could be chained.
         */
        public Builder setVariableOrder(@Nullable final VariableOrder varOrder) {
            this.varOrder = varOrder;
            return this;
        }

        /**
         * Sets the node id of this node's parent.
         *
         * @param parentNodeId - The node id of this node's parent.
         * @return This builder so that method invocation could be chained.
         */
        public Builder setParentNodeId(@Nullable final String parentNodeId) {
            this.parentNodeId = parentNodeId;
            return this;
        }

        /**
         * Sets the type of join algorithm that will be used by this join.
         *
         * @param joinType - Defines which join algorithm the join will use.
         * @return This builder so that method invocation could be chained.
         */
        public Builder setJoinType(@Nullable final JoinType joinType) {
            this.joinType = joinType;
            return this;
        }

        /**
         * Set one of the nodes whose results are being joined.
         *
         * @param leftChildNodeId - One of the nodes whose results are being joined.
         * @return This builder so that method invocation could be chained.
         */
        public Builder setLeftChildNodeId(@Nullable final String leftChildNodeId) {
            this.leftChildNodeId = leftChildNodeId;
            return this;
        }

        /**
         * Set the other node whose results are being joined.
         *
         * @param rightChildNodeId - The other node whose results are being joined.
         * @return This builder so that method invocation could be chained.
         */
        public Builder setRightChildNodeId(@Nullable final String rightChildNodeId) {
            this.rightChildNodeId = rightChildNodeId;
            return this;
        }

        /**
         * @return An instance of {@link JoinMetadata} built using this builder's values.
         */
        public JoinMetadata build() {
            return new JoinMetadata(
                    nodeId,
                    varOrder,
                    joinType,
                    parentNodeId,
                    leftChildNodeId,
                    rightChildNodeId);
        }
    }
}