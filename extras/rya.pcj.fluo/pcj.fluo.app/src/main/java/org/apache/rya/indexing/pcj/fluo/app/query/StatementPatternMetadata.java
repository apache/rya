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
 * Metadata that is specific to Statement Pattern nodes.
 */
@Immutable
@ParametersAreNonnullByDefault
public class StatementPatternMetadata extends CommonNodeMetadata {

    private final String statementPattern;
    private final String parentNodeId;

    /**
     * Constructs an instance of {@link StatementPatternMetadata}.
     *
     * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
     * @param varOrder - The variable order of binding sets that are emitted by this node. (not null)
     * @param statementPattern - The statement pattern new statements are matched against. (not null)
     * @param parentNodeId - The node id of this node's parent. (not null)
     */
    public StatementPatternMetadata(
            final String nodeId,
            final VariableOrder varOrder,
            final String statementPattern,
            final String parentNodeId) {
        super(nodeId, varOrder);
        this.statementPattern = checkNotNull(statementPattern);
        this.parentNodeId = checkNotNull(parentNodeId);
    }

    /**
     * @return The statement pattern new statements are matched against.
     */
    public String getStatementPattern() {
        return statementPattern;
    }

    /**
     * @return The node id of this node's parent.
     */
    public String getParentNodeId() {
        return parentNodeId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                super.getNodeId(),
                super.getVariableOrder(),
                statementPattern,
                parentNodeId);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }

        if(o instanceof StatementPatternMetadata) {
            if(super.equals(o)) {
                final StatementPatternMetadata spMetadata = (StatementPatternMetadata)o;
                return new EqualsBuilder()
                        .append(statementPattern, spMetadata.statementPattern)
                        .append(parentNodeId, spMetadata.parentNodeId)
                        .isEquals();
            }
            return false;
        }

        return false;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Statement Pattern Metadata {\n")
                .append("    Node ID: " + super.getNodeId() + "\n")
                .append("    Variable Order: " + super.getVariableOrder() + "\n")
                .append("    Parent Node ID: " + parentNodeId + "\n")
                .append("    Statement Pattern: " + statementPattern + "\n")
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
     * Builds instances of {@link StatementPatternMetadata}.
     */
    @ParametersAreNonnullByDefault
    public static final class Builder {

        private final String nodeId;
        private VariableOrder varOrder;
        private String statementPattern;
        private String parentNodeId;

        /**
         * Constructs an instance of {@link Builder}.
         *
         * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
         */
        public Builder(final String nodeId) {
            this.nodeId = checkNotNull(nodeId);
        }

        /**
         * @return The ID the Fluo app uses to reference this node.
         */
        public String getNodeId() {
            return nodeId;
        }

        /**
         * Sets the variable order of binding sets that are emitted by this node.
         *
         * @param varOrder - The variable order of binding sets that are emitted by this node.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setVarOrder(@Nullable final VariableOrder varOrder) {
            this.varOrder = varOrder;
            return this;
        }

        /**
         * Sets the statement pattern new statements are matched against.
         *
         * @param statementPattern - The statement pattern new statements are matched against.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setStatementPattern(@Nullable final String statementPattern) {
            this.statementPattern = statementPattern;
            return this;
        }

        /**
         * Sets the node id of this node's parent.
         *
         * @param parentNodeId - The node id of this node's parent.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setParentNodeId(@Nullable final String parentNodeId) {
            this.parentNodeId = parentNodeId;
            return this;
        }

        /**
         * @return Constructs an instance of {@link StatementPatternMetadata} using the values that are in this builder.
         */
        public StatementPatternMetadata build() {
            return new StatementPatternMetadata(
                    nodeId,
                    varOrder,
                    statementPattern,
                    parentNodeId);
        }
    }
}
