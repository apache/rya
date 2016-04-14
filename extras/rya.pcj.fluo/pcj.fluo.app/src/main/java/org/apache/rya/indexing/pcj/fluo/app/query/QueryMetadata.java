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
 * Metadata that is specific to a Projection.
 */
@Immutable
@ParametersAreNonnullByDefault
public class QueryMetadata extends CommonNodeMetadata {

    private final String sparql;
    private final String childNodeId;

    /**
     * Constructs an instance of {@link QueryMetadata}.
     *
     * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
     * @param varOrder - The variable order of binding sets that are emitted by this node. (not null)
     * @param sparql - The SPARQL query whose results are being updated by the Fluo app. (not null)
     * @param childNodeId - The node whose results are projected to the query's SELECT variables. (not null)
     */
    public QueryMetadata(
            final String nodeId,
            final VariableOrder varOrder,
            final String sparql,
            final String childNodeId) {
        super(nodeId, varOrder);
        this.sparql = checkNotNull(sparql);
        this.childNodeId = checkNotNull(childNodeId);
    }

    /**
     * @return The SPARQL query whose results are being updated by the Fluo app.
     */
    public String getSparql() {
        return sparql;
    }

    /**
     * @return The node whose results are projected to the query's SELECT variables.
     */
    public String getChildNodeId() {
        return childNodeId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                super.getNodeId(),
                super.getVariableOrder(),
                sparql,
                childNodeId);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }

        if(o instanceof QueryMetadata) {
            if(super.equals(o)) {
                final QueryMetadata queryMetadata = (QueryMetadata)o;
                return new EqualsBuilder()
                        .append(sparql, queryMetadata.sparql)
                        .append(childNodeId, queryMetadata.childNodeId)
                        .isEquals();
            }
            return false;
        }

        return false;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("QueryMetadata {\n")
                .append("    Node ID: " + super.getNodeId() + "\n")
                .append("    Variable Order: " + super.getVariableOrder() + "\n")
                .append("    Child Node ID: " + childNodeId + "\n")
                .append("    SPARQL: " + sparql + "\n")
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
     * Builds instances of {@link QueryMetadata}.
     */
    @ParametersAreNonnullByDefault
    public static final class Builder {

        private final String nodeId;
        private VariableOrder varOrder;
        private String sparql;
        private String childNodeId;

        /**
         * Constructs an instance of {@link Builder}.
         *
         * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
         */
        public Builder(final String nodeId) {
            this.nodeId = checkNotNull(nodeId);
        }

        /**
         * Set the variable order of binding sets that are emitted by this node.
         *
         * @param varOrder - The variable order of binding sets that are emitted by this node.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setVariableOrder(@Nullable final VariableOrder varOrder) {
            this.varOrder = varOrder;
            return this;
        }

        /**
         * Set the SPARQL query whose results are being updated by the Fluo app.
         *
         * @param sparql - The SPARQL query whose results are being updated by the Fluo app.
         * @return This builder so that method invocations may be chained.
         */
        public Builder setSparql(@Nullable final String sparql) {
            this.sparql = sparql;
            return this;
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

        /**
         * @return An instance of {@link QueryMetadata} build using this builder's values.
         */
        public QueryMetadata build() {
            return new QueryMetadata(nodeId, varOrder, sparql, childNodeId);
        }
    }
}