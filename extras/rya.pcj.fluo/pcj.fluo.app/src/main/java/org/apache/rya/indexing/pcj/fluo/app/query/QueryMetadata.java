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
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.CreatePCJ.QueryType;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Objects;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

/**
 * Metadata for a query registered with Fluo.  This metadata is for the topmost node
 * in the {@link FluoQuery}, and it includes information about how to export results
 * for the query.
 */
@Immutable
@DefaultAnnotation(NonNull.class)
public class QueryMetadata extends StateNodeMetadata {

    private final String sparql;
    private final String childNodeId;
    private final Set<ExportStrategy> exportStrategy;
    private final QueryType queryType;
    private final String exportId;
    

    /**
     * Constructs an instance of {@link QueryMetadata}.
     *
     * @param nodeId - The ID the Fluo app uses to reference this node. (not null)
     * @param varOrder - The variable order of binding sets that are emitted by this node. (not null)
     * @param stateMetadata - Optional containing information about the aggregation state that this node depends on. (not null)
     * @param sparql - The SPARQL query whose results are being updated by the Fluo app. (not null)
     * @param childNodeId - The node whose results are projected to the query's SELECT variables. (not null)
     * @param exportStrategy - Set of export strategies used for emiting results from Rya-Fluo app
     */
    public QueryMetadata(
            final String nodeId,
            final VariableOrder varOrder,
            final Optional<CommonNodeMetadataImpl> stateMetadata,
            final String sparql,
            final String childNodeId,
            final Set<ExportStrategy> exportStrategy,
            final QueryType queryType) {
        super(nodeId, varOrder, stateMetadata);
        this.sparql = checkNotNull(sparql);
        this.childNodeId = checkNotNull(childNodeId);
        this.exportStrategy = checkNotNull(exportStrategy);
        this.queryType = checkNotNull(queryType);
        String[] idSplit = nodeId.split("_");
        if(idSplit.length != 2) {
            throw new IllegalArgumentException("Invalid Query Node Id");
        }
        this.exportId = idSplit[1];
    }
    
    public String getExportId() {
        return exportId;
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
    
    /**
     * @return strategies used for exporting results from Rya-Fluo Application
     */
    public Set<ExportStrategy> getExportStrategies() {
        return exportStrategy;
    }
    
    /**
     * @return the {@link QueryType} of this query
     */
    public QueryType getQueryType() {
        return queryType;
    }
    
    
    @Override
    public int hashCode() {
        return Objects.hashCode(
                super.getNodeId(),
                super.getVariableOrder(),
                super.getStateMetadata(),
                sparql,
                childNodeId,
                exportStrategy,
                queryType);
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
                        .append(exportStrategy, queryMetadata.exportStrategy)
                        .append(queryType, queryMetadata.queryType)
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
                .append("    State Metadata: " + super.getStateMetadata() + "\n")
                .append("    Child Node ID: " + childNodeId + "\n")
                .append("    SPARQL: " + sparql + "\n")
                .append("    Query Type: " + queryType + "\n")
                .append("    Export Strategies: " + exportStrategy + "\n")
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
    @DefaultAnnotation(NonNull.class)
    public static final class Builder implements CommonNodeMetadata.Builder {

        private String nodeId;
        private VariableOrder varOrder;
        private CommonNodeMetadataImpl state;
        private String sparql;
        private String childNodeId;
        private Set<ExportStrategy> exportStrategies;
        private QueryType queryType;
        private Optional<Integer> joinBatchSize = Optional.empty();
        

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
         * @param varOrder - The variable order of binding sets that are emitted by this node.
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
         * Sets export strategies used for emitting results form Rya Fluo app
         * @param export - Set of export strategies
         * @return This builder so that method invocations may be chained
         */
        public Builder setExportStrategies(Set<ExportStrategy> export) {
            this.exportStrategies = export;
            return this;
        }
        
        /**
         * Set query type for the given query
         * @param queryType - {@link QueryType} of the given query
         * @return This builder so that method invocations may be chained
         */
        public Builder setQueryType(QueryType queryType) {
            this.queryType = queryType;
            return this;
        }
        
        /**
         * @return QueryType for the given query
         */
        public QueryType getQueryType() {
            return queryType;
        }
        
        
        /**
         * @return id of the child node of this node
         */
        public String getChildNodeId() {
            return childNodeId;
        }
        
        /**
         * Sets batch size used to process joins for this query
         * @param joinBatchSize - batch size used to process joins
         */
        public Builder setJoinBatchSize(Optional<Integer> joinBatchSize) {
            this.joinBatchSize = joinBatchSize;
            return this;
        }
        
        /**
         * @return Optional containing the batch size used to process large joins
         */
        public Optional<Integer> getJoinBatchSize() {
            return joinBatchSize;
        }

        /**
         * @return An instance of {@link QueryMetadata} build using this builder's values.
         */
        public QueryMetadata build() {
            return new QueryMetadata(nodeId, varOrder, Optional.ofNullable(state), sparql, childNodeId, exportStrategies, queryType);
        }
    }
}