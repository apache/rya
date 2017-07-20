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

import org.apache.rya.indexing.pcj.fluo.app.NodeType;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

/**
 * Base visitor class for navigating a {@link FluoQuery.Builder}.
 * The visit methods in this class provide the basic functionality
 * for navigating between the Builders that make u the FluoQuery.Builder.
 *
 */
public abstract class QueryBuilderVisitorBase {

    private FluoQuery.Builder fluoBuilder;
    
    public QueryBuilderVisitorBase(FluoQuery.Builder fluoBuilder) {
        this.fluoBuilder = Preconditions.checkNotNull(fluoBuilder);
    }
    
    public void visit() {
        this.visit(fluoBuilder.getQueryBuilder());
    }
    
    /**
     * Visits the {@link FluoQuery.Builder} starting at the Metadata bulder node with the given id
     * @param nodeId - id of the node this visitor will start at
     */
    public void visit(String nodeId) {
        visitNode(nodeId);
    }
    
    public void visit(QueryMetadata.Builder queryBuilder) {
        visitNode(queryBuilder.getChildNodeId());
    }
    
    public void visit(ConstructQueryMetadata.Builder constructBuilder) {
        visitNode(constructBuilder.getChildNodeId());
    }
    
    public void visit(ProjectionMetadata.Builder projectionBuilder) {
        visitNode(projectionBuilder.getChildNodeId());
    }
    
    public void visit(PeriodicQueryMetadata.Builder periodicBuilder) {
        visitNode(periodicBuilder.getChildNodeId());
    }
    
    public void visit(FilterMetadata.Builder filterBuilder) {
        visitNode(filterBuilder.getChildNodeId());
    }
    
    public void visit(JoinMetadata.Builder joinBuilder) {
        visitNode(joinBuilder.getLeftChildNodeId());
        visitNode(joinBuilder.getRightChildNodeId());
    }
    
    public void visit(AggregationMetadata.Builder aggregationBuilder) {
        visitNode(aggregationBuilder.getChildNodeId());
    }
    
    public void visit(StatementPatternMetadata.Builder statementPatternBuilder) {}
    
    public void visitNode(String nodeId) {
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        try {
            switch(type.get()) {
            case AGGREGATION:
                visit(fluoBuilder.getAggregateBuilder(nodeId).get());
                break;
            case CONSTRUCT:
                visit(fluoBuilder.getConstructQueryBuilder(nodeId).get());
                break;
            case FILTER:
                visit(fluoBuilder.getFilterBuilder(nodeId).get());
                break;
            case JOIN:
                visit(fluoBuilder.getJoinBuilder(nodeId).get());
                break;
            case PERIODIC_QUERY:
                visit(fluoBuilder.getPeriodicQueryBuilder(nodeId).get());
                break;
            case PROJECTION:
                visit(fluoBuilder.getProjectionBuilder(nodeId).get());
                break;
            case QUERY:
                visit(fluoBuilder.getQueryBuilder(nodeId).get());
                break;
            case STATEMENT_PATTERN:
                visit(fluoBuilder.getStatementPatternBuilder(nodeId).get());
                break;
            default:
                throw new RuntimeException();
            } 
        } catch(Exception e) {
            throw new IllegalArgumentException("Invalid Fluo Query.");
        }
    }
    
}
