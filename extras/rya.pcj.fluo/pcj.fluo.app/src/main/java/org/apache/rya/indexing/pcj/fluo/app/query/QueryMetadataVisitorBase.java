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

public abstract class QueryMetadataVisitorBase {

 private FluoQuery fluoQuery;
    
    public QueryMetadataVisitorBase(FluoQuery fluoQuery) {
        this.fluoQuery = Preconditions.checkNotNull(fluoQuery);
    }
    
    public void visit() {
        visit(fluoQuery.getQueryMetadata());
    }
    
    /**
     * Visits the {@link FluoQuery} starting at the Metadata node with the given id
     * @param nodeId - id of the node this visitor will start at
     */
    public void visit(String nodeId) {
        visitNode(nodeId);
    }
    
    public void visit(QueryMetadata queryMetadata) {
        visitNode(queryMetadata.getChildNodeId());
    }
    
    public void visit(ConstructQueryMetadata constructMetadata) {
        visitNode(constructMetadata.getChildNodeId());
    }
    
    public void visit(ProjectionMetadata projectionMetadata) {
        visitNode(projectionMetadata.getChildNodeId());
    }
    
    public void visit(PeriodicQueryMetadata periodicMetadata) {
        visitNode(periodicMetadata.getChildNodeId());
    }
    
    public void visit(FilterMetadata filterMetadata) {
        visitNode(filterMetadata.getChildNodeId());
    }
    
    public void visit(JoinMetadata joinMetadata) {
        visitNode(joinMetadata.getLeftChildNodeId());
        visitNode(joinMetadata.getRightChildNodeId());
    }
    
    public void visit(AggregationMetadata aggregationMetadata) {
        visitNode(aggregationMetadata.getChildNodeId());
    }
    
    public void visit(StatementPatternMetadata statementPatternMetadata) {}
    
    public void visitNode(String nodeId) {
        Optional<NodeType> type = NodeType.fromNodeId(nodeId);
        try {
            switch(type.get()) {
            case AGGREGATION:
                visit(fluoQuery.getAggregationMetadata(nodeId).get());
                break;
            case CONSTRUCT:
                visit(fluoQuery.getConstructQueryMetadata(nodeId).get());
                break;
            case FILTER:
                visit(fluoQuery.getFilterMetadata(nodeId).get());
                break;
            case JOIN:
                visit(fluoQuery.getJoinMetadata(nodeId).get());
                break;
            case PERIODIC_QUERY:
                visit(fluoQuery.getPeriodicQueryMetadata(nodeId).get());
                break;
            case PROJECTION:
                visit(fluoQuery.getProjectionMetadata(nodeId).get());
                break;
            case QUERY:
                visit(fluoQuery.getQueryMetadata(nodeId).get());
                break;
            case STATEMENT_PATTERN:
                visit(fluoQuery.getStatementPatternMetadata(nodeId).get());
                break;
            default:
                throw new RuntimeException();
            } 
        } catch(Exception e) {
            throw new IllegalArgumentException("Invalid Fluo Query.");
        }
    }
    
}
