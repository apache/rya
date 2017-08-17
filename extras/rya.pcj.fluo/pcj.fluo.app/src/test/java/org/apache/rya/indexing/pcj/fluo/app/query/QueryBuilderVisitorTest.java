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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.junit.Assert;
import org.junit.Test;

public class QueryBuilderVisitorTest {

    @Test
    public void builderTest() {
        
        FluoQuery.Builder fluoBuilder = FluoQuery.builder();
        
        String queryId = NodeType.generateNewFluoIdForType(NodeType.QUERY);
        String projectionId = NodeType.generateNewFluoIdForType(NodeType.PROJECTION);
        String joinId = NodeType.generateNewFluoIdForType(NodeType.JOIN);
        String leftSp = NodeType.generateNewFluoIdForType(NodeType.STATEMENT_PATTERN);
        String rightSp = NodeType.generateNewFluoIdForType(NodeType.STATEMENT_PATTERN);
        
        List<String> expected = Arrays.asList(queryId, projectionId, joinId, leftSp, rightSp);
        
        QueryMetadata.Builder queryBuilder = QueryMetadata.builder(queryId);
        queryBuilder.setChildNodeId(projectionId);
        
        ProjectionMetadata.Builder projectionBuilder = ProjectionMetadata.builder(projectionId);
        projectionBuilder.setChildNodeId(joinId);
        
        JoinMetadata.Builder joinBuilder = JoinMetadata.builder(joinId);
        joinBuilder.setLeftChildNodeId(leftSp);
        joinBuilder.setRightChildNodeId(rightSp);
        
        StatementPatternMetadata.Builder left = StatementPatternMetadata.builder(leftSp);
        StatementPatternMetadata.Builder right = StatementPatternMetadata.builder(rightSp);
        
        fluoBuilder.setQueryMetadata(queryBuilder);
        fluoBuilder.addProjectionBuilder(projectionBuilder);
        fluoBuilder.addJoinMetadata(joinBuilder);
        fluoBuilder.addStatementPatternBuilder(left);
        fluoBuilder.addStatementPatternBuilder(right);
        
        QueryBuilderPrinter printer = new QueryBuilderPrinter(fluoBuilder);
        printer.visit();
        Assert.assertEquals(expected, printer.getIds());
    }
    
    
    public static class QueryBuilderPrinter extends QueryBuilderVisitorBase {
        
        private List<String> ids = new ArrayList<>();
        
        public List<String> getIds() {
            return ids;
        }
        
        public QueryBuilderPrinter(FluoQuery.Builder builder) {
            super(builder);
        }
        
        public void visit(QueryMetadata.Builder queryBuilder) {
            ids.add(queryBuilder.getNodeId());
            super.visit(queryBuilder);
        }
        
        public void visit(ProjectionMetadata.Builder projectionBuilder) {
            ids.add(projectionBuilder.getNodeId());
            super.visit(projectionBuilder);
        }
        
        public void visit(JoinMetadata.Builder joinBuilder) {
            ids.add(joinBuilder.getNodeId());
            super.visit(joinBuilder);
        }
        
        public void visit(StatementPatternMetadata.Builder statementBuilder) {
            ids.add(statementBuilder.getNodeId());
        }
    }
    
}
