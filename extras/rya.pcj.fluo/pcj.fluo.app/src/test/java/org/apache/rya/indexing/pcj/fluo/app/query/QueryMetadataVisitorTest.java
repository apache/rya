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

public class QueryMetadataVisitorTest {

    @Test
    public void builderTest() throws UnsupportedQueryException {
        String query = "prefix function: <http://org.apache.rya/function#> " // n
                + "prefix time: <http://www.w3.org/2006/time#> " // n
                + "select ?id (count(?obs) as ?total) where {" // n
                + "Filter(function:periodic(?time, 2, .5, time:hours)) " // n
                + "?obs <uri:hasTime> ?time. " // n
                + "?obs <uri:hasId> ?id } group by ?id"; // n
        
        SparqlFluoQueryBuilder builder = new SparqlFluoQueryBuilder();
        builder.setFluoQueryId(NodeType.generateNewFluoIdForType(NodeType.QUERY));
        builder.setSparql(query);
        FluoQuery fluoQuery = builder.build();
        
        QueryMetadata queryMetadata = fluoQuery.getQueryMetadata();
        String queryId = queryMetadata.getNodeId();
        String projectionId = queryMetadata.getChildNodeId();
        String aggId = fluoQuery.getProjectionMetadata(projectionId).get().getChildNodeId();
        String periodicId = fluoQuery.getAggregationMetadata(aggId).get().getChildNodeId();
        String joinId = fluoQuery.getPeriodicQueryMetadata(periodicId).get().getChildNodeId();
        String leftSp = fluoQuery.getJoinMetadata(joinId).get().getLeftChildNodeId();
        String rightSp = fluoQuery.getJoinMetadata(joinId).get().getRightChildNodeId();
        
        List<String> expected = Arrays.asList(queryId, projectionId, aggId, periodicId, joinId, leftSp, rightSp);
        QueryMetadataVisitor visitor = new QueryMetadataVisitor(fluoQuery);
        visitor.visit();
        
        Assert.assertEquals(expected, visitor.getIds());
    }
    
    public static class QueryMetadataVisitor extends QueryMetadataVisitorBase {
        
        private List<String> ids = new ArrayList<>();
        
        public List<String> getIds() {
            return ids;
        }
        
        public QueryMetadataVisitor(FluoQuery metadata) {
            super(metadata);
        }
        
        public void visit(QueryMetadata metadata) {
            ids.add(metadata.getNodeId());
            super.visit(metadata);
        }
        
        public void visit(ProjectionMetadata metadata) {
            ids.add(metadata.getNodeId());
            super.visit(metadata);
        }
        
        public void visit(JoinMetadata metadata) {
            ids.add(metadata.getNodeId());
            super.visit(metadata);
        }
        
        public void visit(StatementPatternMetadata metadata) {
            ids.add(metadata.getNodeId());
        }
        
        public void visit(PeriodicQueryMetadata metadata) {
            ids.add(metadata.getNodeId());
            super.visit(metadata);
        }
        
        public void visit(FilterMetadata metadata) {
            ids.add(metadata.getNodeId());
            super.visit(metadata);
        }
        
        public void visit(AggregationMetadata metadata) {
            ids.add(metadata.getNodeId());
            super.visit(metadata);
        }
    }
    
}