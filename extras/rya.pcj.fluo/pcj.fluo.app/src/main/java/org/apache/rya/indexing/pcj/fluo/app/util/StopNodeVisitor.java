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
package org.apache.rya.indexing.pcj.fluo.app.util;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryBuilderVisitorBase;

/**
 *  Base class that enables all visitors extending this class to stop traversing
 *  the Builder tree at a prescribed node.  Any visitor extending this class will
 *  traverse the visitor tree and process all nodes up to and including the prescribed stop
 *  node.  
 */
public abstract class StopNodeVisitor extends QueryBuilderVisitorBase {

    private String stopNodeId;
    private boolean processedStopNode = false;
    
    public StopNodeVisitor(FluoQuery.Builder fluoBuilder,String nodeId) {
        super(fluoBuilder);
        this.stopNodeId = checkNotNull(nodeId);
    }
    
    @Override
    public void visitNode(String nodeId) {
        //process the stop node, then stop traversing
        if(!processedStopNode) {
            processedStopNode = atStopNode(nodeId);
            super.visitNode(nodeId);
        }
    }
    
    boolean atStopNode(String nodeId) {
        return nodeId.equals(stopNodeId);
    }
}
