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

import java.util.List;

import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.util.VariableOrderUpdateVisitor.UpdateAction;

import com.google.common.base.Preconditions;

/**
 * Utility class for manipulating components of a {@link FluoQuery}.
 *
 */
public class FluoQueryUtils {

    /**
     * Updates the {@link VariableOrder}s of a given {@link FluoQuery.Builder}.
     * @param builder - builder whose VariableOrders will be updated
     * @param action - add or delete variables
     * @param variables - variables to be added or deleted
     * @param stopNodeId - node to stop at
     * @return - FluoQuery.Builder with updated VariableOrders
     */
    public static FluoQuery.Builder updateVarOrders(FluoQuery.Builder builder, UpdateAction action, List<String> variables, String stopNodeId) {
        VariableOrderUpdateVisitor visitor = new VariableOrderUpdateVisitor(builder, action, variables, stopNodeId);
        visitor.visit();
        
        return builder;
    }
    
    /**
     * Converts the fluo query id to a pcj id
     * @param fluoQueryId - query id of the form query_prefix + _ + UUID
     * @return the pcjid which consists of only the UUID portion of the fluo query id
     */
    public static String convertFluoQueryIdToPcjId(String fluoQueryId) {
        Preconditions.checkNotNull(fluoQueryId);
        String[] queryIdParts = fluoQueryId.split(IncrementalUpdateConstants.QUERY_PREFIX + "_");
        Preconditions.checkArgument(queryIdParts.length == 2 && queryIdParts[1]!= null && queryIdParts[1].length() > 0);
        return queryIdParts[1];
    }
    
    /**
     * Uses a {@link NodeIdCollector} visitor to do a pre-order traverse of the
     * FluoQuery and gather the nodeIds of the metadata nodes.
     * @param query - FluoQuery to be traversed
     * @return - List of nodeIds, ordered according to the pre-order traversal of the FluoQuery
     */
    public static List<String> collectNodeIds(FluoQuery query) {
        NodeIdCollector collector = new NodeIdCollector(query);
        collector.visit();
        return collector.getNodeIds();
    }
    
}
