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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.query.AggregationMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.CommonNodeMetadataImpl;
import org.apache.rya.indexing.pcj.fluo.app.query.ConstructQueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FilterMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.PeriodicQueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.ProjectionMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Preconditions;

/**
 * Visitor that traverses a {@link FluoQuery.Builder} and performs the indicated {@link UpdateAction} on the
 * {@link VariableOrder}s of each node using a provided list of variables. The visitor either adds the provided list of
 * variables to the VariableOrder of each node or deletes the provided variables from the VariableOrder of each node.
 *
 */
public class VariableOrderUpdateVisitor extends StopNodeVisitor {

    /**
     * Enum class indicating whether to add or delete variables from the VariableOrders of nodes in the FluoQuery.
     *
     */
    public static enum UpdateAction {
        AddVariable, DeleteVariable
    };

    private UpdateAction action;
    private List<String> variables;

    /**
     * Creates a VariableOrderUpdateVisitor to update the variables in a given FluoQuery.Builder
     * 
     * @param fluoBuilder - builder whose VariableOrder will be updated
     * @param action - either add or delete
     * @param variables - variables to be added or deleted
     * @param stopNodeId - indicates the builder node to stop at
     */
    public VariableOrderUpdateVisitor(FluoQuery.Builder fluoBuilder, UpdateAction action, List<String> variables, String stopNodeId) {
        super(fluoBuilder, stopNodeId);
        this.action = Preconditions.checkNotNull(action);
        this.variables = Preconditions.checkNotNull(variables);
    }

    public void visit(QueryMetadata.Builder builder) {
        builder.setVarOrder(updateOrder(builder.getVariableOrder()));
        builder.setStateMetadata(updateAggregationMetadata(builder.getStateMetadata()).orElse(null));
        super.visit(builder);
    }

    public void visit(ProjectionMetadata.Builder builder) {
        builder.setVarOrder(updateOrder(builder.getVariableOrder()));
        builder.setStateMetadata(updateAggregationMetadata(builder.getStateMetadata()).orElse(null));
        if (action == UpdateAction.AddVariable) {
            builder.setProjectedVars(updateOrder(builder.getProjectionVars()));
        }
        super.visit(builder);
    }

    public void visit(ConstructQueryMetadata.Builder builder) {
        builder.setVarOrder(updateOrder(builder.getVariableOrder()));
        builder.setStateMetadata(updateAggregationMetadata(builder.getStateMetadata()).orElse(null));
        super.visit(builder);
    }

    public void visit(FilterMetadata.Builder builder) {
        builder.setVarOrder(updateOrder(builder.getVariableOrder()));
        builder.setStateMetadata(updateAggregationMetadata(builder.getStateMetadata()).orElse(null));
        super.visit(builder);
    }

    public void visit(PeriodicQueryMetadata.Builder builder) {
        builder.setVarOrder(updateOrder(builder.getVariableOrder()));
        builder.setStateMetadata(updateAggregationMetadata(builder.getStateMetadata()).orElse(null));
        super.visit(builder);
    }

    public void visit(JoinMetadata.Builder builder) {
        builder.setVarOrder(updateOrder(builder.getVariableOrder()));
        builder.setStateMetadata(updateAggregationMetadata(builder.getStateMetadata()).orElse(null));
        super.visit(builder);
    }

    public void visit(AggregationMetadata.Builder builder) {
        builder.setVarOrder(updateOrder(builder.getVariableOrder()));
        builder.setGroupByVariableOrder(updateOrder(builder.getGroupByVariableOrder()));
        super.visit(builder);
    }

    public void visit(StatementPatternMetadata.Builder builder) {
        super.visit(builder);
    }

    private VariableOrder updateOrder(VariableOrder varOrder) {

        switch (action) {
        case AddVariable:
            varOrder = addBindingToOrder(varOrder);
            break;
        case DeleteVariable:
            varOrder = deleteBindingFromOrder(varOrder);
            break;
        }
        return varOrder;
    }
    
    private Optional<CommonNodeMetadataImpl> updateAggregationMetadata(Optional<CommonNodeMetadataImpl> aggStateMetadata) {
        if(aggStateMetadata.isPresent()) {
            CommonNodeMetadataImpl metadata = aggStateMetadata.get();
            metadata.setVariableOrder(updateOrder(metadata.getVariableOrder()));
            return Optional.of(metadata);
        }
        
        return Optional.empty();
    }

    private VariableOrder addBindingToOrder(VariableOrder varOrder) {
        List<String> orderList = new ArrayList<>(varOrder.getVariableOrders());
        orderList.addAll(0, variables);
        return new VariableOrder(orderList);
    }

    private VariableOrder deleteBindingFromOrder(VariableOrder varOrder) {
        List<String> vars = new ArrayList<>();
        varOrder.getVariableOrders().forEach(x -> {
            if (!variables.contains(x) || x.equals(IncrementalUpdateConstants.PERIODIC_BIN_ID)) {
                vars.add(x);
            }
        });
        return new VariableOrder(vars);
    }
}
