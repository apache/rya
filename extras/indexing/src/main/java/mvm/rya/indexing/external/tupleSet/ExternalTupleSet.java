package mvm.rya.indexing.external.tupleSet;

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



import info.aduna.iteration.CloseableIteration;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

/**
 * Abstract class for an External Tuple Set.  This Tuple 
 */
public abstract class ExternalTupleSet extends ExternalSet {

    private Projection tupleExpr;
    private Map<String, String> tableVarMap = Maps.newHashMap();
    private Map<String, Set<String>> supportedVarOrders = Maps.newHashMap();

    
    public ExternalTupleSet() {
    	
    }
    
    public ExternalTupleSet(Projection tupleExpr) {
        this.tupleExpr = tupleExpr;
    }

    @Override
    abstract public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings) throws QueryEvaluationException;

    @Override
    public Set<String> getBindingNames() {
        return tupleExpr.getBindingNames();
    }

    @Override
    public Set<String> getAssuredBindingNames() {
        return tupleExpr.getAssuredBindingNames();
    }

    @Override
    public String getSignature() {
        return "(External Projection) " + Joiner.on(", ").join(tupleExpr.getProjectionElemList().getElements()).replaceAll("\\s+", " ");
    }

    public Projection getTupleExpr() {
        return tupleExpr;
    }

    public void setProjectionExpr(Projection tupleExpr) {
        this.tupleExpr = tupleExpr;
    }
    
    
    public void setTableVarMap(Map<String,String> vars) {
        this.tableVarMap = vars;
    }
    
    
    public Map<String, String> getTableVarMap() {
        return this.tableVarMap;
    }
    
    
    public void setSupportedVariableOrderMap(Map<String, Set<String>> varOrders) {
        this.supportedVarOrders = varOrders;
    }
    
    
    public Map<String, Set<String>> getSupportedVariableOrderMap() {
        return supportedVarOrders;
    }
    

    public void updateTupleExp(final Map<Var, Var> oldToNewBindings) {
        tupleExpr.visit(new QueryModelVisitorBase<RuntimeException>() {
            @Override
            public void meet(Var var) {
                if (oldToNewBindings.containsKey(var)) {
                    var.replaceWith(oldToNewBindings.get(var));
                }
            }
        });
    }

    @Override
    public ExternalSet clone() {
        ExternalTupleSet clone = (ExternalTupleSet) super.clone();
        clone.tupleExpr = this.tupleExpr.clone();
        clone.tableVarMap = Maps.newHashMap();
        for(String s: this.tableVarMap.keySet()) {
            clone.tableVarMap.put(s,this.tableVarMap.get(s));
        }
        clone.supportedVarOrders = Maps.newHashMap();
        for(String s: this.supportedVarOrders.keySet()) {
            clone.supportedVarOrders.put(s,this.supportedVarOrders.get(s));
        }
        return clone;
    }
    
    
    public Map<String, Set<String>> getSupportedVariableOrders() {
        
        if (supportedVarOrders.size() != 0) {
            return supportedVarOrders;
        } else {

            Set<String> varSet = Sets.newHashSet();
            String t = "";

            for (String s : tupleExpr.getAssuredBindingNames()) {
                if (t.length() == 0) {
                    t = s;
                } else {
                    t = t + "\u0000" + s;
                }

                varSet.add(s);
                supportedVarOrders.put(t, new HashSet<String>(varSet));

            }

            return supportedVarOrders;
        }
    }
    
    
    
    
    public boolean supportsBindingSet(Set<String> bindingNames) {

        Map<String, Set<String>> varOrderMap = getSupportedVariableOrders();
        String bNames = "";

        for (String s : tupleExpr.getAssuredBindingNames()) {
            if (bindingNames.contains(s)) {
                if(bNames.length() == 0) {
                    bNames = s;
                } else {
                    bNames = bNames + "\u0000"+ s;
                }
            }
        }

        return varOrderMap.containsKey(bNames);
    }
        
        
    
    @Override
    public boolean equals(Object other) {

        if (!(other instanceof ExternalTupleSet)) {
            return false;
        } else {

            ExternalTupleSet arg = (ExternalTupleSet) other;
            if (this.getTupleExpr().equals(arg.getTupleExpr())) {
                return true;
            } else {
                return false;
            }

        }

    }
    
    
    @Override
    public int hashCode() {
        int result = 17;
        result = 31*result + tupleExpr.hashCode();
        
        return result;
    }
    
    
 
    
    
    
}
