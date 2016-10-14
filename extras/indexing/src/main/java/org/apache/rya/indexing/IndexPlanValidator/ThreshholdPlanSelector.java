package org.apache.rya.indexing.IndexPlanValidator;

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


import java.util.Iterator;
import java.util.Set;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.openrdf.query.algebra.BindingSetAssignment;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.google.common.collect.Sets;

public class ThreshholdPlanSelector implements IndexedQueryPlanSelector {

    private TupleExpr query;
    private int queryNodeCount = 0;

    public ThreshholdPlanSelector(TupleExpr query) {
        this.query = query;
        QueryNodeCount qnc = new QueryNodeCount();
        query.visit(qnc);

        this.queryNodeCount = qnc.getNodeCount();
        
        if(queryNodeCount == 0) {
            throw new IllegalArgumentException("TupleExpr must contain at least one node!");
        }
    }

    
    
    
    @Override
    public TupleExpr getThreshholdQueryPlan(Iterator<TupleExpr> tuples, double threshhold, double indexWeight,
            double commonVarWeight, double extProdWeight) {

        if (threshhold < 0 || threshhold > 1) {
            throw new IllegalArgumentException("Threshhold must be between 0 and 1!");
        }
        double minCost = Double.MAX_VALUE;
        TupleExpr minTup = null;

        double tempCost = 0;
        TupleExpr tempTup = null;

        
        
        while (tuples.hasNext()) {

            tempTup = tuples.next();
            tempCost = getCost(tempTup, indexWeight, commonVarWeight, extProdWeight);

            if (tempCost < minCost) {
                minCost = tempCost;
                minTup = tempTup;
            }

            if (minCost <= threshhold) {
                return minTup;
            }

        }

        return minTup;
    }

    public double getCost(TupleExpr te, double indexWeight, double commonVarWeight, double dirProdWeight) {

        if (indexWeight + commonVarWeight + dirProdWeight != 1) {
            throw new IllegalArgumentException("Weights must sum to 1!");
        }
        
        if(te == null) {
            throw new IllegalArgumentException("TupleExpr cannot be null!");
        }

        QueryNodeCount qnc = new QueryNodeCount();
        te.visit(qnc);

        double nodeCount = qnc.getNodeCount();
        double commonJoinVars = qnc.getCommonJoinVarCount();
        double joinVars = qnc.getJoinVarCount();
        double joinCount = qnc.getJoinCount();
        double dirProdCount = qnc.getDirProdCount();
        double dirProductScale;
        
        if(queryNodeCount > nodeCount) {
            dirProductScale = 1/((double)(queryNodeCount - nodeCount));
        } else {
            dirProductScale = 1/((double)(queryNodeCount - nodeCount + 1));
        }
        
        double joinVarRatio;
        double dirProductRatio;
        
        if(joinVars != 0) {
            joinVarRatio = (joinVars - commonJoinVars) / joinVars;
        } else {
            joinVarRatio = 0;
        }
        
        if(joinCount != 0) {
            dirProductRatio = dirProdCount / joinCount;
        } else {
            dirProductRatio = 0;
        }
        
        
        double cost = indexWeight * (nodeCount / queryNodeCount) + commonVarWeight*joinVarRatio
                + dirProdWeight *dirProductRatio*dirProductScale;
        
//        System.out.println("Tuple is " + te + " and cost is " + cost);
//        System.out.println("Node count is " + nodeCount + " and query node count is " + queryNodeCount);
//        System.out.println("Common join vars are " + commonJoinVars + " and join vars " + joinVars);
//        System.out.println("Join count is " + joinCount + " and direct prod count is " + dirProdCount);
        
        return cost;
    }

    public static class QueryNodeCount extends QueryModelVisitorBase<RuntimeException> {

        private int nodeCount = 0;
        private int commonJoinVars = 0;
        private int joinVars = 0;
        private int joinCount = 0;
        private int dirProdCount = 0;

        public int getCommonJoinVarCount() {
            return commonJoinVars;
        }

        public int getJoinVarCount() {
            return joinVars;
        }

        public int getNodeCount() {
            return nodeCount;
        }

        public int getJoinCount() {
            return joinCount;
        }

        public int getDirProdCount() {
            return dirProdCount;
        }

        public void meet(Projection node) {
            node.getArg().visit(this);
        }

        public void meetNode(QueryModelNode node) {
            if (node instanceof ExternalTupleSet) {
                nodeCount += 1;
                return;
            }
            super.meetNode(node);
            return;
        }

        @Override
        public void meet(StatementPattern node) {
            nodeCount += 1;
            return;
        }

        @Override
        public void meet(Filter node) {
            nodeCount += 1;
            node.getArg().visit(this);
        }

        public void meet(BindingSetAssignment node) {
            nodeCount += 1;
            return;
        }

        @Override
        public void meet(Join node) {

            int tempCount = 0;

            Set<String> lNames = node.getLeftArg().getAssuredBindingNames();
            Set<String> rNames = node.getRightArg().getAssuredBindingNames();
            
            for(String s: node.getLeftArg().getBindingNames()) {
                if(s.startsWith("-const-")) {
                    lNames.remove(s);
                }
            }
            
            for(String s: node.getRightArg().getBindingNames()) {
                if(s.startsWith("-const-")) {
                    rNames.remove(s);
                }
            }
           
            
            joinVars += Math.min(lNames.size(), rNames.size());
            tempCount = Sets.intersection(lNames, rNames).size();
            if (tempCount == 0) {
                dirProdCount += 1;
            } else {
                commonJoinVars += tempCount;
            }
            joinCount += 1;

            super.meet(node);

        }

    }

}
