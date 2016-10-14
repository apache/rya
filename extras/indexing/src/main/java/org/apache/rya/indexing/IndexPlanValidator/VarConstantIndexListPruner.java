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


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;




public class VarConstantIndexListPruner implements IndexListPruner {

    private Map<String, Integer> queryConstantMap;
    private int querySpCount;
    private int queryFilterCount;

    public VarConstantIndexListPruner(TupleExpr te) {

        ConstantCollector cc = new ConstantCollector();
        te.visit(cc);
        this.queryConstantMap = cc.getConstantMap();
        querySpCount = cc.getSpCount();
        queryFilterCount = cc.getFilterCount();
    }

    @Override
	public List<ExternalTupleSet> getRelevantIndices(List<ExternalTupleSet> indexList) {

        List<ExternalTupleSet> relIndexSet = new ArrayList<>();

        for (ExternalTupleSet e : indexList) {

            if (isRelevant(e.getTupleExpr())) {
                relIndexSet.add(e);
            }

        }

        return relIndexSet;
    }

    private boolean isRelevant(TupleExpr index) {

        ConstantCollector cc = new ConstantCollector();
        index.visit(cc);

        Map<String, Integer> indexConstantMap = cc.getConstantMap();
        int indexSpCount = cc.getSpCount();
        int indexFilterCount = cc.getFilterCount();
        Set<String> indexConstants = indexConstantMap.keySet();

        if (indexSpCount > querySpCount || indexFilterCount > queryFilterCount
                || !Sets.intersection(indexConstants, queryConstantMap.keySet()).equals(indexConstants)) {
            return false;
        }

        for (String s : indexConstants) {
            if (indexConstantMap.get(s) > queryConstantMap.get(s)) {
                return false;
            }
        }

        return true;
    }


    private static class ConstantCollector extends QueryModelVisitorBase<RuntimeException> {

        private Map<String, Integer> constantMap = Maps.newHashMap();
        private int spCount = 0;
        private int filterCount = 0;


        @Override
        public void meet(StatementPattern node) throws RuntimeException {

           spCount++;
           super.meet(node);

        }


        @Override
        public void meet(Filter node) throws RuntimeException {

           filterCount++;
           super.meet(node);

        }




        @Override
        public void meet(Var node) throws RuntimeException {

            if (node.isConstant()) {
                String key = node.getValue().toString();
                if(constantMap.containsKey(key)){
                    int count = constantMap.get(key);
                    count += 1;
                    constantMap.put(key, count);
                } else {
                    constantMap.put(key, 1);
                }
            }

        }


        @Override
		public void meet(ValueConstant node) throws RuntimeException {

            String key = node.getValue().toString();

            if(constantMap.containsKey(key)) {
                int count = constantMap.get(key);
                count += 1;
                constantMap.put(key, count);
            } else {
                constantMap.put(key,1);
            }

        }


        public Map<String, Integer> getConstantMap() {
            return constantMap;
        }

        public int getSpCount(){
            return spCount;
        }


        public int getFilterCount() {
            return filterCount;
        }

    }

}
