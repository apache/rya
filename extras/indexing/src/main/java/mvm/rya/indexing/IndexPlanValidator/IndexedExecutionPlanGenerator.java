package mvm.rya.indexing.IndexPlanValidator;

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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import mvm.rya.indexing.external.QueryVariableNormalizer;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class IndexedExecutionPlanGenerator implements ExternalIndexMatcher {

    private final TupleExpr query;
    private List<ExternalTupleSet> normalizedIndexList;
    
    public IndexedExecutionPlanGenerator(TupleExpr query, List<ExternalTupleSet> indexList) {
        this.query = query;
        VarConstantIndexListPruner vci = new VarConstantIndexListPruner(query);
        normalizedIndexList = getNormalizedIndices(vci.getRelevantIndices(indexList));
    }
    
    public List<ExternalTupleSet> getNormalizedIndices() {
        return normalizedIndexList;
    }
    
  
    
    
    @Override
    public Iterator<TupleExpr> getIndexedTuples() {
        
        ValidIndexCombinationGenerator vic = new ValidIndexCombinationGenerator(query);
        final Iterator<List<ExternalTupleSet>> iter = vic.getValidIndexCombos(normalizedIndexList);

        return new Iterator<TupleExpr>() {

            private TupleExpr next = null;
            private boolean hasNextCalled = false;
            private boolean isEmpty = false;

            @Override
            public boolean hasNext() {

                if (!hasNextCalled && !isEmpty) {
                    while (iter.hasNext()) {
                        TupleExpr temp = GeneralizedExternalProcessor.process(query, iter.next());
                        if (temp != null) {
                            next = temp;
                            hasNextCalled = true;
                            return true;
                        }
                    }
                    isEmpty = true;
                    return false;
                } else if(isEmpty) {
                    return false;
                } else {
                    return true;
                }
            }

            @Override
            public TupleExpr next() {

                if (hasNextCalled) {
                    hasNextCalled = false;
                    return next;
                } else if(isEmpty) {
                    throw new NoSuchElementException();
                }else {
                    if (this.hasNext()) {
                        hasNextCalled = false;
                        return next;
                    } else {
                        throw new NoSuchElementException();
                    }

                }

            }

            @Override
            public void remove() {

                throw new UnsupportedOperationException("Cannot delete from iterator!");

            }

        };
    }

    
    private List<ExternalTupleSet> getNormalizedIndices(Set<ExternalTupleSet> indexSet) {

        ExternalTupleSet tempIndex;
        List<ExternalTupleSet> normalizedIndexSet = Lists.newArrayList();

        for (ExternalTupleSet e : indexSet) {

            List<TupleExpr> tupList = null;
            try {
                tupList = QueryVariableNormalizer.getNormalizedIndex(query, e.getTupleExpr());
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }

            for (TupleExpr te : tupList) {

                tempIndex = (ExternalTupleSet) e.clone();
                setTableMap(te, tempIndex);
                setSupportedVarOrderMap(tempIndex);
                tempIndex.setProjectionExpr((Projection) te);
                normalizedIndexSet.add(tempIndex);

            }

        }

        return normalizedIndexSet;
    }

    private void setTableMap(TupleExpr tupleMatch, ExternalTupleSet index) {

        List<String> replacementVars = Lists.newArrayList(tupleMatch.getBindingNames());
        List<String> tableVars = Lists.newArrayList(index.getTupleExpr().getBindingNames());

        Map<String, String> tableMap = Maps.newHashMap();

        for (int i = 0; i < tableVars.size(); i++) {
            tableMap.put(replacementVars.get(i), tableVars.get(i));
        }
        // System.out.println("Table map is " + tableMap);
        index.setTableVarMap(tableMap);

    }
    
    
    private void setSupportedVarOrderMap(ExternalTupleSet index) {

        Map<String, Set<String>> supportedVarOrders = Maps.newHashMap();
        BiMap<String, String> biMap = HashBiMap.create(index.getTableVarMap()).inverse();
        Map<String, Set<String>> oldSupportedVarOrders = index.getSupportedVariableOrderMap();

        Set<String> temp = null;
        Set<String> keys = oldSupportedVarOrders.keySet();

        for (String s : keys) {
            temp = oldSupportedVarOrders.get(s);
            Set<String> newSet = Sets.newHashSet();

            for (String t : temp) {
                newSet.add(biMap.get(t));
            }
            
            String[] tempStrings = s.split("\u0000");
            String v = "";
            for(String u: tempStrings) {
                if(v.length() == 0){
                    v = v + biMap.get(u);
                } else {
                    v = v + "\u0000" + biMap.get(u);
                }
            }

            supportedVarOrders.put(v, newSet);

        }

        index.setSupportedVariableOrderMap(supportedVarOrders);

    }
    
    
    

}
