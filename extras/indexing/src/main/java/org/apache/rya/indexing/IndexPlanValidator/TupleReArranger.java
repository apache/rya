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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.rdftriplestore.inference.DoNotExpandSP;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;


//A given TupleExpr can be broken up into "join segments", which are sections of the TupleExpr where nodes can
//be freely exchanged.  This class creates a list of permuted TupleExpr from a specified TupleExpr by permuting the nodes
//in each join segment.
public class TupleReArranger {

    private static Map<Join, List<List<TupleExpr>>> joinArgs;
    private static Map<Join, List<Filter>> filterArgs;

    
    public static Iterator<TupleExpr> getPlans(Iterator<TupleExpr> indexPlans) {

        final Iterator<TupleExpr> iter = indexPlans;

        return new Iterator<TupleExpr>() {

            private TupleExpr next = null;
            private boolean hasNextCalled = false;
            private boolean isEmpty = false;
            Iterator<TupleExpr> tuples = null;

            @Override
            public boolean hasNext() {

                if (!hasNextCalled && !isEmpty) {
                    if (tuples != null && tuples.hasNext()) {
                        next = tuples.next();
                        hasNextCalled = true;
                        return true;
                    } else {
                        while (iter.hasNext()) {
                            tuples = getTupleReOrderings(iter.next()).iterator();
                            if (tuples == null) {
                                throw new IllegalStateException("Plans cannot be null!");
                            }
                            next = tuples.next();
                            hasNextCalled = true;
                            return true;
                        }
                        isEmpty = true;
                        return false;
                    }
                } else if (isEmpty) {
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
                } else if (isEmpty) {
                    throw new NoSuchElementException();
                } else {
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
    
    
    //Give a TupleExpr, return list of join segment permuted TupleExpr
    public static List<TupleExpr> getTupleReOrderings(TupleExpr te) {

        joinArgs = Maps.newHashMap();
        filterArgs = Maps.newHashMap();
        
        NodeCollector nc = new NodeCollector();
        te.visit(nc);
        joinArgs = nc.getPerms();
        List<Join> joins = Lists.newArrayList(joinArgs.keySet());
        
        return getPlans(getReOrderings(joins), te);

    }

    
    //iterates through the reOrder maps, and for each reOrder map builds a new, reordered tupleExpr
    private static List<TupleExpr> getPlans(List<Map<Join, List<TupleExpr>>> reOrderings, TupleExpr te) {

        List<TupleExpr> queryPlans = Lists.newArrayList();
        PermInserter pm = new PermInserter();

        for (Map<Join, List<TupleExpr>> order : reOrderings) {
            TupleExpr clone = te.clone();
            pm.setReOrderMap(order);
            clone.visit(pm);
            queryPlans.add(clone);
        }

        return queryPlans;
    }

  
  
    //recursive method which produces a list of maps.  Each map associates a join with
    //a list of the non-join arguments below it contained in same join segment.  The list 
    //represents an ordering of the
    //non-join arguments and creating a TupleExpr from this map yields a new TupleExpr
    //whose non-join arguments are permuted
    private static List<Map<Join, List<TupleExpr>>> getReOrderings(List<Join> joins) {
        Map<Join, List<TupleExpr>> reOrder = Maps.newHashMap();
        List<Map<Join, List<TupleExpr>>> reOrderings = Lists.newArrayList();
        getReOrderings(joins, reOrder, reOrderings);
        return reOrderings;

    }

    private static void getReOrderings(List<Join> joins, Map<Join, List<TupleExpr>> reOrder,
            List<Map<Join, List<TupleExpr>>> reOrderings) {

        if (joins.isEmpty()) {
            reOrderings.add(reOrder);
            return;
        }

        List<Join> joinsCopy = Lists.newArrayList(joins);
        Join join = joinsCopy.remove(0);
        List<List<TupleExpr>> joinArgPerms = joinArgs.get(join);
        for (List<TupleExpr> tupList : joinArgPerms) {
            Map<Join, List<TupleExpr>> newReOrder = Maps.newHashMap(reOrder);
            newReOrder.put(join, tupList);
            getReOrderings(joinsCopy, newReOrder, reOrderings);
        }
        
        return;

    }
    
    
   //creates a map which associates each first join of a TupleExpr join segment with all permutations of
    //the non-join nodes after it.  More specifically, each join is associated with a list of TupleExpr
    //lists, where each list represents an ordering of the non-join nodes following the associated join
    private static class NodeCollector extends QueryModelVisitorBase<RuntimeException> {

        private static List<Filter> filterList;

        public Map<Join, List<List<TupleExpr>>> getPerms() {
            return joinArgs;
        }

        @Override
        public void meet(Join node) {

            filterList = Lists.newArrayList();
            
            List<TupleExpr> args = Lists.newArrayList();
            args = getJoinArgs(node, args);
            List<List<TupleExpr>> argPerms = Lists.newArrayList(Collections2.permutations(args));
            joinArgs.put(node, argPerms);
            filterArgs.put(node, filterList);

            for (TupleExpr te : args) {
                if (!(te instanceof StatementPattern) && !(te instanceof ExternalTupleSet)) {
                    te.visit(this);
                }
            }

        }

        
        //get all non-join nodes below tupleExpr in same join segment
        private static List<TupleExpr> getJoinArgs(TupleExpr tupleExpr, List<TupleExpr> joinArgs) {
            if (tupleExpr instanceof Join) {
                if (!(((Join) tupleExpr).getLeftArg() instanceof FixedStatementPattern)
                        && !(((Join) tupleExpr).getRightArg() instanceof DoNotExpandSP)) {
                    Join join = (Join) tupleExpr;
                    getJoinArgs(join.getLeftArg(), joinArgs);
                    getJoinArgs(join.getRightArg(), joinArgs);
                } // assumes all filter occur above first join of segment --
                  // this should be the state
                  // after PrecompJoinOptimizer is called
            } else if (tupleExpr instanceof Filter) {
                filterList.add((Filter) tupleExpr);
                getJoinArgs(((Filter) tupleExpr).getArg(), joinArgs);
            } else {
                joinArgs.add(tupleExpr);
            }

            return joinArgs;
        }

    }

    

    //for a given reOrder map, searches through TupleExpr and places each reordered collection
    //of nodes at appropriate join
    private static class PermInserter extends QueryModelVisitorBase<RuntimeException> {

        private Map<Join, List<TupleExpr>> reOrderMap = Maps.newHashMap();

        public void setReOrderMap(Map<Join, List<TupleExpr>> reOrderMap) {
            this.reOrderMap = reOrderMap;
        }

        @Override
        public void meet(Join node) {

            List<TupleExpr> reOrder = reOrderMap.get(node);
            if (reOrder != null) {
                List<Filter> filterList = Lists.newArrayList(filterArgs.get(node));
                node.replaceWith(getNewJoin(reOrder, getFilterChain(filterList)));

                for (TupleExpr te : reOrder) {
                    if (!(te instanceof StatementPattern) && !(te instanceof ExternalTupleSet)) {
                        te.visit(this);
                    }
                }
            }
            super.meet(node);
        }
    }
   

    // chain filters together and return front and back of chain
    private static List<TupleExpr> getFilterChain(List<Filter> filters) {
        List<TupleExpr> filterTopBottom = Lists.newArrayList();
        Filter filterChainTop = null;
        Filter filterChainBottom = null;

        for (Filter filter : filters) {
            if (filterChainTop == null) {
                filterChainTop = filter.clone();
            } else if (filterChainBottom == null) {
                filterChainBottom = filter.clone();
                filterChainTop.setArg(filterChainBottom);
            } else {
                Filter newFilter = filter.clone();
                filterChainBottom.setArg(newFilter);
                filterChainBottom = newFilter;
            }
        }
        if (filterChainTop != null) {
            filterTopBottom.add(filterChainTop);
        }
        if (filterChainBottom != null) {
            filterTopBottom.add(filterChainBottom);
        }
        return filterTopBottom;
    }

    // build newJoin node given remaining joinArgs and chain of filters
    private static TupleExpr getNewJoin(List<TupleExpr> args, List<TupleExpr> filterChain) {
        TupleExpr newJoin;
        List<TupleExpr> joinArgs = Lists.newArrayList(args);

        if (joinArgs.size() > 1) {
            if (filterChain.size() > 0) {
                TupleExpr finalJoinArg = joinArgs.remove(0).clone();
                TupleExpr tempJoin;
                TupleExpr temp = filterChain.get(0);

                if (joinArgs.size() > 1) {
                    tempJoin = new Join(joinArgs.remove(0).clone(), joinArgs.remove(0).clone());
                    for (TupleExpr te : joinArgs) {
                        tempJoin = new Join(tempJoin, te.clone());
                    }
                } else {
                    tempJoin = joinArgs.remove(0).clone();
                }

                if (filterChain.size() == 1) {
                    ((Filter) temp).setArg(tempJoin);
                } else {
                    ((Filter) filterChain.get(1)).setArg(tempJoin);
                }
                newJoin = new Join(temp, finalJoinArg);
            } else {
                newJoin = new Join(joinArgs.remove(0).clone(), joinArgs.remove(0).clone());

                for (TupleExpr te : joinArgs) {
                    newJoin = new Join(newJoin, te.clone());
                }
            }
        } else if (joinArgs.size() == 1) {
            if (filterChain.size() > 0) {
                newJoin = filterChain.get(0);
                if (filterChain.size() == 1) {
                    ((Filter) newJoin).setArg(joinArgs.get(0).clone());
                } else {
                    ((Filter) filterChain.get(1)).setArg(joinArgs.get(0).clone());
                }
            } else {
                newJoin = joinArgs.get(0).clone();
            }
        } else {
            throw new IllegalStateException("JoinArgs size cannot be zero.");
        }
        return newJoin;
    }

}
