package org.apache.rya.indexing.accumulo.entity;

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
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.joinselect.SelectivityEvalDAO;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.joinselect.AccumuloSelectivityEvalDAO;
import org.apache.rya.prospector.service.ProspectorServiceEvalStatsDAO;
import org.apache.rya.rdftriplestore.inference.DoNotExpandSP;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class EntityOptimizer implements QueryOptimizer, Configurable {
    private static final Logger LOG = LoggerFactory.getLogger(EntityTupleSet.class);

    private SelectivityEvalDAO<RdfCloudTripleStoreConfiguration> eval;
    private RdfCloudTripleStoreConfiguration conf;
    private boolean isEvalDaoSet = false;


    public EntityOptimizer() {

    }

    public EntityOptimizer(RdfCloudTripleStoreConfiguration conf) {
        if(conf.isUseStats() && conf.isUseSelectivity()) {
            try {
                eval = new AccumuloSelectivityEvalDAO(conf, ConfigUtils.getConnector(conf));
                ((AccumuloSelectivityEvalDAO)eval).setRdfEvalDAO(new ProspectorServiceEvalStatsDAO(ConfigUtils.getConnector(conf), conf));
                eval.init();
            } catch (final AccumuloException | AccumuloSecurityException e) {
                LOG.warn("A problem was encountered while constructing the EntityOptimizer.", e);
            }

            isEvalDaoSet = true;
        } else {
            eval = null;
            isEvalDaoSet = true;
        }
        this.conf = conf;
    }

    public EntityOptimizer(SelectivityEvalDAO<RdfCloudTripleStoreConfiguration> eval) {
        this.eval = eval;
        this.conf = eval.getConf();
        isEvalDaoSet = true;
    }

    @Override
    public void setConf(Configuration conf) {
        if(conf instanceof RdfCloudTripleStoreConfiguration) {
            this.conf = (RdfCloudTripleStoreConfiguration) conf;
        } else {
            this.conf = new AccumuloRdfConfiguration(conf);
        }

        if (!isEvalDaoSet) {
            if(this.conf.isUseStats() && this.conf.isUseSelectivity()) {
                try {
                    eval = new AccumuloSelectivityEvalDAO(this.conf, ConfigUtils.getConnector(this.conf));
                    ((AccumuloSelectivityEvalDAO)eval).setRdfEvalDAO(new ProspectorServiceEvalStatsDAO(ConfigUtils.getConnector(this.conf), this.conf));
                    eval.init();
                } catch (final AccumuloException | AccumuloSecurityException e) {
                    LOG.warn("A problem was encountered while setting the Configuration for the EntityOptimizer.", e);
                }

                isEvalDaoSet = true;
            } else {
                eval = null;
                isEvalDaoSet = true;
            }
        }

    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    /**
     * Applies generally applicable optimizations: path expressions are sorted
     * from more to less specific.
     *
     * @param tupleExpr
     */
    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {
        tupleExpr.visit(new JoinVisitor());
    }

    protected class JoinVisitor extends QueryModelVisitorBase<RuntimeException> {

        @Override
        public void meet(Join node) {
            try {
                if (node.getLeftArg() instanceof FixedStatementPattern && node.getRightArg() instanceof DoNotExpandSP) {
                    return;
                }
                List<TupleExpr> joinArgs = getJoinArgs(node, new ArrayList<TupleExpr>());
                HashMultimap<String, StatementPattern> varMap = getVarBins(joinArgs);
                while (!varMap.keySet().isEmpty()) {
                    String s = getHighestPriorityKey(varMap);
                    constructTuple(varMap, joinArgs, s);
                }
                List<TupleExpr> filterChain = getFilterChain(joinArgs);

                for (TupleExpr te : joinArgs) {
                    if (!(te instanceof StatementPattern) || !(te instanceof EntityTupleSet)) {
                        te.visit(this);
                    }
                }
                // Replace old join hierarchy
                node.replaceWith(getNewJoin(joinArgs, filterChain));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private List<TupleExpr> getFilterChain(List<TupleExpr> joinArgs) {
            List<TupleExpr> filterTopBottom = Lists.newArrayList();
            TupleExpr filterChainTop = null;
            TupleExpr filterChainBottom = null;

            for(int i = 0; i < joinArgs.size(); i++) {
                if(joinArgs.get(i) instanceof Filter) {
                    if(filterChainTop == null) {
                        filterChainTop = joinArgs.remove(i);
                        i--;
                    } else if(filterChainBottom == null){
                        filterChainBottom = joinArgs.remove(i);
                        ((Filter)filterChainTop).setArg(filterChainBottom);
                        i--;
                    } else {
                        ((Filter)filterChainBottom).setArg(joinArgs.remove(i));
                        filterChainBottom = ((Filter)filterChainBottom).getArg();
                        i--;
                    }
                }
            }
            if(filterChainTop != null) {
                filterTopBottom.add(filterChainTop);
            }
            if(filterChainBottom != null) {
                filterTopBottom.add(filterChainBottom);
            }
            return filterTopBottom;
        }

        private TupleExpr getNewJoin(List<TupleExpr> joinArgs, List<TupleExpr> filterChain) {
            TupleExpr newJoin;

            if (joinArgs.size() > 1) {
                if (filterChain.size() > 0) {
                    TupleExpr finalJoinArg = joinArgs.remove(0);
                    TupleExpr tempJoin;
                    TupleExpr temp = filterChain.get(0);

                    if (joinArgs.size() > 1) {
                        tempJoin = new Join(joinArgs.remove(0), joinArgs.remove(0));
                        for (TupleExpr te : joinArgs) {
                            tempJoin = new Join(tempJoin, te);
                        }
                    } else {
                        tempJoin = joinArgs.remove(0);
                    }

                    if (filterChain.size() == 1) {
                        ((Filter) temp).setArg(tempJoin);
                    } else {
                        ((Filter) filterChain.get(1)).setArg(tempJoin);
                    }
                    newJoin = new Join(temp, finalJoinArg);
                } else {
                    newJoin = new Join(joinArgs.get(0), joinArgs.get(1));
                    joinArgs.remove(0);
                    joinArgs.remove(0);

                    for (TupleExpr te : joinArgs) {
                        newJoin = new Join(newJoin, te);
                    }
                }
            } else if (joinArgs.size() == 1) {
                if (filterChain.size() > 0) {
                    newJoin = filterChain.get(0);
                    if (filterChain.size() == 1) {
                        ((Filter) newJoin).setArg(joinArgs.get(0));
                    } else {
                        ((Filter) filterChain.get(1)).setArg(joinArgs.get(0));
                    }
                } else {
                    newJoin = joinArgs.get(0);
                }
            } else {
                throw new IllegalStateException("JoinArgs size cannot be zero.");
            }
            return newJoin;
        }

        private HashMultimap<String, StatementPattern> getVarBins(List<TupleExpr> nodes) {

            HashMultimap<String, StatementPattern> varMap = HashMultimap.create();

            for (QueryModelNode node : nodes) {
                if (node instanceof StatementPattern) {
                    StatementPattern sp = (StatementPattern) node;
                    if (sp.getPredicateVar().isConstant()) {
                        varMap.put(sp.getSubjectVar().getName(), sp);
                        varMap.put(sp.getObjectVar().getName(), sp);
                    }
                }
            }

            removeInvalidBins(varMap, true);

            return varMap;
        }

        private void updateVarMap(HashMultimap<String, StatementPattern> varMap, Set<StatementPattern> bin) {

            for (StatementPattern sp : bin) {
                varMap.remove(sp.getSubjectVar().getName(), sp);
                varMap.remove(sp.getObjectVar().getName(), sp);
            }

            removeInvalidBins(varMap, false);

        }

        private void removeInvalidBins(HashMultimap<String, StatementPattern> varMap, boolean newMap) {

            Set<String> keys = Sets.newHashSet(varMap.keySet());

            if (newMap) {
                for (String s : keys) {
                    Set<StatementPattern> spSet = Sets.newHashSet(varMap.get(s));
                    if (!StarQuery.isValidStarQuery(spSet)) {
                        for (StatementPattern sp : spSet) {
                            varMap.remove(s, sp);
                        }
                    }

                }
            } else {

                for (String s : keys) {
                    Set<StatementPattern> spSet = Sets.newHashSet(varMap.get(s));
                    if (spSet.size() == 1) {
                        for (StatementPattern sp : spSet) {
                            varMap.remove(s, sp);
                        }
                    }

                }
            }

        }

        private void constructTuple(HashMultimap<String, StatementPattern> varMap, List<TupleExpr> joinArgs,
                String binName) {

            Set<StatementPattern> bin = Sets.newHashSet(varMap.get(binName));
            StarQuery sq = new StarQuery(bin);

            updateVarMap(varMap, bin);
            for (StatementPattern sp : bin) {
                joinArgs.remove(sp);
            }

            joinArgs.add(new EntityTupleSet(sq, conf));

        }

        private String getHighestPriorityKey(HashMultimap<String, StatementPattern> varMap) {

            double tempPriority = -1;
            double priority = -Double.MAX_VALUE;
            String priorityKey = "";
            Set<StatementPattern> bin = null;

            Set<String> keys = varMap.keySet();

            for (String s : keys) {
                bin = varMap.get(s);
                tempPriority = bin.size();
                tempPriority *= getCardinality(bin);
                tempPriority *= getMinCardSp(bin);

                // weight starQuery where common Var is constant slightly more -- this factor is subject
                // to change
                if(s.startsWith("-const-")) {
                    tempPriority *= 10;
                }
                if (tempPriority > priority) {
                    priority = tempPriority;
                    priorityKey = s;
                }
            }
            return priorityKey;
        }

        private double getMinCardSp(Collection<StatementPattern> nodes) {

            double cardinality = Double.MAX_VALUE;
            double tempCard = -1;

            if (eval == null) {
                return 1;
            }

            for (StatementPattern sp : nodes) {

                try {
                    tempCard = eval.getCardinality(conf, sp);

                    if (tempCard < cardinality) {
                        cardinality = tempCard;

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            return cardinality;

        }

        private double getCardinality(Collection<StatementPattern> spNodes) {

            double cardinality = Double.MAX_VALUE;
            double tempCard = -1;


            if(eval == null) {
                return 1;
            }

            List<StatementPattern> nodes = Lists.newArrayList(spNodes);

            AccumuloSelectivityEvalDAO ase = (AccumuloSelectivityEvalDAO) eval;
            ase.setDenormalized(true);

            try {

                for (int i = 0; i < nodes.size(); i++) {
                    for (int j = i + 1; j < nodes.size(); j++) {
                        tempCard = ase.getJoinSelect(conf, nodes.get(i), nodes.get(j));
                        if (tempCard < cardinality) {
                            cardinality = tempCard;
                        }
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            ase.setDenormalized(false);

            return cardinality / (nodes.size() + 1);

        }

        protected <L extends List<TupleExpr>> L getJoinArgs(TupleExpr tupleExpr, L joinArgs) {
            if (tupleExpr instanceof Join) {
                if (!(((Join) tupleExpr).getLeftArg() instanceof FixedStatementPattern)
                        && !(((Join) tupleExpr).getRightArg() instanceof DoNotExpandSP)) {
                    Join join = (Join) tupleExpr;
                    getJoinArgs(join.getLeftArg(), joinArgs);
                    getJoinArgs(join.getRightArg(), joinArgs);
                }
            } else if(tupleExpr instanceof Filter) {
                joinArgs.add(tupleExpr);
                getJoinArgs(((Filter)tupleExpr).getArg(), joinArgs);
            } else {
                joinArgs.add(tupleExpr);
            }

            return joinArgs;
        }

    }



}
