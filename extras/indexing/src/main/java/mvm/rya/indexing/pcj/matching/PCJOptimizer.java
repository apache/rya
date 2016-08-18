package mvm.rya.indexing.pcj.matching;

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

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.indexing.IndexPlanValidator.IndexPlanValidator;
import mvm.rya.indexing.IndexPlanValidator.IndexedExecutionPlanGenerator;
import mvm.rya.indexing.IndexPlanValidator.ThreshholdPlanSelector;
import mvm.rya.indexing.IndexPlanValidator.TupleReArranger;
import mvm.rya.indexing.IndexPlanValidator.ValidIndexCombinationGenerator;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.external.accumulo.AccumuloPcjStorage;
import mvm.rya.indexing.external.tupleSet.AccumuloIndexSet;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.sail.SailException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * {@link QueryOptimizer} which matches {@link TupleExpr}s associated with
 * pre-computed queries to sub-queries of a given query. Each matched sub-query
 * is replaced by an {@link ExternalTupleSet} node to delegate that portion of
 * the query to the pre-computed query index.
 * <p>
 *
 * A query is be broken up into {@link QuerySegment}s. Pre-computed query
 * indices, or {@link ExternalTupleset} objects, are compared against the
 * {@link QueryModelNode}s in each QuerySegment. If an ExternalTupleSets nodes
 * match a subset of the given QuerySegments nodes, those nodes are replaced by
 * the ExternalTupleSet in the QuerySegment.
 *
 */
public class PCJOptimizer implements QueryOptimizer, Configurable {

    private static final Logger log = Logger.getLogger(PCJOptimizer.class);
    private List<ExternalTupleSet> indexSet;
    private Configuration conf;
    private boolean init = false;

    public PCJOptimizer() {
    }

    public PCJOptimizer(Configuration conf) {
        this.conf = conf;
        try {
            indexSet = PCJOptimizerUtilities.getValidPCJs(getAccIndices(conf)); // TODO
            // validate
            // PCJs
            // during
            // table
            // creation
        } catch (MalformedQueryException | SailException
                | QueryEvaluationException | TableNotFoundException
                | AccumuloException | AccumuloSecurityException | PcjException e) {
            e.printStackTrace();
        }
        init = true;
    }

    public PCJOptimizer(List<ExternalTupleSet> indices, boolean useOptimalPcj) {
        this.indexSet = PCJOptimizerUtilities.getValidPCJs(indices);
        conf = new Configuration();
        conf.setBoolean(ConfigUtils.USE_OPTIMAL_PCJ, useOptimalPcj);
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        if (!init) {
            try {
                indexSet = PCJOptimizerUtilities
                        .getValidPCJs(getAccIndices(conf));
            } catch (MalformedQueryException | SailException
                    | QueryEvaluationException | TableNotFoundException
                    | AccumuloException | AccumuloSecurityException
                    | PcjException e) {
                throw new Error(e);
            }
            init = true;
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    /**
     * This method optimizes a specified query by matching subsets of it with
     * PCJ queries.
     *
     * @param tupleExpr
     *            - the query to be optimized
     */
    @Override
    public void optimize(TupleExpr tupleExpr, Dataset dataset,
            BindingSet bindings) {

        Projection projection = PCJOptimizerUtilities.getProjection(tupleExpr);
        if (projection == null) {
            log.debug("TupleExpr has no Projection.  Invalid TupleExpr.");
            return;
        }
        IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
                tupleExpr, indexSet);
        List<ExternalTupleSet> pcjs = iep.getNormalizedIndices();
        // first standardize query by pulling all filters to top of query if
        // they exist
        // using TopOfQueryFilterRelocator
        tupleExpr = TopOfQueryFilterRelocator.moveFiltersToTop(tupleExpr);

        if (ConfigUtils.getUseOptimalPCJ(conf) && pcjs.size() > 0) {

            // get potential relevant index combinations
            ValidIndexCombinationGenerator vic = new ValidIndexCombinationGenerator(
                    tupleExpr);
            Iterator<List<ExternalTupleSet>> iter = vic
                    .getValidIndexCombos(pcjs);
            TupleExpr bestTup = null;
            TupleExpr tempTup = null;
            double tempCost = 0;
            double minCost = Double.MAX_VALUE;

            while (iter.hasNext()) {
                // apply join visitor to place external index nodes in query
                TupleExpr clone = tupleExpr.clone();
                QuerySegmentPCJMatchVisitor.matchPCJs(clone, iter.next());

                // get all valid execution plans for given external index
                // combination by considering all
                // permutations of nodes in TupleExpr
                IndexPlanValidator ipv = new IndexPlanValidator(false);
                Iterator<TupleExpr> validTups = ipv
                        .getValidTuples(TupleReArranger.getTupleReOrderings(
                                clone).iterator());

                // set valid plan according to a specified cost threshold, where
                // cost depends on specified weights
                // for number of external index nodes, common variables among
                // joins in execution plan, and number of
                // external products in execution plan
                ThreshholdPlanSelector tps = new ThreshholdPlanSelector(
                        tupleExpr);
                tempTup = tps.getThreshholdQueryPlan(validTups, .4, .5, .2, .3);

                // choose best threshhold TupleExpr among all index node
                // combinations
                tempCost = tps.getCost(tempTup, .5, .2, .3);
                if (tempCost < minCost) {
                    minCost = tempCost;
                    bestTup = tempTup;
                }
            }
            if (bestTup != null) {
                Projection bestTupProject = PCJOptimizerUtilities
                        .getProjection(bestTup);
                projection.setArg(bestTupProject.getArg());
            }
            return;
        } else if (pcjs.size() > 0) {
            QuerySegmentPCJMatchVisitor.matchPCJs(tupleExpr, pcjs);
        } else {
            return;
        }
    }

    /**
     * This visitor navigates query until it reaches either a Join, Filter, or
     * LeftJoin. Once it reaches this node, it gets the appropriate PCJMatcher
     * from the {@link QuerySegmentPCJMatchVisitor} and uses this to match each
     * of the PCJs to the {@link QuerySegment} starting with the Join, Filter,
     * or LeftJoin. Once each PCJ has been compared for matching, the portion of
     * the query starting with the Join, Filter, or LeftJoin is replaced by the
     * {@link TupleExpr} returned by {@link PCJMatcher#getQuery()}.  This visitor
     * then visits each of the nodes returned by {@link PCJMatcher#getUnmatchedArgs()}.
     *
     */
    static class QuerySegmentPCJMatchVisitor extends
    QueryModelVisitorBase<RuntimeException> {

        private static List<ExternalTupleSet> pcjs;
        private static final QuerySegmentPCJMatchVisitor INSTANCE = new QuerySegmentPCJMatchVisitor();

        private QuerySegmentPCJMatchVisitor() {
        };

        public static void matchPCJs(TupleExpr te,
                List<ExternalTupleSet> indexSet) {
            pcjs = indexSet;
            te.visit(INSTANCE);
        }

        @Override
        public void meet(Join node) {
            PCJMatcher matcher = PCJMatcherFactory.getPCJMatcher(node);
            for (ExternalTupleSet pcj : pcjs) {
                matcher.matchPCJ(pcj);
            }

            node.replaceWith(matcher.getQuery());
            Set<TupleExpr> unmatched = matcher.getUnmatchedArgs();
            PCJOptimizerUtilities.relocateFilters(matcher.getFilters());

            for (TupleExpr tupleExpr : unmatched) {
                tupleExpr.visit(this);
            }
        }

        @Override
        public void meet(LeftJoin node) {
            PCJMatcher matcher = PCJMatcherFactory.getPCJMatcher(node);
            for (ExternalTupleSet pcj : pcjs) {
                matcher.matchPCJ(pcj);
            }

            node.replaceWith(matcher.getQuery());
            Set<TupleExpr> unmatched = matcher.getUnmatchedArgs();
            PCJOptimizerUtilities.relocateFilters(matcher.getFilters());

            for (TupleExpr tupleExpr : unmatched) {
                tupleExpr.visit(this);
            }
        }

        @Override
        public void meet(Filter node) {
            PCJMatcher matcher = PCJMatcherFactory.getPCJMatcher(node);
            for (ExternalTupleSet pcj : pcjs) {
                matcher.matchPCJ(pcj);
            }

            node.replaceWith(matcher.getQuery());
            Set<TupleExpr> unmatched = matcher.getUnmatchedArgs();
            PCJOptimizerUtilities.relocateFilters(matcher.getFilters());

            for (TupleExpr tupleExpr : unmatched) {
                tupleExpr.visit(this);
            }
        }

    }

    /**
     *
     *
     * @param conf
     *            - client configuration
     *
     * @return - list of {@link ExternalTupleSet}s or PCJs that are either
     *         specified by user in Configuration or exist in system.
     *
     * @throws MalformedQueryException
     * @throws SailException
     * @throws QueryEvaluationException
     * @throws TableNotFoundException
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws PcjException
     */
    private static List<ExternalTupleSet> getAccIndices(Configuration conf)
            throws MalformedQueryException, SailException,
            QueryEvaluationException, TableNotFoundException,
            AccumuloException, AccumuloSecurityException, PcjException {

        requireNonNull(conf);
        String tablePrefix = requireNonNull(conf
                .get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX));
        Connector conn = requireNonNull(ConfigUtils.getConnector(conf));
        List<String> tables = null;

        if (conf instanceof RdfCloudTripleStoreConfiguration) {
            tables = ((RdfCloudTripleStoreConfiguration) conf).getPcjTables();
        }
        // this maps associates pcj table name with pcj sparql query
        Map<String, String> indexTables = Maps.newLinkedHashMap();
        PrecomputedJoinStorage storage = new AccumuloPcjStorage(conn, tablePrefix);
        PcjTableNameFactory pcjFactory = new PcjTableNameFactory();

        boolean tablesProvided = tables != null && !tables.isEmpty();

        if (tablesProvided) {
            //if tables provided, associate table name with sparql
            for (final String table : tables) {
                indexTables.put(table, storage.getPcjMetadata(pcjFactory.getPcjId(table)).getSparql());
            }
        } else {
            //if no tables are provided by user, get ids for rya instance id, create table name,
            //and associate table name with sparql
            // TODO: storage.listPcjs() returns tablenames, not PCJ-IDs.  
            //     See mvm.rya.indexing.external.accumulo.AccumuloPcjStorage.dropPcj(String)
            List<String> ids = storage.listPcjs();
            for(String id: ids) {
                indexTables.put(id, storage.getPcjMetadata(id).getSparql());
            }
        }

        //use table name sparql map (indexTables) to create {@link AccumuloIndexSet}
        final List<ExternalTupleSet> index = Lists.newArrayList();
        if (indexTables.isEmpty()) {
            System.out.println("No Index found");
        } else {
            for (final String table : indexTables.keySet()) {
                final String indexSparqlString = indexTables.get(table);
                index.add(new AccumuloIndexSet(indexSparqlString, conf, table));
            }
        }
        return index;
    }

}
