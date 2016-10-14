package org.apache.rya.indexing.pcj.matching;

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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
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

import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.indexing.IndexPlanValidator.IndexPlanValidator;
import org.apache.rya.indexing.IndexPlanValidator.IndexedExecutionPlanGenerator;
import org.apache.rya.indexing.IndexPlanValidator.ThreshholdPlanSelector;
import org.apache.rya.indexing.IndexPlanValidator.TupleReArranger;
import org.apache.rya.indexing.IndexPlanValidator.ValidIndexCombinationGenerator;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.tupleSet.AccumuloIndexSet;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;

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

    public PCJOptimizer(final Configuration conf) {
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
            log.error(e.getMessage(), e);
        }
        init = true;
    }

    public PCJOptimizer(final List<ExternalTupleSet> indices, final boolean useOptimalPcj) {
        this.indexSet = PCJOptimizerUtilities.getValidPCJs(indices);
        conf = new Configuration();
        conf.setBoolean(ConfigUtils.USE_OPTIMAL_PCJ, useOptimalPcj);
    }

    @Override
    public void setConf(final Configuration conf) {
        this.conf = conf;
        if (!init) {
            try {
                indexSet = PCJOptimizerUtilities.getValidPCJs(getAccIndices(conf));
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
    public void optimize(TupleExpr tupleExpr, final Dataset dataset,
            final BindingSet bindings) {

        final Projection projection = PCJOptimizerUtilities.getProjection(tupleExpr);
        if (projection == null) {
            log.debug("TupleExpr has no Projection.  Invalid TupleExpr.");
            return;
        }
        final IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
                tupleExpr, indexSet);
        final List<ExternalTupleSet> pcjs = iep.getNormalizedIndices();
        // first standardize query by pulling all filters to top of query if
        // they exist
        // using TopOfQueryFilterRelocator
        tupleExpr = TopOfQueryFilterRelocator.moveFiltersToTop(tupleExpr);

        if (ConfigUtils.getUseOptimalPCJ(conf) && pcjs.size() > 0) {

            // get potential relevant index combinations
            final ValidIndexCombinationGenerator vic = new ValidIndexCombinationGenerator(
                    tupleExpr);
            final Iterator<List<ExternalTupleSet>> iter = vic
                    .getValidIndexCombos(pcjs);
            TupleExpr bestTup = null;
            TupleExpr tempTup = null;
            double tempCost = 0;
            double minCost = Double.MAX_VALUE;

            while (iter.hasNext()) {
                // apply join visitor to place external index nodes in query
                final TupleExpr clone = tupleExpr.clone();
                QuerySegmentPCJMatchVisitor.matchPCJs(clone, iter.next());

                // get all valid execution plans for given external index
                // combination by considering all
                // permutations of nodes in TupleExpr
                final IndexPlanValidator ipv = new IndexPlanValidator(false);
                final Iterator<TupleExpr> validTups = ipv
                        .getValidTuples(TupleReArranger.getTupleReOrderings(
                                clone).iterator());

                // set valid plan according to a specified cost threshold, where
                // cost depends on specified weights
                // for number of external index nodes, common variables among
                // joins in execution plan, and number of
                // external products in execution plan
                final ThreshholdPlanSelector tps = new ThreshholdPlanSelector(
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
                final Projection bestTupProject = PCJOptimizerUtilities
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

        public static void matchPCJs(final TupleExpr te,
                final List<ExternalTupleSet> indexSet) {
            pcjs = indexSet;
            te.visit(INSTANCE);
        }

        @Override
        public void meet(final Join node) {
            final PCJMatcher matcher = PCJMatcherFactory.getPCJMatcher(node);
            for (final ExternalTupleSet pcj : pcjs) {
                matcher.matchPCJ(pcj);
            }

            node.replaceWith(matcher.getQuery());
            final Set<TupleExpr> unmatched = matcher.getUnmatchedArgs();
            PCJOptimizerUtilities.relocateFilters(matcher.getFilters());

            for (final TupleExpr tupleExpr : unmatched) {
                tupleExpr.visit(this);
            }
        }

        @Override
        public void meet(final LeftJoin node) {
            final PCJMatcher matcher = PCJMatcherFactory.getPCJMatcher(node);
            for (final ExternalTupleSet pcj : pcjs) {
                matcher.matchPCJ(pcj);
            }

            node.replaceWith(matcher.getQuery());
            final Set<TupleExpr> unmatched = matcher.getUnmatchedArgs();
            PCJOptimizerUtilities.relocateFilters(matcher.getFilters());

            for (final TupleExpr tupleExpr : unmatched) {
                tupleExpr.visit(this);
            }
        }

        @Override
        public void meet(final Filter node) {
            final PCJMatcher matcher = PCJMatcherFactory.getPCJMatcher(node);
            for (final ExternalTupleSet pcj : pcjs) {
                matcher.matchPCJ(pcj);
            }

            node.replaceWith(matcher.getQuery());
            final Set<TupleExpr> unmatched = matcher.getUnmatchedArgs();
            PCJOptimizerUtilities.relocateFilters(matcher.getFilters());

            for (final TupleExpr tupleExpr : unmatched) {
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
    private static List<ExternalTupleSet> getAccIndices(final Configuration conf)
            throws MalformedQueryException, SailException,
            QueryEvaluationException, TableNotFoundException,
            AccumuloException, AccumuloSecurityException, PcjException {

        requireNonNull(conf);
        final String tablePrefix = requireNonNull(conf.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX));
        final Connector conn = requireNonNull(ConfigUtils.getConnector(conf));
        List<String> tables = null;

        if (conf instanceof RdfCloudTripleStoreConfiguration) {
            tables = ((RdfCloudTripleStoreConfiguration) conf).getPcjTables();
        }
        // this maps associates pcj table name with pcj sparql query
        final Map<String, String> indexTables = Maps.newLinkedHashMap();
        final PrecomputedJoinStorage storage = new AccumuloPcjStorage(conn, tablePrefix);
        final PcjTableNameFactory pcjFactory = new PcjTableNameFactory();

        final boolean tablesProvided = tables != null && !tables.isEmpty();

        if (tablesProvided) {
            //if tables provided, associate table name with sparql
            for (final String table : tables) {
                indexTables.put(table, storage.getPcjMetadata(pcjFactory.getPcjId(table)).getSparql());
            }
        } else if(hasRyaDetails(tablePrefix, conn)) {
            // If this is a newer install of Rya, and it has PCJ Details, then use those.
            final List<String> ids = storage.listPcjs();
            for(final String id: ids) {
                indexTables.put(pcjFactory.makeTableName(tablePrefix, id), storage.getPcjMetadata(id).getSparql());
            }
        } else {
            // Otherwise figure it out by scanning tables.
            final PcjTables pcjTables = new PcjTables();
            for(final String table : conn.tableOperations().list()) {
                if(table.startsWith(tablePrefix + "INDEX")) {
                    indexTables.put(table, pcjTables.getPcjMetadata(conn, table).getSparql());
                }
            }
        }

        //use table name sparql map (indexTables) to create {@link AccumuloIndexSet}
        final List<ExternalTupleSet> index = Lists.newArrayList();
        if (indexTables.isEmpty()) {
            log.info("No Index found");
        } else {
            for (final String table : indexTables.keySet()) {
                final String indexSparqlString = indexTables.get(table);
                index.add(new AccumuloIndexSet(indexSparqlString, conf, table));
            }
        }
        return index;
    }

    private static boolean hasRyaDetails(final String ryaInstanceName, final Connector conn) {
        final RyaDetailsRepository detailsRepo = new AccumuloRyaInstanceDetailsRepository(conn, ryaInstanceName);
        try {
            detailsRepo.getRyaInstanceDetails();
            return true;
        } catch(final RyaDetailsRepositoryException e) {
            return false;
        }
    }
}
