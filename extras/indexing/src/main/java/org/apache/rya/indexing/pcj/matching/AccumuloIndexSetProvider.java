/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.pcj.matching;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.instance.RyaDetailsRepository;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;
import org.apache.rya.indexing.IndexPlanValidator.IndexedExecutionPlanGenerator;
import org.apache.rya.indexing.IndexPlanValidator.ValidIndexCombinationGenerator;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.matching.ExternalSetProvider;
import org.apache.rya.indexing.external.matching.QuerySegment;
import org.apache.rya.indexing.external.tupleSet.AccumuloIndexSet;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.sail.SailException;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link ExternalSetProvider} that provides {@link ExternalTupleSet}s.
 * This provider uses either user specified Accumulo configuration information or user a specified
 * List of ExternalTupleSets to populate an internal cache of ExternalTupleSets.  If Accumulo configuration
 * is provided, the provider connects to an instance of RyaDetails and populates the cache with
 * PCJs registered in RyaDetails.
 *
 */
public class AccumuloIndexSetProvider implements ExternalSetProvider<ExternalTupleSet> {

    private static final Logger log = Logger.getLogger(ExternalSetProvider.class);
    private static final PCJToSegmentConverter converter = new PCJToSegmentConverter();
    private List<ExternalTupleSet> indexCache;
    private final Configuration conf;
    private boolean init = false;

    public AccumuloIndexSetProvider(final Configuration conf) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
    }

    public AccumuloIndexSetProvider(final Configuration conf, final List<ExternalTupleSet> indices) {
        Preconditions.checkNotNull(conf);
        this.conf = conf;
        indexCache = indices;
        init = true;
    }

    /**
     *
     * @return - size of underlying PCJ cache
     * @throws Exception
     */
    public int size() throws Exception {
        if(!init) {
            indexCache = PCJOptimizerUtilities.getValidPCJs(getAccIndices());
            init = true;
        }
        return indexCache.size();
    }

    /**
     * @param segment - QuerySegment used to get relevant queries form index cache for matching
     * @return List of PCJs for matching
     */
    @Override
    public List<ExternalTupleSet> getExternalSets(final QuerySegment<ExternalTupleSet> segment) {
        try {
            if(!init) {
                indexCache = PCJOptimizerUtilities.getValidPCJs(getAccIndices());
                init = true;
            }
            final TupleExpr query = segment.getQuery().getTupleExpr();
            final IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(query, indexCache);
            final List<ExternalTupleSet> pcjs = iep.getNormalizedIndices();
            final List<ExternalTupleSet> tuples = new ArrayList<>();
            for (final ExternalTupleSet tuple: pcjs) {
                final QuerySegment<ExternalTupleSet> pcj = converter.setToSegment(tuple);
                if (segment.containsQuerySegment(pcj)) {
                    tuples.add(tuple);
                }
            }
            return tuples;

        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param segment - QuerySegment used to get relevant queries form index cache for matching
     *
     * @return Iterator of Lists (combos) of PCJs used to build an optimal query plan
     */
    @Override
    public Iterator<List<ExternalTupleSet>> getExternalSetCombos(final QuerySegment<ExternalTupleSet> segment) {
        final ValidIndexCombinationGenerator comboGen = new ValidIndexCombinationGenerator(segment.getOrderedNodes());
        return comboGen.getValidIndexCombos(getExternalSets(segment));
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
    private List<ExternalTupleSet> getAccIndices() throws Exception {

        requireNonNull(conf);
        final String tablePrefix = requireNonNull(conf.get(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX));
        final Connector conn = requireNonNull(ConfigUtils.getConnector(conf));
        List<String> tables = null;

        if (conf instanceof RdfCloudTripleStoreConfiguration) {
            tables = ((RdfCloudTripleStoreConfiguration) conf).getPcjTables();
        }
        // this maps associates pcj table name with pcj sparql query
        final Map<String, String> indexTables = Maps.newLinkedHashMap();

        try(final PrecomputedJoinStorage storage = new AccumuloPcjStorage(conn, tablePrefix)) {
            final PcjTableNameFactory pcjFactory = new PcjTableNameFactory();

            final boolean tablesProvided = tables != null && !tables.isEmpty();

            if (tablesProvided) {
                // if tables provided, associate table name with sparql
                for (final String table : tables) {
                    indexTables.put(table, storage.getPcjMetadata(pcjFactory.getPcjId(table)).getSparql());
                }
            } else if (hasRyaDetails(tablePrefix, conn)) {
                // If this is a newer install of Rya, and it has PCJ Details, then
                // use those.
                final List<String> ids = storage.listPcjs();
                for (final String id : ids) {
                    indexTables.put(pcjFactory.makeTableName(tablePrefix, id), storage.getPcjMetadata(id).getSparql());
                }
            } else {
                // Otherwise figure it out by scanning tables.
                final PcjTables pcjTables = new PcjTables();
                for (final String table : conn.tableOperations().list()) {
                    if (table.startsWith(tablePrefix + "INDEX")) {
                        indexTables.put(table, pcjTables.getPcjMetadata(conn, table).getSparql());
                    }
                }
            }
        }

        // use table name sparql map (indexTables) to create {@link
        // AccumuloIndexSet}
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
        } catch (final RyaDetailsRepositoryException e) {
            return false;
        }
    }


}
