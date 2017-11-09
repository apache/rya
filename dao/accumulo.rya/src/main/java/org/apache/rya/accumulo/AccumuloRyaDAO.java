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
package org.apache.rya.accumulo;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.accumulo.AccumuloRdfConstants.ALL_AUTHORIZATIONS;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.EMPTY_TEXT;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.INFO_NAMESPACE_TXT;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.MAX_MEMORY;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.MAX_TIME;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.NUM_THREADS;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.RTS_SUBJECT_RYA;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.RTS_VERSION_PREDICATE_RYA;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.VERSION_RYA;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.rya.accumulo.experimental.AccumuloIndexer;
import org.apache.rya.accumulo.query.AccumuloRyaQueryEngine;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.layout.TableLayoutStrategy;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.RyaNamespaceManager;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Namespace;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class AccumuloRyaDAO implements RyaDAO<AccumuloRdfConfiguration>, RyaNamespaceManager<AccumuloRdfConfiguration> {
    private static final Log logger = LogFactory.getLog(AccumuloRyaDAO.class);

    private boolean initialized = false;
    private boolean flushEachUpdate = true;
    private Connector connector;
    private BatchWriterConfig batchWriterConfig;

    private MultiTableBatchWriter mt_bw;

    // Do not flush these individually
    private BatchWriter bw_spo;
    private BatchWriter bw_po;
    private BatchWriter bw_osp;

    private BatchWriter bw_ns;

    private List<AccumuloIndexer> secondaryIndexers;

    private AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
    private RyaTableMutationsFactory ryaTableMutationsFactory;
    private TableLayoutStrategy tableLayoutStrategy;
    private AccumuloRyaQueryEngine queryEngine;
    private RyaTripleContext ryaContext;

    @Override
    public boolean isInitialized() throws RyaDAOException {
        return initialized;
    }

    @Override
    public void init() throws RyaDAOException {
        if (initialized) {
            return;
        }
        try {
            checkNotNull(conf);
            checkNotNull(connector);

            if(batchWriterConfig == null){
                batchWriterConfig = new BatchWriterConfig();
                batchWriterConfig.setMaxMemory(MAX_MEMORY);
                batchWriterConfig.setTimeout(MAX_TIME, TimeUnit.MILLISECONDS);
                batchWriterConfig.setMaxWriteThreads(NUM_THREADS);
            }

            tableLayoutStrategy = conf.getTableLayoutStrategy();
            ryaContext = RyaTripleContext.getInstance(conf);
            ryaTableMutationsFactory = new RyaTableMutationsFactory(ryaContext);

            secondaryIndexers = conf.getAdditionalIndexers();

            flushEachUpdate = conf.flushEachUpdate();

            final TableOperations tableOperations = connector.tableOperations();
            AccumuloRdfUtils.createTableIfNotExist(tableOperations, tableLayoutStrategy.getSpo());
            AccumuloRdfUtils.createTableIfNotExist(tableOperations, tableLayoutStrategy.getPo());
            AccumuloRdfUtils.createTableIfNotExist(tableOperations, tableLayoutStrategy.getOsp());
            AccumuloRdfUtils.createTableIfNotExist(tableOperations, tableLayoutStrategy.getNs());

            for (final AccumuloIndexer index : secondaryIndexers) {
                index.setConf(conf);
            }

            mt_bw = connector.createMultiTableBatchWriter(batchWriterConfig);

            //get the batch writers for tables
            bw_spo = mt_bw.getBatchWriter(tableLayoutStrategy.getSpo());
            bw_po = mt_bw.getBatchWriter(tableLayoutStrategy.getPo());
            bw_osp = mt_bw.getBatchWriter(tableLayoutStrategy.getOsp());

            bw_ns = mt_bw.getBatchWriter(tableLayoutStrategy.getNs());

            for (final AccumuloIndexer index : secondaryIndexers) {
               index.setConnector(connector);
               index.setMultiTableBatchWriter(mt_bw);
               index.init();
            }

            queryEngine = new AccumuloRyaQueryEngine(connector, conf);

            checkVersion();

            initialized = true;
        } catch (final Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
	public String getVersion() throws RyaDAOException {
        String version = null;
        final CloseableIteration<RyaStatement, RyaDAOException> versIter = queryEngine.query(new RyaStatement(RTS_SUBJECT_RYA, RTS_VERSION_PREDICATE_RYA, null), conf);
        if (versIter.hasNext()) {
            version = versIter.next().getObject().getData();
        }
        versIter.close();

        return version;
    }

    @Override
    public void add(final RyaStatement statement) throws RyaDAOException {
        commit(Iterators.singletonIterator(statement));
    }

    @Override
    public void add(final Iterator<RyaStatement> iter) throws RyaDAOException {
        commit(iter);
    }

    @Override
    public void delete(final RyaStatement stmt, final AccumuloRdfConfiguration aconf) throws RyaDAOException {
        this.delete(Iterators.singletonIterator(stmt), aconf);
    }

    @Override
    public void delete(final Iterator<RyaStatement> statements, final AccumuloRdfConfiguration conf) throws RyaDAOException {
        try {
            while (statements.hasNext()) {
                final RyaStatement stmt = statements.next();
                //query first
                final CloseableIteration<RyaStatement, RyaDAOException> query = this.queryEngine.query(stmt, conf);
                while (query.hasNext()) {
                    deleteSingleRyaStatement(query.next());
                }

                for (final AccumuloIndexer index : secondaryIndexers) {
                    index.deleteStatement(stmt);
                }
            }
            if (flushEachUpdate) { mt_bw.flush(); }
        } catch (final Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public void dropGraph(final AccumuloRdfConfiguration conf, final RyaURI... graphs) throws RyaDAOException {
        BatchDeleter bd_spo = null;
        BatchDeleter bd_po = null;
        BatchDeleter bd_osp = null;

        try {
            bd_spo = createBatchDeleter(tableLayoutStrategy.getSpo(), conf.getAuthorizations());
            bd_po = createBatchDeleter(tableLayoutStrategy.getPo(), conf.getAuthorizations());
            bd_osp = createBatchDeleter(tableLayoutStrategy.getOsp(), conf.getAuthorizations());

            bd_spo.setRanges(Collections.singleton(new Range()));
            bd_po.setRanges(Collections.singleton(new Range()));
            bd_osp.setRanges(Collections.singleton(new Range()));

            for (final RyaURI graph : graphs){
                bd_spo.fetchColumnFamily(new Text(graph.getData()));
                bd_po.fetchColumnFamily(new Text(graph.getData()));
                bd_osp.fetchColumnFamily(new Text(graph.getData()));
            }

            bd_spo.delete();
            bd_po.delete();
            bd_osp.delete();

            //TODO indexers do not support delete-UnsupportedOperation Exception will be thrown
//            for (AccumuloIndex index : secondaryIndexers) {
//                index.dropGraph(graphs);
//            }

        } catch (final Exception e) {
            throw new RyaDAOException(e);
        } finally {
            if (bd_spo != null) {
                bd_spo.close();
            }
            if (bd_po != null) {
                bd_po.close();
            }
            if (bd_osp != null) {
                bd_osp.close();
            }
        }

    }

    protected void deleteSingleRyaStatement(final RyaStatement stmt) throws IOException, MutationsRejectedException {
        final Map<TABLE_LAYOUT, Collection<Mutation>> map = ryaTableMutationsFactory.serializeDelete(stmt);
        bw_spo.addMutations(map.get(TABLE_LAYOUT.SPO));
        bw_po.addMutations(map.get(TABLE_LAYOUT.PO));
        bw_osp.addMutations(map.get(TABLE_LAYOUT.OSP));
    }

    protected void commit(final Iterator<RyaStatement> commitStatements) throws RyaDAOException {
        try {
            //TODO: Should have a lock here in case we are adding and committing at the same time
            while (commitStatements.hasNext()) {
                final RyaStatement stmt = commitStatements.next();

                final Map<TABLE_LAYOUT, Collection<Mutation>> mutationMap = ryaTableMutationsFactory.serialize(stmt);
                final Collection<Mutation> spo = mutationMap.get(TABLE_LAYOUT.SPO);
                final Collection<Mutation> po = mutationMap.get(TABLE_LAYOUT.PO);
                final Collection<Mutation> osp = mutationMap.get(TABLE_LAYOUT.OSP);
                bw_spo.addMutations(spo);
                bw_po.addMutations(po);
                bw_osp.addMutations(osp);

                for (final AccumuloIndexer index : secondaryIndexers) {
                    index.storeStatement(stmt);
                }
            }

            if (flushEachUpdate) { mt_bw.flush(); }
        } catch (final Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public void destroy() throws RyaDAOException {
        if (!initialized) {
            return;
        }
        //TODO: write lock
        try {
            initialized = false;
            mt_bw.flush();

            mt_bw.close();
        } catch (final Exception e) {
            throw new RyaDAOException(e);
        }
        for(final AccumuloIndexer indexer : this.secondaryIndexers) {
            try {
                indexer.destroy();
            } catch(final Exception e) {
                logger.warn("Failed to destroy indexer", e);
            }
        }
    }

    @Override
    public void addNamespace(final String pfx, final String namespace) throws RyaDAOException {
        try {
            final Mutation m = new Mutation(new Text(pfx));
            m.put(INFO_NAMESPACE_TXT, EMPTY_TEXT, new Value(namespace.getBytes(StandardCharsets.UTF_8)));
            bw_ns.addMutation(m);
            if (flushEachUpdate) { mt_bw.flush(); }
        } catch (final Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public String getNamespace(final String pfx) throws RyaDAOException {
        try {
            final Scanner scanner = connector.createScanner(tableLayoutStrategy.getNs(),
                    ALL_AUTHORIZATIONS);
            scanner.fetchColumn(INFO_NAMESPACE_TXT, EMPTY_TEXT);
            scanner.setRange(new Range(new Text(pfx)));
            final Iterator<Map.Entry<Key, Value>> iterator = scanner
                    .iterator();

            if (iterator.hasNext()) {
                return new String(iterator.next().getValue().get(), StandardCharsets.UTF_8);
            }
        } catch (final Exception e) {
            throw new RyaDAOException(e);
        }
        return null;
    }

    @Override
    public void removeNamespace(final String pfx) throws RyaDAOException {
        try {
            final Mutation del = new Mutation(new Text(pfx));
            del.putDelete(INFO_NAMESPACE_TXT, EMPTY_TEXT);
            bw_ns.addMutation(del);
            if (flushEachUpdate) { mt_bw.flush(); }
        } catch (final Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public CloseableIteration<Namespace, RyaDAOException> iterateNamespace() throws RyaDAOException {
        try {
            final Scanner scanner = connector.createScanner(tableLayoutStrategy.getNs(),
                    ALL_AUTHORIZATIONS);
            scanner.fetchColumnFamily(INFO_NAMESPACE_TXT);
            final Iterator<Map.Entry<Key, Value>> result = scanner.iterator();
            return new AccumuloNamespaceTableIterator(result);
        } catch (final Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public RyaNamespaceManager<AccumuloRdfConfiguration> getNamespaceManager() {
        return this;
    }

    @Override
    public void purge(final RdfCloudTripleStoreConfiguration configuration) {
        for (final String tableName : getTables()) {
            try {
                purge(tableName, configuration.getAuths());
                compact(tableName);
            } catch (final TableNotFoundException e) {
                logger.error(e.getMessage());
            } catch (final MutationsRejectedException e) {
                logger.error(e.getMessage());
            }
        }
        for(final AccumuloIndexer indexer : this.secondaryIndexers) {
            try {
                indexer.purge(configuration);
            } catch(final Exception e) {
                logger.error("Failed to purge indexer", e);
            }
        }
    }

    @Override
    public void dropAndDestroy() throws RyaDAOException {
        for (final String tableName : getTables()) {
            try {
                drop(tableName);
            } catch (final AccumuloSecurityException e) {
                logger.error(e.getMessage());
                throw new RyaDAOException(e);
            } catch (final AccumuloException e) {
                logger.error(e.getMessage());
                throw new RyaDAOException(e);
            } catch (final TableNotFoundException e) {
                logger.warn(e.getMessage());
            }
        }
        destroy();
        for(final AccumuloIndexer indexer : this.secondaryIndexers) {
            try {
                indexer.dropAndDestroy();
            } catch(final Exception e) {
                logger.error("Failed to drop and destroy indexer", e);
            }
        }
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnector(final Connector connector) {
        this.connector = connector;
    }

    public BatchWriterConfig getBatchWriterConfig(){
        return batchWriterConfig;
    }

    public void setBatchWriterConfig(final BatchWriterConfig batchWriterConfig) {
        this.batchWriterConfig = batchWriterConfig;
    }

    protected MultiTableBatchWriter getMultiTableBatchWriter(){
    	return mt_bw;
    }

    @Override
	public AccumuloRdfConfiguration getConf() {
        return conf;
    }

    @Override
	public void setConf(final AccumuloRdfConfiguration conf) {
        this.conf = conf;
    }

    public RyaTableMutationsFactory getRyaTableMutationsFactory() {
        return ryaTableMutationsFactory;
    }

    public void setRyaTableMutationsFactory(final RyaTableMutationsFactory ryaTableMutationsFactory) {
        this.ryaTableMutationsFactory = ryaTableMutationsFactory;
    }

    @Override
	public AccumuloRyaQueryEngine getQueryEngine() {
        return queryEngine;
    }

    public void setQueryEngine(final AccumuloRyaQueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    @Override
    public void flush() throws RyaDAOException {
        try {
            mt_bw.flush();
            flushIndexers();
        } catch (final MutationsRejectedException e) {
            throw new RyaDAOException(e);
        }
    }

    private void flushIndexers() throws RyaDAOException {
        for (final AccumuloIndexer indexer : secondaryIndexers) {
            try {
                indexer.flush();
            } catch (final IOException e) {
                logger.error("Error flushing data in indexer: " + indexer.getClass().getSimpleName(), e);
            }
        }
    }

    protected String[] getTables() {
        // core tables
        final List<String> tableNames = Lists.newArrayList(
                tableLayoutStrategy.getSpo(),
                tableLayoutStrategy.getPo(),
                tableLayoutStrategy.getOsp(),
                tableLayoutStrategy.getNs(),
                tableLayoutStrategy.getEval());

        // Additional Tables
        for (final AccumuloIndexer index : secondaryIndexers) {
            tableNames.add(index.getTableName());
        }

        return tableNames.toArray(new String[]{});
    }

    private void purge(final String tableName, final String[] auths) throws TableNotFoundException, MutationsRejectedException {
        if (tableExists(tableName)) {
            logger.info("Purging accumulo table: " + tableName);
            final BatchDeleter batchDeleter = createBatchDeleter(tableName, new Authorizations(auths));
            try {
                batchDeleter.setRanges(Collections.singleton(new Range()));
                batchDeleter.delete();
            } finally {
                batchDeleter.close();
            }
        }
    }

    private void compact(final String tableName) {
        logger.info("Requesting major compaction for table " + tableName);
        try {
            connector.tableOperations().compact(tableName, null, null, true, false);
        } catch (final Exception e) {
            logger.error(e.getMessage());
        }
    }

    private boolean tableExists(final String tableName) {
        return getConnector().tableOperations().exists(tableName);
    }

    private BatchDeleter createBatchDeleter(final String tableName, final Authorizations authorizations) throws TableNotFoundException {
        return connector.createBatchDeleter(tableName, authorizations, NUM_THREADS, MAX_MEMORY, MAX_TIME, NUM_THREADS);
    }

    private void checkVersion() throws RyaDAOException, IOException, MutationsRejectedException {
        final String version = getVersion();
        if (version == null) {
            //adding to core Rya tables but not Indexes
            final Map<TABLE_LAYOUT, Collection<Mutation>> mutationMap = ryaTableMutationsFactory.serialize(getVersionRyaStatement());
            final Collection<Mutation> spo = mutationMap.get(TABLE_LAYOUT.SPO);
            final Collection<Mutation> po = mutationMap.get(TABLE_LAYOUT.PO);
            final Collection<Mutation> osp = mutationMap.get(TABLE_LAYOUT.OSP);
            bw_spo.addMutations(spo);
            bw_po.addMutations(po);
            bw_osp.addMutations(osp);
        }
        //TODO: Do a version check here
    }

    protected RyaStatement getVersionRyaStatement() {
        return new RyaStatement(RTS_SUBJECT_RYA, RTS_VERSION_PREDICATE_RYA, VERSION_RYA);
    }

    private void drop(final String tableName) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        logger.info("Dropping cloudbase table: " + tableName);
        connector.tableOperations().delete(tableName);
    }
}
