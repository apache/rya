package org.apache.rya.accumulo;

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
import org.openrdf.model.Namespace;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import info.aduna.iteration.CloseableIteration;
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

            TableOperations tableOperations = connector.tableOperations();
            AccumuloRdfUtils.createTableIfNotExist(tableOperations, tableLayoutStrategy.getSpo());
            AccumuloRdfUtils.createTableIfNotExist(tableOperations, tableLayoutStrategy.getPo());
            AccumuloRdfUtils.createTableIfNotExist(tableOperations, tableLayoutStrategy.getOsp());
            AccumuloRdfUtils.createTableIfNotExist(tableOperations, tableLayoutStrategy.getNs());

            for (AccumuloIndexer index : secondaryIndexers) {
                index.setConf(conf);
            }

            mt_bw = connector.createMultiTableBatchWriter(batchWriterConfig);

            //get the batch writers for tables
            bw_spo = mt_bw.getBatchWriter(tableLayoutStrategy.getSpo());
            bw_po = mt_bw.getBatchWriter(tableLayoutStrategy.getPo());
            bw_osp = mt_bw.getBatchWriter(tableLayoutStrategy.getOsp());

            bw_ns = mt_bw.getBatchWriter(tableLayoutStrategy.getNs());

            for (AccumuloIndexer index : secondaryIndexers) {
               index.setConnector(connector);
               index.setMultiTableBatchWriter(mt_bw);
               index.init();
            }

            queryEngine = new AccumuloRyaQueryEngine(connector, conf);

            checkVersion();

            initialized = true;
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
	public String getVersion() throws RyaDAOException {
        String version = null;
        CloseableIteration<RyaStatement, RyaDAOException> versIter = queryEngine.query(new RyaStatement(RTS_SUBJECT_RYA, RTS_VERSION_PREDICATE_RYA, null), conf);
        if (versIter.hasNext()) {
            version = versIter.next().getObject().getData();
        }
        versIter.close();

        return version;
    }

    @Override
    public void add(RyaStatement statement) throws RyaDAOException {
        commit(Iterators.singletonIterator(statement));
    }

    @Override
    public void add(Iterator<RyaStatement> iter) throws RyaDAOException {
        commit(iter);
    }

    @Override
    public void delete(RyaStatement stmt, AccumuloRdfConfiguration aconf) throws RyaDAOException {
        this.delete(Iterators.singletonIterator(stmt), aconf);
    }

    @Override
    public void delete(Iterator<RyaStatement> statements, AccumuloRdfConfiguration conf) throws RyaDAOException {
        try {
            while (statements.hasNext()) {
                RyaStatement stmt = statements.next();
                //query first
                CloseableIteration<RyaStatement, RyaDAOException> query = this.queryEngine.query(stmt, conf);
                while (query.hasNext()) {
                    deleteSingleRyaStatement(query.next());
                }

                for (AccumuloIndexer index : secondaryIndexers) {
                    index.deleteStatement(stmt);
                }
            }
            if (flushEachUpdate) { mt_bw.flush(); }
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public void dropGraph(AccumuloRdfConfiguration conf, RyaURI... graphs) throws RyaDAOException {
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

            for (RyaURI graph : graphs){
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

        } catch (Exception e) {
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

    protected void deleteSingleRyaStatement(RyaStatement stmt) throws IOException, MutationsRejectedException {
        Map<TABLE_LAYOUT, Collection<Mutation>> map = ryaTableMutationsFactory.serializeDelete(stmt);
        bw_spo.addMutations(map.get(TABLE_LAYOUT.SPO));
        bw_po.addMutations(map.get(TABLE_LAYOUT.PO));
        bw_osp.addMutations(map.get(TABLE_LAYOUT.OSP));
    }

    protected void commit(Iterator<RyaStatement> commitStatements) throws RyaDAOException {
        try {
            //TODO: Should have a lock here in case we are adding and committing at the same time
            while (commitStatements.hasNext()) {
                RyaStatement stmt = commitStatements.next();

                Map<TABLE_LAYOUT, Collection<Mutation>> mutationMap = ryaTableMutationsFactory.serialize(stmt);
                Collection<Mutation> spo = mutationMap.get(TABLE_LAYOUT.SPO);
                Collection<Mutation> po = mutationMap.get(TABLE_LAYOUT.PO);
                Collection<Mutation> osp = mutationMap.get(TABLE_LAYOUT.OSP);
                bw_spo.addMutations(spo);
                bw_po.addMutations(po);
                bw_osp.addMutations(osp);

                for (AccumuloIndexer index : secondaryIndexers) {
                    index.storeStatement(stmt);
                }
            }

            if (flushEachUpdate) { mt_bw.flush(); }
        } catch (Exception e) {
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
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
        for(AccumuloIndexer indexer : this.secondaryIndexers) {
            try {
                indexer.destroy();
            } catch(Exception e) {
                logger.warn("Failed to destroy indexer", e);
            }
        }
    }

    @Override
    public void addNamespace(String pfx, String namespace) throws RyaDAOException {
        try {
            Mutation m = new Mutation(new Text(pfx));
            m.put(INFO_NAMESPACE_TXT, EMPTY_TEXT, new Value(namespace.getBytes()));
            bw_ns.addMutation(m);
            if (flushEachUpdate) { mt_bw.flush(); }
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public String getNamespace(String pfx) throws RyaDAOException {
        try {
            Scanner scanner = connector.createScanner(tableLayoutStrategy.getNs(),
                    ALL_AUTHORIZATIONS);
            scanner.fetchColumn(INFO_NAMESPACE_TXT, EMPTY_TEXT);
            scanner.setRange(new Range(new Text(pfx)));
            Iterator<Map.Entry<Key, Value>> iterator = scanner
                    .iterator();

            if (iterator.hasNext()) {
                return new String(iterator.next().getValue().get());
            }
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
        return null;
    }

    @Override
    public void removeNamespace(String pfx) throws RyaDAOException {
        try {
            Mutation del = new Mutation(new Text(pfx));
            del.putDelete(INFO_NAMESPACE_TXT, EMPTY_TEXT);
            bw_ns.addMutation(del);
            if (flushEachUpdate) { mt_bw.flush(); }
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public CloseableIteration<Namespace, RyaDAOException> iterateNamespace() throws RyaDAOException {
        try {
            Scanner scanner = connector.createScanner(tableLayoutStrategy.getNs(),
                    ALL_AUTHORIZATIONS);
            scanner.fetchColumnFamily(INFO_NAMESPACE_TXT);
            Iterator<Map.Entry<Key, Value>> result = scanner.iterator();
            return new AccumuloNamespaceTableIterator(result);
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public RyaNamespaceManager<AccumuloRdfConfiguration> getNamespaceManager() {
        return this;
    }

    @Override
    public void purge(RdfCloudTripleStoreConfiguration configuration) {
        for (String tableName : getTables()) {
            try {
                purge(tableName, configuration.getAuths());
                compact(tableName);
            } catch (TableNotFoundException e) {
                logger.error(e.getMessage());
            } catch (MutationsRejectedException e) {
                logger.error(e.getMessage());
            }
        }
        for(AccumuloIndexer indexer : this.secondaryIndexers) {
            try {
                indexer.purge(configuration);
            } catch(Exception e) {
                logger.error("Failed to purge indexer", e);
            }
        }
    }

    @Override
    public void dropAndDestroy() throws RyaDAOException {
        for (String tableName : getTables()) {
            try {
                drop(tableName);
            } catch (AccumuloSecurityException e) {
                logger.error(e.getMessage());
                throw new RyaDAOException(e);
            } catch (AccumuloException e) {
                logger.error(e.getMessage());
                throw new RyaDAOException(e);
            } catch (TableNotFoundException e) {
                logger.warn(e.getMessage());
            }
        }
        destroy();
        for(AccumuloIndexer indexer : this.secondaryIndexers) {
            try {
                indexer.dropAndDestroy();
            } catch(Exception e) {
                logger.error("Failed to drop and destroy indexer", e);
            }
        }
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public BatchWriterConfig getBatchWriterConfig(){
        return batchWriterConfig;
    }

    public void setBatchWriterConfig(BatchWriterConfig batchWriterConfig) {
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
	public void setConf(AccumuloRdfConfiguration conf) {
        this.conf = conf;
    }

    public RyaTableMutationsFactory getRyaTableMutationsFactory() {
        return ryaTableMutationsFactory;
    }

    public void setRyaTableMutationsFactory(RyaTableMutationsFactory ryaTableMutationsFactory) {
        this.ryaTableMutationsFactory = ryaTableMutationsFactory;
    }

    @Override
	public AccumuloRyaQueryEngine getQueryEngine() {
        return queryEngine;
    }

    public void setQueryEngine(AccumuloRyaQueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    public void flush() throws RyaDAOException {
        try {
            mt_bw.flush();
        } catch (MutationsRejectedException e) {
            throw new RyaDAOException(e);
        }
    }

    protected String[] getTables() {
        // core tables
        List<String> tableNames = Lists.newArrayList(
                tableLayoutStrategy.getSpo(),
                tableLayoutStrategy.getPo(),
                tableLayoutStrategy.getOsp(),
                tableLayoutStrategy.getNs(),
                tableLayoutStrategy.getEval());

        // Additional Tables
        for (AccumuloIndexer index : secondaryIndexers) {
            tableNames.add(index.getTableName());
        }

        return tableNames.toArray(new String[]{});
    }

    private void purge(String tableName, String[] auths) throws TableNotFoundException, MutationsRejectedException {
        if (tableExists(tableName)) {
            logger.info("Purging accumulo table: " + tableName);
            BatchDeleter batchDeleter = createBatchDeleter(tableName, new Authorizations(auths));
            try {
                batchDeleter.setRanges(Collections.singleton(new Range()));
                batchDeleter.delete();
            } finally {
                batchDeleter.close();
            }
        }
    }

    private void compact(String tableName) {
        logger.info("Requesting major compaction for table " + tableName);
        try {
            connector.tableOperations().compact(tableName, null, null, true, false);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private boolean tableExists(String tableName) {
        return getConnector().tableOperations().exists(tableName);
    }

    private BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations) throws TableNotFoundException {
        return connector.createBatchDeleter(tableName, authorizations, NUM_THREADS, MAX_MEMORY, MAX_TIME, NUM_THREADS);
    }

    private void checkVersion() throws RyaDAOException, IOException, MutationsRejectedException {
        String version = getVersion();
        if (version == null) {
            //adding to core Rya tables but not Indexes
            Map<TABLE_LAYOUT, Collection<Mutation>> mutationMap = ryaTableMutationsFactory.serialize(getVersionRyaStatement());
            Collection<Mutation> spo = mutationMap.get(TABLE_LAYOUT.SPO);
            Collection<Mutation> po = mutationMap.get(TABLE_LAYOUT.PO);
            Collection<Mutation> osp = mutationMap.get(TABLE_LAYOUT.OSP);
            bw_spo.addMutations(spo);
            bw_po.addMutations(po);
            bw_osp.addMutations(osp);
        }
        //TODO: Do a version check here
    }

    protected RyaStatement getVersionRyaStatement() {
        return new RyaStatement(RTS_SUBJECT_RYA, RTS_VERSION_PREDICATE_RYA, VERSION_RYA);
    }

    private void drop(String tableName) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
        logger.info("Dropping cloudbase table: " + tableName);
        connector.tableOperations().delete(tableName);
    }
}
