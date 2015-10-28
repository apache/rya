package mvm.rya.cloudbase;

import cloudbase.core.client.*;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.admin.TableOperations;
import cloudbase.core.client.impl.TabletServerBatchDeleter;
import cloudbase.core.conf.Property;
import cloudbase.core.data.Key;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Range;
import cloudbase.core.security.Authorizations;
import cloudbase.core.security.ColumnVisibility;
import com.google.common.collect.Iterators;
import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.layout.TableLayoutStrategy;
import mvm.rya.api.persist.RyaDAO;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.RyaNamespaceManager;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;
import mvm.rya.cloudbase.query.CloudbaseRyaQueryEngine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.openrdf.model.Namespace;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;
import static mvm.rya.api.RdfCloudTripleStoreConstants.*;
import static mvm.rya.cloudbase.CloudbaseRdfConstants.ALL_AUTHORIZATIONS;
import static mvm.rya.cloudbase.CloudbaseRdfConstants.EMPTY_CV;

/**
 * Class CloudbaseRyaDAO
 * Date: Feb 29, 2012
 * Time: 12:37:22 PM
 */
public class CloudbaseRyaDAO implements RyaDAO<CloudbaseRdfConfiguration>, RyaNamespaceManager<CloudbaseRdfConfiguration> {
    private static final Log logger = LogFactory.getLog(CloudbaseRyaDAO.class);

    private boolean initialized = false;
    private Connector connector;

    private BatchWriter bw_spo;
    private BatchWriter bw_po;
    private BatchWriter bw_osp;
    private BatchWriter bw_ns;

    private CloudbaseRdfConfiguration conf = new CloudbaseRdfConfiguration();
    private ColumnVisibility cv = EMPTY_CV;
    private RyaTableMutationsFactory ryaTableMutationsFactory = new RyaTableMutationsFactory();
    private TableLayoutStrategy tableLayoutStrategy;
    private CloudbaseRyaQueryEngine queryEngine;
    private RyaContext ryaContext = RyaContext.getInstance();

    @Override
    public boolean isInitialized() throws RyaDAOException {
        return initialized;
    }

    @Override
    public void init() throws RyaDAOException {
        if (initialized)
            return;
        try {
            checkNotNull(conf);
            checkNotNull(connector);

            tableLayoutStrategy = conf.getTableLayoutStrategy();
            String cv_s = conf.getCv();
            if (cv_s != null) {
                cv = new ColumnVisibility(cv_s);
            }

            TableOperations tableOperations = connector.tableOperations();
            CloudbaseRdfUtils.createTableIfNotExist(tableOperations, tableLayoutStrategy.getSpo());
            CloudbaseRdfUtils.createTableIfNotExist(tableOperations, tableLayoutStrategy.getPo());
            CloudbaseRdfUtils.createTableIfNotExist(tableOperations, tableLayoutStrategy.getOsp());
            CloudbaseRdfUtils.createTableIfNotExist(tableOperations, tableLayoutStrategy.getNs());

            //get the batch writers for tables
            bw_spo = connector.createBatchWriter(tableLayoutStrategy.getSpo(), MAX_MEMORY, MAX_TIME,
                    NUM_THREADS);
            bw_po = connector.createBatchWriter(tableLayoutStrategy.getPo(), MAX_MEMORY, MAX_TIME,
                    NUM_THREADS);
            bw_osp = connector.createBatchWriter(tableLayoutStrategy.getOsp(), MAX_MEMORY, MAX_TIME,
                    NUM_THREADS);

            bw_ns = connector.createBatchWriter(tableLayoutStrategy.getNs(), MAX_MEMORY,
                    MAX_TIME, 1);

            queryEngine = new CloudbaseRyaQueryEngine(connector, getConf());

            checkVersion();

            initialized = true;
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
    }

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
    public void delete(RyaStatement stmt, CloudbaseRdfConfiguration aconf) throws RyaDAOException {
        this.delete(Iterators.singletonIterator(stmt), aconf);
    }

    @Override
    public void delete(Iterator<RyaStatement> statements, CloudbaseRdfConfiguration conf) throws RyaDAOException {
        try {
            while (statements.hasNext()) {
                RyaStatement stmt = statements.next();
                //query first
                CloseableIteration<RyaStatement, RyaDAOException> query = this.queryEngine.query(stmt, conf);
                while (query.hasNext()) {
                    deleteSingleRyaStatement(query.next());
                }
            }
            bw_spo.flush();
            bw_po.flush();
            bw_osp.flush();
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
    }

    protected void deleteSingleRyaStatement(RyaStatement stmt) throws TripleRowResolverException, MutationsRejectedException {
        Map<TABLE_LAYOUT, TripleRow> map = ryaContext.serializeTriple(stmt);
        bw_spo.addMutation(deleteMutation(map.get(TABLE_LAYOUT.SPO)));
        bw_po.addMutation(deleteMutation(map.get(TABLE_LAYOUT.PO)));
        bw_osp.addMutation(deleteMutation(map.get(TABLE_LAYOUT.OSP)));
    }

    protected Mutation deleteMutation(TripleRow tripleRow) {
        Mutation m = new Mutation(new Text(tripleRow.getRow()));

        byte[] columnFamily = tripleRow.getColumnFamily();
        Text cfText = columnFamily == null ? EMPTY_TEXT : new Text(columnFamily);

        byte[] columnQualifier = tripleRow.getColumnQualifier();
        Text cqText = columnQualifier == null ? EMPTY_TEXT : new Text(columnQualifier);

        m.putDelete(cfText, cqText, new ColumnVisibility(tripleRow.getColumnVisibility()), tripleRow.getTimestamp());
        return m;
    }

    protected void commit(Iterator<RyaStatement> commitStatements) throws RyaDAOException {
        try {
            //TODO: Should have a lock here in case we are adding and committing at the same time
            while (commitStatements.hasNext()) {

                Map<TABLE_LAYOUT, Collection<Mutation>> mutationMap = ryaTableMutationsFactory.serialize(commitStatements.next());
                Collection<Mutation> spo = mutationMap.get(TABLE_LAYOUT.SPO);
                Collection<Mutation> po = mutationMap.get(TABLE_LAYOUT.PO);
                Collection<Mutation> osp = mutationMap.get(TABLE_LAYOUT.OSP);
                bw_spo.addMutations(spo);
                bw_po.addMutations(po);
                bw_osp.addMutations(osp);
            }

            bw_spo.flush();
            bw_po.flush();
            bw_osp.flush();
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
            bw_osp.flush();
            bw_spo.flush();
            bw_po.flush();
            bw_ns.flush();

            bw_osp.close();
            bw_spo.close();
            bw_po.close();
            bw_ns.close();
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public void addNamespace(String pfx, String namespace) throws RyaDAOException {
        try {
            Mutation m = new Mutation(new Text(pfx));
            m.put(INFO_NAMESPACE_TXT, EMPTY_TEXT, new cloudbase.core.data.Value(
                    namespace.getBytes()));
            bw_ns.addMutation(m);
            bw_ns.flush();
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
            Iterator<Map.Entry<Key, cloudbase.core.data.Value>> iterator = scanner
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
            bw_ns.flush();
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public CloseableIteration<Namespace, RyaDAOException> iterateNamespace() throws RyaDAOException {
        try {
            Scanner scanner = connector.createScanner(tableLayoutStrategy.getNs(),
                    ALL_AUTHORIZATIONS);
            scanner.fetchColumnFamily(INFO_NAMESPACE_TXT);
            Iterator<Map.Entry<Key, cloudbase.core.data.Value>> result = scanner.iterator();
            return new CloudbaseNamespaceTableIterator(result);
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
    }

    @Override
    public RyaNamespaceManager<CloudbaseRdfConfiguration> getNamespaceManager() {
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
        try {
            if (isInitialized()) {
                checkVersion();
            }
        } catch (RyaDAOException e) {
            logger.error("checkVersion() failed?", e);
        }
    }

    @Override
    public void dropAndDestroy() throws RyaDAOException {
        for (String tableName : getTables()) {
            try {
                drop(tableName);
            } catch (CBSecurityException e) {
                logger.error(e.getMessage());
                throw new RyaDAOException(e);
            } catch (CBException e) {
                logger.error(e.getMessage());
                throw new RyaDAOException(e);
            } catch (TableNotFoundException e) {
                logger.warn(e.getMessage());
            }
        }
        destroy();
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public CloudbaseRdfConfiguration getConf() {
        return conf;
    }

    public void setConf(CloudbaseRdfConfiguration conf) {
        this.conf = conf;
    }

    public RyaTableMutationsFactory getRyaTableMutationsFactory() {
        return ryaTableMutationsFactory;
    }

    public void setRyaTableMutationsFactory(RyaTableMutationsFactory ryaTableMutationsFactory) {
        this.ryaTableMutationsFactory = ryaTableMutationsFactory;
    }

    public CloudbaseRyaQueryEngine getQueryEngine() {
        return queryEngine;
    }

    public void setQueryEngine(CloudbaseRyaQueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    protected String[] getTables() {
        return new String[] {
                tableLayoutStrategy.getSpo()
                , tableLayoutStrategy.getPo()
                , tableLayoutStrategy.getOsp()
                , tableLayoutStrategy.getNs()
                , tableLayoutStrategy.getEval()
        };
    }

    private void purge(String tableName, String[] auths) throws TableNotFoundException, MutationsRejectedException {
        if (tableExists(tableName)) {
            logger.info("Purging cloudbase table: " + tableName);
            BatchDeleter batchDeleter = createBatchDeleter(tableName, new Authorizations(auths));
            try {
                batchDeleter.setRanges(Collections.singleton(new Range()));
                batchDeleter.delete();
            } finally {
                ((TabletServerBatchDeleter)batchDeleter).close();
            }
        }
    }

    private void compact(String tableName) {
        Date now = new Date(System.currentTimeMillis());
        SimpleDateFormat dateParser = new SimpleDateFormat("yyyyMMddHHmmssz", Locale.getDefault());
        String nowStr = dateParser.format(now);
        try {
            for (Map.Entry<String, String> prop : connector.tableOperations().getProperties(tableName)) {
                if (prop.getKey().equals(Property.TABLE_MAJC_COMPACTALL_AT.getKey())) {
                    if (dateParser.parse(prop.getValue()).after(now)) {
                        return;
                    } else {
                        break;
                    }
                }
            }

            connector.tableOperations().flush(tableName);
            logger.info("Requesting major compaction for table " + tableName);
            connector.tableOperations().setProperty(tableName, Property.TABLE_MAJC_COMPACTALL_AT.getKey(), nowStr);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    private Authorizations getAuthorizations(String auth) {
        if (auth == null) {
            return new Authorizations();
        } else {
            String[] auths = auth.split(",");
            return new Authorizations(auths);
        }
    }

    private boolean tableExists(String tableName) {
        return getConnector().tableOperations().exists(tableName);
    }

    private BatchDeleter createBatchDeleter(String tableName, Authorizations authorizations) throws TableNotFoundException {
        return connector.createBatchDeleter(tableName, authorizations, NUM_THREADS, MAX_MEMORY, MAX_TIME, NUM_THREADS);
    }

    private void checkVersion() throws RyaDAOException {
        String version = getVersion();
        if (version == null) {
            this.add(getVersionRyaStatement());
        }
        //TODO: Do a version check here
    }

    protected RyaStatement getVersionRyaStatement() {
        return new RyaStatement(RTS_SUBJECT_RYA, RTS_VERSION_PREDICATE_RYA, VERSION_RYA);
    }

    private void drop(String tableName) throws CBSecurityException, CBException, TableNotFoundException {
        logger.info("Dropping cloudbase table: " + tableName);
        connector.tableOperations().delete(tableName);
    }
}
