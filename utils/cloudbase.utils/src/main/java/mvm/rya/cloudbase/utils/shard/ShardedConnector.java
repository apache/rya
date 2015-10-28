package mvm.rya.cloudbase.utils.shard;

import cloudbase.core.client.*;
import cloudbase.core.client.admin.TableOperations;
import cloudbase.core.data.Mutation;
import cloudbase.core.security.Authorizations;
import cloudbase.core.util.ArgumentChecker;
import mvm.rya.cloudbase.utils.scanner.BatchScannerList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/18/12
 * Time: 10:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class ShardedConnector {
    //TODO: Use Ketema and perform proper Consistent Hashing
    private Connector connector;
    private int numShards;
    private String tablePrefix;
    private HashAlgorithm hashAlgorithm;

    private Map<String, BatchWriter> writers = new HashMap<String, BatchWriter>();
    private boolean initialized = false;

    public ShardedConnector(Connector connector, int numShards, String tablePrefix, HashAlgorithm hashAlgorithm) {
        ArgumentChecker.notNull(connector);
        ArgumentChecker.notNull(tablePrefix);
        if (numShards <= 0) {
            throw new IllegalArgumentException("Number of shards cannot be 0");
        }
        if (hashAlgorithm == null) {
            this.hashAlgorithm = new HashCodeHashAlgorithm();
        }
        this.connector = connector;
        this.numShards = numShards;
        this.tablePrefix = tablePrefix;
    }

    //createShardedBatchScanner
    public BatchScanner createBatchScanner(String key, Authorizations authorizations, int numQueryThreads) throws TableNotFoundException {
        List<BatchScanner> scanners = new ArrayList<BatchScanner>();
        if (key != null) {
            String shardTableName = buildShardTablename(key);
            scanners.add(connector.createBatchScanner(shardTableName, authorizations, numQueryThreads));
        } else {
            //TODO: Use Ketema to do proper Consistent Hashing
            for (int i = 0; i < numShards; i++) {
                String shardTablename = buildShardTablename(i);
                scanners.add(connector.createBatchScanner(shardTablename, authorizations, numQueryThreads)); //TODO: Will make scanner.size * numThreads = number of threads.
            }
        }
        return new BatchScannerList(scanners);
    }
    //createShardedScanner

    public ShardedBatchWriter createBatchWriter() throws TableNotFoundException {
        return new ShardedBatchWriter(this);
    }


    protected void addMutation(Mutation mutation, String key) throws MutationsRejectedException {
        retrieveBatchWriter(key).addMutation(mutation);
    }

    protected void addMutations(Iterable<Mutation> mutations, String key) throws MutationsRejectedException {
        retrieveBatchWriter(key).addMutations(mutations);
    }

    public void init() throws Exception {
        if (isInitialized()) {
            throw new UnsupportedOperationException("ShardedConnector already initialized");
        }
        //init tables
        TableOperations tableOperations = connector.tableOperations();
        //create writers
        //TODO: Use Ketema to do proper Consistent Hashing
        for (int i = 0; i < numShards; i++) {
            String shardTablename = buildShardTablename(i);
            if (!tableOperations.exists(shardTablename)) {
                tableOperations.create(shardTablename);
            }
            writers.put(shardTablename, connector.createBatchWriter(shardTablename, 1000000l, 60000l, 2)); //TODO: configurable
        }

        initialized = true;
    }

    public void close() throws Exception {
        //close writers
        for (Map.Entry<String, BatchWriter> entry : writers.entrySet()) {
            entry.getValue().close();
        }
        initialized = false;
    }

    protected BatchWriter retrieveBatchWriter(String key) {
        String tableName = buildShardTablename(key);
        return writers.get(tableName);
    }

    protected void commitWriters() throws MutationsRejectedException {
        for (Map.Entry<String, BatchWriter> entry : writers.entrySet()) {
            entry.getValue().flush();
        }
    }

    protected String buildShardTablename(String key) {
        long shard = hashAlgorithm.hash(key) % numShards;
        return buildShardTablename(shard);
    }

    protected String buildShardTablename(long shardId) {
        return tablePrefix + shardId;
    }


    public Connector getConnector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public int getNumShards() {
        return numShards;
    }

    public void setNumShards(int numShards) {
        this.numShards = numShards;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public HashAlgorithm getHashAlgorithm() {
        return hashAlgorithm;
    }

    public void setHashAlgorithm(HashAlgorithm hashAlgorithm) {
        this.hashAlgorithm = hashAlgorithm;
    }

    public boolean isInitialized() {
        return initialized;
    }
}
