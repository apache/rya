package mvm.rya.cloudbase.utils.shard;

import cloudbase.core.client.MutationsRejectedException;
import cloudbase.core.data.Mutation;
import cloudbase.core.util.ArgumentChecker;

/**
 * TODO: Think about what execption to use here
 * Created by IntelliJ IDEA.
 * Date: 4/18/12
 * Time: 10:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class ShardedBatchWriter {
    private ShardedConnector shardedConnector;

    public ShardedBatchWriter(ShardedConnector shardedConnector) {
        ArgumentChecker.notNull(shardedConnector);
        this.shardedConnector = shardedConnector;
    }

    //addMutation
    public void addMutation(Mutation mutation, String key) throws MutationsRejectedException {
        shardedConnector.addMutation(mutation, key);
    }

    public void addMutations(Iterable<Mutation> mutations, String key) throws MutationsRejectedException {
        shardedConnector.addMutations(mutations, key);
    }
    //flush
    public void flush() throws MutationsRejectedException {
        shardedConnector.commitWriters();
    }
    public void flush(String key) throws MutationsRejectedException {
        shardedConnector.retrieveBatchWriter(key).flush();
    }
    //close
    public void close() throws MutationsRejectedException {
        //commit?
        flush();
        //maybe do nothing here because the writers are in the connector
    }

    public ShardedConnector getShardedConnector() {
        return shardedConnector;
    }

    public void setShardedConnector(ShardedConnector shardedConnector) {
        this.shardedConnector = shardedConnector;
    }
}
