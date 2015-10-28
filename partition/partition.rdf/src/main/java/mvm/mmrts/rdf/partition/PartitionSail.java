package mvm.mmrts.rdf.partition;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Connector;
import cloudbase.core.client.ZooKeeperInstance;
import mvm.mmrts.rdf.partition.converter.ContextColVisConverter;
import mvm.mmrts.rdf.partition.shard.DateHashModShardValueGenerator;
import mvm.mmrts.rdf.partition.shard.ShardValueGenerator;
import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailBase;

/**
 * Class PartitionSail
 * Date: Jul 6, 2011
 * Time: 11:40:52 AM
 */
public class PartitionSail extends SailBase {

    protected Connector connector;

    protected String table;
    //MMRTS-148
    protected String shardTable;

    protected ShardValueGenerator generator = new DateHashModShardValueGenerator();

    protected Configuration conf = new Configuration();

    protected ContextColVisConverter contextColVisConverter;

    public PartitionSail(Connector connector, String table) {
        this(connector, table, table, null);
    }

    public PartitionSail(Connector connector, String table, String shardTable) {
        this(connector, table, shardTable, null);
    }

    public PartitionSail(String instance, String zk, String user, String password, String table)
            throws CBSecurityException, CBException {
        this(instance, zk, user, password, table, (ShardValueGenerator) null);
    }

    public PartitionSail(String instance, String zk, String user, String password, String table, ShardValueGenerator generator)
            throws CBSecurityException, CBException {
        this(new ZooKeeperInstance(instance, zk).getConnector(user, password.getBytes()), table, table, generator);
    }

    public PartitionSail(String instance, String zk, String user, String password, String table, String shardTable)
            throws CBSecurityException, CBException {
        this(instance, zk, user, password, table, shardTable, null);
    }

    public PartitionSail(String instance, String zk, String user, String password, String table, String shardTable, ShardValueGenerator generator)
            throws CBSecurityException, CBException {
        this(new ZooKeeperInstance(instance, zk).getConnector(user, password.getBytes()), table, shardTable, generator);
    }

    public PartitionSail(Connector connector, String table, ShardValueGenerator generator) {
        this(connector, table, table, generator);
    }

    public PartitionSail(Connector connector, String table, String shardTable, ShardValueGenerator generator) {
        this.connector = connector;
        this.table = table;
        this.shardTable = shardTable;
        if (generator != null)
            this.generator = generator;
    }

    @Override
    protected void shutDownInternal() throws SailException {
    }

    @Override
    protected SailConnection getConnectionInternal() throws SailException {
        return new PartitionConnection(this);
    }

    @Override
    public boolean isWritable() throws SailException {
        return true;
    }

    @Override
    public ValueFactory getValueFactory() {
        return ValueFactoryImpl.getInstance();
    }

    public Configuration getConf() {
        return conf;
    }

    public Connector getConnector() {
        return connector;
    }

    public ShardValueGenerator getGenerator() {
        return generator;
    }

    public String getTable() {
        return table;
    }

    public String getShardTable() {
        return shardTable;
    }

    public ContextColVisConverter getContextColVisConverter() {
        return contextColVisConverter;
    }

    public void setContextColVisConverter(ContextColVisConverter contextColVisConverter) {
        this.contextColVisConverter = contextColVisConverter;
    }
}
