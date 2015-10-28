package mvm.rya.generic.mr.accumulo

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import mvm.rya.generic.mr.api.MRInfo
import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.ZooKeeperInstance

/**
 * Date: 12/3/12
 * Time: 9:00 AM
 */
class AccumuloMRInfo implements MRInfo {

    def Configuration conf
    def connector;

    @Override
    void initMRJob(Job job, String table, String outtable, String[] auths) {
        Configuration conf = job.configuration
        String username = conf.get(USERNAME)
        String password = conf.get(PASSWORD)
        String instance = conf.get(INSTANCE)
        String zookeepers = conf.get(ZOOKEEPERS)
        String mock = conf.get(MOCK)

        //input
        if (Boolean.parseBoolean(mock)) {
            AccumuloInputFormat.setMockInstance(conf, instance)
            AccumuloOutputFormat.setMockInstance(conf, instance)
        } else if (zookeepers != null) {
            AccumuloInputFormat.setZooKeeperInstance(conf, instance, zookeepers)
            AccumuloOutputFormat.setZooKeeperInstance(conf, instance, zookeepers)
        } else {
            throw new IllegalArgumentException("Must specify either mock or zookeepers");
        }

        AccumuloInputFormat.setInputInfo(conf, username, password.getBytes(), table, new Authorizations(auths))
        job.setInputFormatClass(AccumuloInputFormat.class);

        // OUTPUT
        job.setOutputFormatClass(AccumuloOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Mutation.class);
        AccumuloOutputFormat.setOutputInfo(job, username, password.getBytes(), true, outtable);
    }

    @Override
    def key(byte[] data) {
        Key key = new Key();
        key.readFields(new DataInputStream(new ByteArrayInputStream(data)))
        return key
    }

    @Override
    def key(String row, String cf, String cq, String cv, long timestamp) {
        return new Key(row, cf, cq, cv, timestamp)
    }

    @Override
    def value(byte[] data) {
        return new Value(data)
    }

    @Override
    def columnVisibility(String cv) {
        return new ColumnVisibility(cv)
    }

    @Override
    def mutation(String row, String cf, String cq, String cv, long timestamp, byte[] val) {
        Mutation mutation = new Mutation(row);
        mutation.put(cf, cq, columnVisibility(cv), timestamp, value(val))
        return mutation
    }

    @Override
    def instance() {
        assert conf != null

        String instance_str = conf.get(INSTANCE)
        String zookeepers = conf.get(ZOOKEEPERS)
        String mock = conf.get(MOCK)
        if (Boolean.parseBoolean(mock)) {
            return new MockInstance(instance_str)
        } else if (zookeepers != null) {
            return new ZooKeeperInstance(instance_str, zookeepers)
        } else {
            throw new IllegalArgumentException("Must specify either mock or zookeepers");
        }
    }

    @Override
    def connector(def instance) {
        if (connector != null) return connector

        String username = conf.get(USERNAME)
        String password = conf.get(PASSWORD)
        if (instance == null)
            instance = instance()
        connector = instance.getConnector(username, password)
        return connector
    }

    @Override
    def void writeMutations(def connector, String tableName, Iterator mutations) {
        def bw = connector.createBatchWriter(tableName, 10000l, 10000l, 4);
        mutations.each { m ->
            bw.addMutation(m)
        }
        bw.flush()
        bw.close()
    }

    @Override
    def scanner(def connector, String tableName, String[] auths) {
        return connector.createScanner(tableName, new Authorizations(auths))
    }

    @Override
    def batchScanner(def connector, String tableName, String[] auths, int numThreads) {
        return connector.createBatchScanner(tableName, new Authorizations(auths), numThreads)
    }

    @Override
    def range(def startKey, def endKey) {
        assert startKey != null

        if (endKey != null)
            return new org.apache.accumulo.core.data.Range(startKey, endKey)
        return new org.apache.accumulo.core.data.Range(startKey)
    }

    @Override
    def authorizations(String[] auths) {
        return new Authorizations(auths)
    }
}
