package mvm.rya.generic.mr.api

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.mapreduce.Job

/**
 * Date: 12/3/12
 * Time: 8:56 AM
 */
public interface MRInfo extends Configurable{

    public static final String USERNAME = "username"
    public static final String PASSWORD = "password"
    public static final String INSTANCE = "instance"
    public static final String ZOOKEEPERS = "zookeepers"
    public static final String MOCK = "mock"

    def void initMRJob(Job job, String table, String outtable, String[] auths)

    def key(byte[] data);

    def key(String row, String cf, String cq, String cv, long timestamp);

    def value(byte[] data);

    def columnVisibility(String cv);

    def mutation(String row, String cf, String cq, String cv, long timestamp, byte[] val);

    def instance()

    def connector(def instance)

    def void writeMutations(def connector, String tableName, Iterator mutations)

    def scanner(def connector, String tableName, String[] auths)

    def batchScanner(def connector, String tableName, String[] auths, int numThreads)

    def range(def startKey, def endKey)

    def authorizations(String[] auths)
}