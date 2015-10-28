package mvm.rya.cloudbase.utils.bulk;

import cloudbase.core.client.mapreduce.lib.partition.RangePartitioner;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Class KeyRangePartitioner
 * Date: Sep 13, 2011
 * Time: 2:45:56 PM
 */
public class KeyRangePartitioner extends Partitioner<Key, Value> implements Configurable {

    private RangePartitioner rangePartitioner = new RangePartitioner();
    private Configuration conf;

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
        rangePartitioner.setConf(conf);
    }

    @Override
    public int getPartition(Key key, Value value, int numReducers) {
        return rangePartitioner.getPartition(key.getRow(), value, numReducers);
    }

    
}
