package mvm.mmrts.rdf.partition.mr.fileinput.bulk;

import cloudbase.core.client.mapreduce.lib.partition.RangePartitioner;
import mvm.mmrts.rdf.partition.PartitionConstants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Class EmbedKeyRangePartitioner
 * Date: Sep 13, 2011
 * Time: 1:49:35 PM
 */
public class EmbedKeyRangePartitioner extends RangePartitioner {
    @Override
    public int getPartition(Text key, Writable value, int numPartitions) {
        Text embedKey = retrieveEmbedKey(key);
        return super.getPartition(embedKey, value, numPartitions);
    }

    public static Text retrieveEmbedKey(Text key) {
        int split = key.find(PartitionConstants.INDEX_DELIM_STR);
        if (split < 0)
            return key;
        Text newText = new Text();
        newText.append(key.getBytes(), 0, split);
        return newText;
    }
}
