package mvm.mmrts.rdf.partition;

import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.Connector;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import org.apache.hadoop.io.Text;
import ss.cloudbase.core.iterators.SortedRangeIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;

/**
 * Class TstBed
 * Date: Aug 2, 2011
 * Time: 9:22:11 AM
 */
public class TstScanner {
    public static void main(String[] args) {
        try {
            Connector connector = new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password");
            BatchScanner bs = connector.createBatchScanner("partitionRdf", ALL_AUTHORIZATIONS, 3);

            Text shard = new Text("2011-08-40");
            String uri = "urn:tde:c5e2f4d8-a5a6-48d8-ba55-1acea969c38d";
            bs.setRanges(Collections.singleton(
                    new Range(
                            new Key(
                                    shard, DOC,
                                    new Text(URI_MARKER_STR + uri + FAMILY_DELIM_STR + "\0")
                            ),
                            new Key(
                                    shard, DOC,
                                    new Text(URI_MARKER_STR + uri + FAMILY_DELIM_STR + "\uFFFD")
                            )
                    )
            ));

            int count = 0;
            Iterator<Map.Entry<Key, Value>> iter = bs.iterator();
            while (iter.hasNext()) {
                count++;
//                iter.next();
                System.out.println(iter.next().getKey().getColumnQualifier());
            }
            System.out.println(count);

            bs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
