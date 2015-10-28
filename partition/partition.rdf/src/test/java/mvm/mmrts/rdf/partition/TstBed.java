package mvm.mmrts.rdf.partition;

import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.Connector;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import com.google.common.io.ByteStreams;
import mvm.mmrts.rdf.partition.utils.RdfIO;
import ss.cloudbase.core.iterators.CellLevelRecordIterator;
import ss.cloudbase.core.iterators.SortedRangeIterator;
import ss.cloudbase.core.iterators.filter.CBConverter;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static mvm.mmrts.rdf.partition.PartitionConstants.*;

/**
 * Class TstBed
 * Date: Aug 2, 2011
 * Time: 9:22:11 AM
 */
public class TstBed {
    public static void main(String[] args) {
        try {

            String predicate = "http://here/2010/tracked-data-provenance/ns#createdItem";

            Connector connector = new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password");
            BatchScanner bs = connector.createBatchScanner("partitionRdf", ALL_AUTHORIZATIONS, 3);

            bs.setScanIterators(21, CellLevelRecordIterator.class.getName(), "ci");

            bs.setScanIterators(20, SortedRangeIterator.class.getName(), "ri");
            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_DOC_COLF, DOC.toString());
            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_COLF, INDEX.toString());
            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_START_INCLUSIVE, "" + true);
            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_END_INCLUSIVE, "" + true);
            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_MULTI_DOC, "" + true);

            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_LOWER_BOUND,
                    "\07http://here/2010/tracked-data-provenance/ns#reportedAt\u0001\u000B2011-08-26T18:01:51.000Z"
            );
            bs.setScanIteratorOption("ri", SortedRangeIterator.OPTION_UPPER_BOUND,
                    "\07http://here/2010/tracked-data-provenance/ns#reportedAt\u0001\u000B2011-08-26T18:01:51.400Z"
            );

            Range range = new Range(
                    new Key("2011-08\0"),
                    new Key("2011-08\uFFFD")
            );

//            scanner.setRange(range);
            bs.setRanges(Collections.singleton(range));
//            bs.fetchColumnFamily(INDEX);
//            bs.setColumnFamilyRegex(INDEX.toString());
//            bs.setColumnQualifierRegex(URI_MARKER_STR + predicate + INDEX_DELIM_STR + "(.*)");

            int count = 0;
            Iterator<Map.Entry<Key, Value>> iter = bs.iterator();
            CBConverter converter = new CBConverter();
            while (iter.hasNext()) {
                count++;
//                iter.next();
                Map.Entry<Key, Value> entry = iter.next();
                Value value = entry.getValue();
//                System.out.println(entry.getKey().getColumnQualifier() + "----" + value);
                org.openrdf.model.Value subj = RdfIO.readValue(ByteStreams.newDataInput(entry.getKey().getColumnQualifier().getBytes()), VALUE_FACTORY, FAMILY_DELIM);
                Map<String, String> map = converter.toMap(entry.getKey(), value);
                for (Map.Entry<String, String> e : map.entrySet()) {
                    String predObj = e.getKey();
                    String[] split = predObj.split("\0");
                    byte[] look = split[0].getBytes();
                    System.out.println(subj
                            + " : " + VALUE_FACTORY.createURI(split[0]) + " : " +
                            RdfIO.readValue(ByteStreams.newDataInput(split[1].getBytes()), VALUE_FACTORY, FAMILY_DELIM));
                }
            }
            System.out.println(count);

            bs.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
