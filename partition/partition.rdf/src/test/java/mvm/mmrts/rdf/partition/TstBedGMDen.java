package mvm.mmrts.rdf.partition;

import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.Connector;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import com.google.common.io.ByteStreams;
import mvm.mmrts.rdf.partition.utils.RdfIO;
import org.apache.hadoop.io.Text;
import org.openrdf.query.algebra.Var;
import ss.cloudbase.core.iterators.CellLevelRecordIterator;
import ss.cloudbase.core.iterators.GMDenIntersectingIterator;
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
public class TstBedGMDen {
    public static void main(String[] args) {
        try {

            Connector connector = new ZooKeeperInstance("stratus", "stratus13:2181").getConnector("root", "password");
            BatchScanner bs = connector.createBatchScanner("rdfPartition", ALL_AUTHORIZATIONS, 3);

            String[] predicates = {"urn:lubm:test#takesCourse",
                                   "urn:lubm:test#name",
                                   "urn:lubm:test#specific",
                                   "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"};

            Text[] queries = new Text[predicates.length];
            for (int i = 0; i < predicates.length; i++) {
                String predicate = predicates[i];
                queries[i] = new Text(GMDenIntersectingIterator.getRangeTerm(INDEX.toString(),
                        URI_MARKER_STR + predicate + INDEX_DELIM_STR + "\0"
                        , true,
                        URI_MARKER_STR + predicate + INDEX_DELIM_STR + "\uFFFD",
                        true
                ));
                System.out.println(queries[i]);
            }

            bs.setScanIterators(21, CellLevelRecordIterator.class.getName(), "ci");

            bs.setScanIterators(20, GMDenIntersectingIterator.class.getName(), "ii");
            bs.setScanIteratorOption("ii", GMDenIntersectingIterator.docFamilyOptionName, DOC.toString());
            bs.setScanIteratorOption("ii", GMDenIntersectingIterator.indexFamilyOptionName, INDEX.toString());
            bs.setScanIteratorOption("ii", GMDenIntersectingIterator.columnFamiliesOptionName, GMDenIntersectingIterator.encodeColumns(queries));
            bs.setScanIteratorOption("ii", GMDenIntersectingIterator.OPTION_MULTI_DOC, "" + true);

            Range range = new Range(
                    new Key("2011-11\0"),
                    new Key("2011-11\uFFFD")
            );

            bs.setRanges(Collections.singleton(range));

            int count = 0;
            Iterator<Map.Entry<Key, Value>> iter = bs.iterator();
            CBConverter converter = new CBConverter();
            while (iter.hasNext()) {
                count++;
                Map.Entry<Key, Value> entry = iter.next();
                Value value = entry.getValue();
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
