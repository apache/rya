package mvm.mmrts.rdf.partition.mr.fileinput.bulk;

import junit.framework.TestCase;
import org.apache.hadoop.io.Text;

/**
 * Class EmbedKeyRangePartitionerTest
 * Date: Sep 13, 2011
 * Time: 1:58:28 PM
 */
public class EmbedKeyRangePartitionerTest extends TestCase {

    public void testRetrieveEmbedKey() throws Exception {
        assertEquals(new Text("hello"), EmbedKeyRangePartitioner.retrieveEmbedKey(new Text("hello\1there")));
        assertEquals(new Text("h"), EmbedKeyRangePartitioner.retrieveEmbedKey(new Text("h\1there")));
        assertEquals(new Text(""), EmbedKeyRangePartitioner.retrieveEmbedKey(new Text("\1there")));
        assertEquals(new Text("hello there"), EmbedKeyRangePartitioner.retrieveEmbedKey(new Text("hello there")));
    }

}
