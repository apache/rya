package mvm.mmrts.rdf.partition.mr.compat;

import junit.framework.TestCase;

/**
 * Class ChangeShardDateFormatToolTest
 * Date: Dec 9, 2011
 * Time: 10:39:31 AM
 */
public class ChangeShardDateFormatToolTest extends TestCase {

    public void testShardDelim() throws Exception {
        String dateDelim = "-";
        String shard = "2011-11-01";
        int shardIndex = shard.lastIndexOf(dateDelim);
        if (shardIndex == -1)
            fail();
        String date = shard.substring(0, shardIndex);
        shard = shard.substring(shardIndex + 1, shard.length());
        assertEquals("2011-11", date);
        assertEquals("01", shard);

        dateDelim = "_";
        shard = "20111101_33";
        shardIndex = shard.lastIndexOf(dateDelim);
        if (shardIndex == -1)
            fail();
        date = shard.substring(0, shardIndex);
        shard = shard.substring(shardIndex + 1, shard.length());
        assertEquals("20111101", date);
        assertEquals("33", shard);
    }
}
