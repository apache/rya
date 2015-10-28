package mvm.rya.iterators;

import cloudbase.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Date: Mar 23, 2011
 * Time: 10:08:58 AM
 */
public class LimitingAgeOffFilterTest {

    @Test
    public void testTimeRange() throws Exception {
        LimitingAgeOffFilter filter = new LimitingAgeOffFilter();
        Map<String, String> map = new HashMap<String, String>();
        map.put(LimitingAgeOffFilter.TTL, "10000");
        map.put(LimitingAgeOffFilter.CURRENT_TIME, "1010001");
        filter.init(map);

        assertFalse(filter.accept(new Key(new Text("row1"), 1000000), null));
        assertTrue(filter.accept(new Key(new Text("row1"), 1000001), null));
        assertTrue(filter.accept(new Key(new Text("row1"), 1000011), null));
        assertTrue(filter.accept(new Key(new Text("row1"), 1010001), null));
        assertFalse(filter.accept(new Key(new Text("row1"), 1010002), null));
        assertFalse(filter.accept(new Key(new Text("row1"), 1010012), null));
    }

    @Test
    public void testTimeRangeSetOptions() throws Exception {
        try {
            LimitingAgeOffFilter filter = new LimitingAgeOffFilter();
            Map<String, String> map = new HashMap<String, String>();
            filter.init(map);
            fail();
        } catch (Exception e) {
        }
    }

    @Test
    public void testTimeRangeCurrentTime() throws Exception {
        long currentTime = System.currentTimeMillis();
        LimitingAgeOffFilter filter = new LimitingAgeOffFilter();
        Map<String, String> map = new HashMap<String, String>();
        map.put(LimitingAgeOffFilter.TTL, "10000");
        filter.init(map);

        assertFalse(filter.accept(new Key(new Text("row1"), currentTime - 15000), null));
        assertTrue(filter.accept(new Key(new Text("row1"), currentTime - 5000), null));
        assertFalse(filter.accept(new Key(new Text("row1"), currentTime + 5000), null));
    }
}
