package mvm.rya.cloudbase.utils.filters;

import cloudbase.core.data.Key;
import junit.framework.TestCase;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;

/**
 * Class TimeRangeFilterTest
 * Date: Mar 23, 2011
 * Time: 10:08:58 AM
 */
public class TimeRangeFilterTest extends TestCase {

    public void testTimeRange() throws Exception {
        TimeRangeFilter filter = new TimeRangeFilter();
        Map<String, String> map = new HashMap<String, String>();
        map.put(TimeRangeFilter.TIME_RANGE_PROP, "10000");
        map.put(TimeRangeFilter.START_TIME_PROP, "1010001");
        filter.init(map);

        assertFalse(filter.accept(new Key(new Text("row1"), 1000000), null));
        assertTrue(filter.accept(new Key(new Text("row1"), 1000001), null));
        assertTrue(filter.accept(new Key(new Text("row1"), 1000011), null));
        assertTrue(filter.accept(new Key(new Text("row1"), 1010001), null));
        assertFalse(filter.accept(new Key(new Text("row1"), 1010002), null));
        assertFalse(filter.accept(new Key(new Text("row1"), 1010012), null));
    }

    public void testTimeRangeSetOptions() throws Exception {
        try {
            TimeRangeFilter filter = new TimeRangeFilter();
            Map<String, String> map = new HashMap<String, String>();
            filter.init(map);
            fail();
        } catch (Exception e) {
        }
    }

    public void testTimeRangeCurrentTime() throws Exception {
        long currentTime = System.currentTimeMillis();
        TimeRangeFilter filter = new TimeRangeFilter();
        Map<String, String> map = new HashMap<String, String>();
        map.put(TimeRangeFilter.TIME_RANGE_PROP, "10000");
        filter.init(map);

        assertFalse(filter.accept(new Key(new Text("row1"), currentTime - 15000), null));
        assertTrue(filter.accept(new Key(new Text("row1"), currentTime - 5000), null));
        assertFalse(filter.accept(new Key(new Text("row1"), currentTime + 5000), null));
    }
}
