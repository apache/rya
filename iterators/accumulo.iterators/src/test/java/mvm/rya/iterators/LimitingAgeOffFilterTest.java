package mvm.rya.iterators;

/*
 * #%L
 * mvm.rya.accumulo.iterators
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.*;

/**
 * Date: 1/11/13
 * Time: 10:18 AM
 */
public class LimitingAgeOffFilterTest {

    @Test
    public void testTimeRange() throws Exception {
        LimitingAgeOffFilter filter = new LimitingAgeOffFilter();
        Map<String, String> map = new HashMap<String, String>();
        map.put(LimitingAgeOffFilter.TTL, "10000");
        map.put(LimitingAgeOffFilter.CURRENT_TIME, "1010001");
        filter.init(new SortedMapIterator(new TreeMap<Key, Value>()), map, null);

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
            filter.init(new SortedMapIterator(new TreeMap<Key, Value>()), map, null);
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
        filter.init(new SortedMapIterator(new TreeMap<Key, Value>()), map, null);

        assertFalse(filter.accept(new Key(new Text("row1"), currentTime - 15000), null));
        assertTrue(filter.accept(new Key(new Text("row1"), currentTime - 5000), null));
        assertFalse(filter.accept(new Key(new Text("row1"), currentTime + 5000), null));
    }
}
