package org.apache.rya.accumulo.query;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.eclipse.rdf4j.query.BindingSet;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Class RangeBindingSetCollection Date: Feb 23, 2011 Time: 10:15:48 AM
 */
public class RangeBindingSetEntries {
    private Multimap<Range, BindingSet> ranges = HashMultimap.create();

    public RangeBindingSetEntries() {
        ranges = HashMultimap.create();
    }

    public void put(Range range, BindingSet bs) {
        ranges.put(range, bs);
    }

    public Collection<BindingSet> containsKey(Key key) {
        Set<BindingSet> bsSet = new HashSet<>();
        for (Range range : ranges.keySet()) {
            // Check to see if the Key falls within Range and has same ColumnFamily
            // as beginning and ending key of Range.
            // The additional ColumnFamily check by the method
            // validateContext(...) is necessary because range.contains(key)
            // returns true if only the Row is within the Range but the ColumnFamily
            // doesn't fall within the Range ColumnFamily bounds.
            if (range.contains(key) && validateContext(key.getColumnFamily(), range.getStartKey().getColumnFamily(),
                    range.getEndKey().getColumnFamily())) {
                bsSet.addAll(ranges.get(range));
            }
        }
        return bsSet;
    }

    /**
     * 
     * @param colFamily
     * @param startColFamily
     * @param stopColFamily
     * @return true if colFamily lies between startColFamily and stopColFamily
     */
    private boolean validateContext(Text colFamily, Text startColFamily, Text stopColFamily) {
        byte[] cfBytes = colFamily.getBytes();
        byte[] start = startColFamily.getBytes();
        byte[] stop = stopColFamily.getBytes();
        // range has empty column family, so all Keys falling with Range Row
        // constraints should match
        if (start.length == 0 && stop.length == 0) {
            return true;
        }
        int result1 = WritableComparator.compareBytes(cfBytes, 0, cfBytes.length, start, 0, start.length);
        int result2 = WritableComparator.compareBytes(cfBytes, 0, cfBytes.length, stop, 0, stop.length);
        return result1 >= 0 && result2 <= 0;
    }
}
