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
package org.apache.rya.accumulo.query;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.rya.api.domain.RyaType;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.ListBindingSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * <p>Tests the methods of {@link RangeBindingSetEntries}.</p>
 *
 * <p>This class is critical is two ways:
 * <ol>
 *     <li>it is critical to getting the correct answer for any SPARQL query, and</li>
 *     <li>it is critical to the performance of the query time.</li>
 * </ol></p>
 *
 * <p>This class tests for both the correct answer and for fast performance.</p>
 *
 * <p>The Glowroot Java profiler found that for a complex SPARQL query that takes about 40 seconds to return,
 * a huge 85% of the query time was spend in this class, doing the {@link RangeBindingSetEntries#containsKey(Key)}
 * function. Therefore optimising the performance of this method is critical to overall Rya performance.</p>
 */
public class RangeBindingSetEntriesTest {

    private final static String CONTEXT = "context";
    private final static String NAME = "name";
    private final static String VALUE = "value";

    private BindingSet createBindingSet(char letter1, char letter2) {
        List<String> bindingNames = new ArrayList<>();
        for (int i = 100; i <= 120; i++) {
            bindingNames.add(NAME + letter1 + letter2 + i);
        }
        List<Value> values = new ArrayList<>();
        for (int i = 100; i <= 120; i++) {
            values.add(new RyaType(VALUE + letter1 + letter2 + i));
        }
        BindingSet bs = new ListBindingSet(bindingNames, values);
        return bs;
    }

    @Test
    public void testRow() {
        final RangeBindingSetEntries entries = new RangeBindingSetEntries();
        // Generate enough test data to check for performance characteristics
        for (char letter1 = 'A'; letter1 <= 'Z'; letter1++) {
            for (char letter2 = 'A'; letter2 <= 'Z'; letter2++) {
                // Create genuine ranges of "AA1-AA9" through "ZZ1-ZZ9".
                Range range = new Range(
                        new Key("" + letter1 + letter2 + 1),
                        new Key("" + letter1 + letter2 + 9));
                BindingSet bs = createBindingSet(letter1, letter2);
                entries.put(range, bs);
            }
        }

        char letter1 = 'S'; // Random letter between A and Z
        char letter2 = 'F'; // Random letter between A and Z
        short number = 5;   // Random number between 1 and 9
        HashSet<BindingSet> bsSet = new HashSet<>();
        // This will match one of the entries from the test data
        BindingSet bs = createBindingSet(letter1, letter2);
        bsSet.add(bs);
        assertEquals(bsSet, entries.containsKey(
                new Key("" + letter1 + letter2 + number)));
    }

    @Test
    public void testColumn() {
        final RangeBindingSetEntries entries = new RangeBindingSetEntries();
        // Generate enough test data to check for performance characteristics
        for (char letter1 = 'A'; letter1 <= 'Z'; letter1++) {
            for (char letter2 = 'A'; letter2 <= 'Z'; letter2++) {
                BindingSet bs = createBindingSet(letter1, letter2);
                // Create genuine ranges of "AA1-AA9" through "ZZ1-ZZ9".
                // Create an example with the correct column, and one with an incorrect column.
                Range rangeRight = new Range(
                        new Key("" + letter1 + letter2 + 1, CONTEXT + letter1 + letter2 + "right"),
                        new Key("" + letter1 + letter2 + 9, CONTEXT + letter1 + letter2 + "right"));
                Range rangeWrong = new Range(
                        new Key("" + letter1 + letter2 + 1, CONTEXT + letter1 + letter2 + "wrong"),
                        new Key("" + letter1 + letter2 + 9, CONTEXT + letter1 + letter2 + "wrong"));
                entries.put(rangeRight, bs);
                entries.put(rangeWrong, bs);
            }
        }

        char letter1 = 'T'; // Random letter between A and Z
        char letter2 = 'H'; // Random letter between A and Z
        short number = 6;   // Random number between 1 and 9
        HashSet<BindingSet> bsSet = new HashSet<>();
        // This will match one of the entries from the test data
        BindingSet bs = createBindingSet(letter1, letter2);
        bsSet.add(bs);
        assertEquals(bsSet, entries.containsKey(
                new Key("" + letter1 + letter2 + number, CONTEXT + letter1 + letter2 + "right")));
    }

}