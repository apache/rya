/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.pcj.storage.accumulo;

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.junit.Test;
import org.openrdf.query.MalformedQueryException;

import com.google.common.collect.Sets;

/**
 * Tests the methods of {@link ShiftVarOrderFactory}.
 */
public class ShiftVarOrderFactoryTest {

    @Test
    public void makeVarOrders_fromSPARQL() throws MalformedQueryException {
        // The SPARQL whose PCJ var orders will be generated from.
        final String sparql =
                "SELECT ?a ?b ?c " +
                "WHERE { " +
                    "?a <http://talksTo> ?b. " +
                    "?b <http://worksAt> ?c. " +
                "}";

        // Run the test.
        final PcjVarOrderFactory factory = new ShiftVarOrderFactory();
        final Set<VariableOrder> varOrders = factory.makeVarOrders(sparql);

        // Ensure the returned set matches the expected results.
        final Set<VariableOrder> expected = Sets.newHashSet();
        expected.add( new VariableOrder("a", "b", "c") );
        expected.add( new VariableOrder("c", "a", "b") );
        expected.add( new VariableOrder("b", "c", "a") );
        assertEquals(expected, varOrders);
    }

    @Test
    public void makeVarOrders_fromVarOrder() {
        // The VariableOrder whose PCJ var orders will be generated from.
        final VariableOrder varOrder = new VariableOrder("a", "b", "c");

        // Run the test.
        final PcjVarOrderFactory factory = new ShiftVarOrderFactory();
        final Set<VariableOrder> varOrders = factory.makeVarOrders(varOrder);

        // Ensure the returned set matches the expected results.
        final Set<VariableOrder> expected = Sets.newHashSet();
        expected.add( new VariableOrder("a", "b", "c") );
        expected.add( new VariableOrder("c", "a", "b") );
        expected.add( new VariableOrder("b", "c", "a") );
        assertEquals(expected, varOrders);
    }
}