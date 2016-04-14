package org.apache.rya.indexing.pcj.storage.accumulo;

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

import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.indexing.pcj.storage.accumulo.ShiftVarOrderFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Tests the classes and methods of {@link PcjTables}.
 */
public class PcjTablesTests {

    @Test
    public void variableOrder_hashCode() {
        assertEquals(new VariableOrder("a", "b", "C").hashCode(), new VariableOrder("a", "b", "C").hashCode());
    }

    @Test
    public void variableOrder_equals() {
        assertEquals(new VariableOrder("a", "b", "C"), new VariableOrder("a", "b", "C"));
    }

    @Test
    public void variableOrder_fromString() {
        assertEquals(new VariableOrder("a", "b", "c"), new VariableOrder("a;b;c"));
    }

    @Test
    public void variableORder_toString() {
        assertEquals("a;b;c", new VariableOrder("a", "b", "c").toString());
    }

    @Test
    public void pcjMetadata_hashCode() {
        PcjMetadata meta1 = new PcjMetadata("A SPARQL string.", 5, Sets.newHashSet(new VariableOrder("a", "b", "c"), new VariableOrder("d", "e", "f")));
        PcjMetadata meta2 = new PcjMetadata("A SPARQL string.", 5, Sets.newHashSet(new VariableOrder("a", "b", "c"), new VariableOrder("d", "e", "f")));
        assertEquals(meta1.hashCode(), meta2.hashCode());
    }

    @Test
    public void pcjMetadata_equals() {
        PcjMetadata meta1 = new PcjMetadata("A SPARQL string.", 5, Sets.newHashSet(new VariableOrder("a", "b", "c"), new VariableOrder("d", "e", "f")));
        PcjMetadata meta2 = new PcjMetadata("A SPARQL string.", 5, Sets.newHashSet(new VariableOrder("a", "b", "c"), new VariableOrder("d", "e", "f")));
        assertEquals(meta1, meta2);
    }

    @Test
    public void shiftVarOrdersFactory() {
        Set<VariableOrder> expected = Sets.newHashSet(
                new VariableOrder("a;b;c"),
                new VariableOrder("b;c;a"),
                new VariableOrder("c;a;b"));

        Set<VariableOrder> varOrders = new ShiftVarOrderFactory().makeVarOrders(new VariableOrder("a;b;c"));
        assertEquals(expected, varOrders);
    }

}