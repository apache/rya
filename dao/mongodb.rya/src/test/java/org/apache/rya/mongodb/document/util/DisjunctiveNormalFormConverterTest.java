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
package org.apache.rya.mongodb.document.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.accumulo.core.security.ColumnVisibility.Node;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rya.mongodb.document.visibility.DocumentVisibility;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Tests the methods of {@link DisjunctiveNormalFormConverter}.
 */
public class DisjunctiveNormalFormConverterTest {
    private static final ImmutableList<Pair<String, String>> INPUT_AND_EXPECTED_BOOLEAN_EXPRESSIONS =
        ImmutableList.of(
            Pair.of("A", "A"),
            Pair.of("A&B", "A&B"),
            Pair.of("A|B", "A|B"),
            Pair.of("A&(B|C)", "(A&B)|(A&C)"),
            Pair.of("A|(B&C)", "A|(B&C)"),
            Pair.of("HCS|(FDW&TGE&(TS|BX))", "HCS|(BX&FDW&TGE)|(FDW&TGE&TS)")
        );

    /**
     * Test truth table with a {@code null} {@link List}.
     */
    @Test (expected=NullPointerException.class)
    public void testTruthTableNullList() {
        final List<String> list = null;
        DisjunctiveNormalFormConverter.createTruthTableInputs(list);
    }

    /**
     * Test truth table with a {@code null} {@link Node}.
     */
    @Test (expected=NullPointerException.class)
    public void testTruthTableNullNode() {
        final Node node = null;
        final byte[] expression = new DocumentVisibility("A").getExpression();
        DisjunctiveNormalFormConverter.createTruthTableInputs(node, expression);
    }

    /**
     * Test truth table with a {@code null} expression.
     */
    @Test (expected=NullPointerException.class)
    public void testTruthTableNullExpression() {
        final Node node = new DocumentVisibility("A").getParseTree();
        final byte[] expression = null;
        DisjunctiveNormalFormConverter.createTruthTableInputs(node, expression);
    }

    /**
     * Test truth table with 0 inputs.
     */
    @Test
    public void testTruthTableSize() {
        final byte[][] truthTable = DisjunctiveNormalFormConverter.createTruthTableInputs(0);
        final byte[][] expected =
            {
                {}
            };
        assertArrayEquals(expected, truthTable);
    }

    /**
     * Test truth table with 1 inputs
     */
    @Test
    public void testTruthTableSize1() {
        final byte[][] truthTable = DisjunctiveNormalFormConverter.createTruthTableInputs(1);
        final byte[][] expected=
            {
                {0},
                {1}
            };
        assertArrayEquals(expected, truthTable);
    }

    /**
     * Test truth table with 2 inputs from a list..
     */
    @Test
    public void testTruthTableSize2FromList() {
        final byte[][] truthTable = DisjunctiveNormalFormConverter.createTruthTableInputs(ImmutableList.of("A", "B"));
        final byte[][] expected =
            {
                {0, 0},
                {0, 1},
                {1, 0},
                {1, 1}
            };
        assertArrayEquals(expected, truthTable);
    }

    /**
     * Test truth table with 3 inputs from a node.
     */
    @Test
    public void testTruthTableSize3FromNode() {
        final DocumentVisibility dv = new DocumentVisibility("(A&B)|(A&C)");
        final byte[][] truthTable = DisjunctiveNormalFormConverter.createTruthTableInputs(dv.getParseTree(), dv.getExpression());
        final byte[][] expected =
            {
                {0, 0, 0},
                {0, 0, 1},
                {0, 1, 0},
                {0, 1, 1},
                {1, 0, 0},
                {1, 0, 1},
                {1, 1, 0},
                {1, 1, 1}
            };
        assertArrayEquals(expected, truthTable);
    }

    /**
     * Test ability to convert expressions into Disjunctive Normal Form.
     */
    @Test
    public void testConvertToDnf() {
        for (final Pair<String, String> pair : INPUT_AND_EXPECTED_BOOLEAN_EXPRESSIONS) {
            final String input = pair.getLeft();
            final String expected = pair.getRight();

            final DocumentVisibility inputDv = new DocumentVisibility(input);
            final DocumentVisibility resultDv = DisjunctiveNormalFormConverter.convertToDisjunctiveNormalForm(inputDv);

            assertEquals(expected, new String(resultDv.flatten()));
        }
    }
}