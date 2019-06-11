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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.rya.mongodb.document.visibility.DocumentVisibility;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;

/**
 * Tests the methods of {@link DocumentVisibilityUtil}.
 */
public class DocumentVisibilityUtilTest {
    private static final Logger log = Logger.getLogger(DocumentVisibilityUtilTest.class);

    private static final ImmutableSet<Pair<String, String>> INPUT_AND_EXPECTED_BOOLEAN_EXPRESSIONS =
        ImmutableSet.copyOf(Arrays.asList(
            Pair.of("", ""),
            Pair.of("A", "A"),
            Pair.of("\"A\"", "\"A\""),
            Pair.of("(A)", "A"),
            Pair.of("A|B", "A|B"),
            Pair.of("(A)|(B)|(C)", "A|B|C"),
            Pair.of("(A|B)", "A|B"),
            Pair.of("A|B|C", "A|B|C"),
            Pair.of("(A|B|C)", "A|B|C"),
            Pair.of("A&B", "A&B"),
            Pair.of("(A&B)", "A&B"),
            Pair.of("A&B&C", "A&B&C"),
            Pair.of("(A&B&C)", "A&B&C"),
            Pair.of("(A&B)|(C&D)", "(A&B)|(C&D)"),
            Pair.of("A&(B|C)", "(A&B)|(A&C)"),
            Pair.of("(A&B)|C", "C|(A&B)"),
            Pair.of("A|(B&C)", "A|(B&C)"),
            Pair.of("(A|B)&C", "(A&C)|(B&C)"),
            Pair.of("(A&B)&(C|D)", "(A&B&C)|(A&B&D)"),
            Pair.of("(A|B)|(C&D)", "A|B|(C&D)"),
            Pair.of("(A|B)&(C|D)", "(A&C)|(A&D)|(B&C)|(B&D)"),
            Pair.of("(A|B)&(C|(D&E))", "(A&C)|(B&C)|(A&D&E)|(B&D&E)"),
            Pair.of("G|(FDW&TGE)", "G|(FDW&TGE)"),
            Pair.of("HCS|(FDW&TGE&(TS|BX))", "HCS|(BX&FDW&TGE)|(FDW&TGE&TS)"),
            Pair.of("\"A&B\"", "\"A&B\""),
            Pair.of("\"A\"&\"B\"", "\"A\"&\"B\""),
            Pair.of("\"A\"|\"B\"", "\"A\"|\"B\""),
            Pair.of("\"A#C\"&B", "\"A#C\"&B"),
            Pair.of("\"A#C\"&\"B+D\"", "\"A#C\"&\"B+D\""),
            Pair.of("(A|C)&(B|C)", "C|(A&B)"),
            Pair.of("(A&B)|(A&C)", "(A&B)|(A&C)")

        ));

    private static final ImmutableSet<String> INVALID_BOOLEAN_EXPRESSIONS =
        ImmutableSet.of(
            "A|B&C",
            "A=B",
            "A|B|",
            "A&|B",
            "()",
            ")",
            "dog|!cat"
        );

    @Test
    public void testGoodExpressions() throws DocumentVisibilityConversionException {
        int count = 1;
        for (final Pair<String, String> pair : INPUT_AND_EXPECTED_BOOLEAN_EXPRESSIONS) {
            final String booleanExpression = pair.getLeft();
            final String expected = pair.getRight();
            log.info("Valid Test: " + count);
            log.info("Original: " + booleanExpression);

            // Convert to multidimensional array
            final DocumentVisibility dv = new DocumentVisibility(booleanExpression);
            final List<Object> multidimensionalArray = DocumentVisibilityUtil.toMultidimensionalArray(dv);
            log.info("Array   : " + Arrays.deepToString(multidimensionalArray.toArray()));

            // Convert multidimensional array back to string
            final String booleanStringResult = DocumentVisibilityUtil.multidimensionalArrayToBooleanString(multidimensionalArray.toArray());
            log.info("Result  : " + booleanStringResult);

            // Compare results
            assertEquals(expected, booleanStringResult);
            log.info("===========================");
            count++;
        }
    }

    @Test
    public void testBadExpressions() {
        int count = 1;
        for (final String booleanExpression : INVALID_BOOLEAN_EXPRESSIONS) {
            log.info("Invalid Test: " + count);
            try {
                log.info("Original: " + booleanExpression);
                // Convert to multidimensional array
                final DocumentVisibility dv = new DocumentVisibility(booleanExpression);
                final List<Object> multidimensionalArray = DocumentVisibilityUtil.toMultidimensionalArray(dv);
                log.info("Array   : " + Arrays.deepToString(multidimensionalArray.toArray()));

                // Convert multidimensional array back to string
                final String booleanString = DocumentVisibilityUtil.multidimensionalArrayToBooleanString(multidimensionalArray.toArray());
                log.info("Result  : " + booleanString);

                // Compare results
                final String expected = new String(dv.flatten(), Charsets.UTF_8);
                assertEquals(expected, booleanString);
                fail("Bad expression passed.");
            } catch (final Exception e) {
                // Expected
            }
            log.info("===========================");
            count++;
        }
    }
}