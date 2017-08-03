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
package org.apache.rya.mongodb.document.operators.query;

import java.util.Arrays;

import org.bson.Document;

/**
 * Utility methods for comparison operators.
 */
public final class ComparisonOperators {
    /**
     * Private constructor to prevent instantiation.
     */
    private ComparisonOperators() {
    }

    /**
     * Creates a $gt MongoDB expression.
     *
     * @param expression the expression.
     * @param value the value to test if the expression is greater than
     * @return the $gt expression {@link Document}.
     */
    public static Document gt(final Document expression, final Number value) {
        return new Document("$gt", Arrays.asList(expression, value));
    }

    /**
     * Creates an $eq MongoDB expression.
     *
     * @param expression1 the first expression.
     * @param expression2 the second expression.
     * @return the $eq expression {@link Document}.
     */
    public static Document eq(final Document expression1, final Object expression2) {
        return new Document("$eq", Arrays.asList(expression1, expression2));
    }
}