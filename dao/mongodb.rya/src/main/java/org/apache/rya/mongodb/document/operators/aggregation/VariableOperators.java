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
package org.apache.rya.mongodb.document.operators.aggregation;

import javax.xml.parsers.DocumentBuilder;

import org.bson.Document;

/**
 * Utility methods for variable operators.
 */
public final class VariableOperators {
    /**
     * Private constructor to prevent instantiation.
     */
    private VariableOperators() {
    }

    /**
     * Applies an expression to each item in an array and returns an array with
     * the applied results.
     * @param input an expression that resolves to an array.
     * @param as the variable name for the items in the {@code input} array.
     * The {@code in} expression accesses each item in the {@code input} array
     * by this variable.
     * @param in the expression to apply to each item in the {@code input}
     * array. The expression accesses the item by its variable name.
     * @return the $map expression {@link DocumentBuilder}.
     */
    public static Document map(final String input, final String as, final Document in) {
        final Document mapDoc = new Document()
            .append("input", input)
            .append("as", as)
            .append("in", in);

        return new Document("$map", mapDoc);
    }
}