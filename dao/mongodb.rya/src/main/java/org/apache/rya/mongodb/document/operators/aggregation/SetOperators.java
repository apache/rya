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

import static org.apache.rya.mongodb.document.operators.query.ConditionalOperators.ifNull;

import java.util.Arrays;
import java.util.Collections;

import org.bson.Document;

import com.mongodb.DBObject;

/**
 * Utility methods for MongoDB set operators.
 */
public final class SetOperators {
    /**
     * Private constructor to prevent instantiation.
     */
    private SetOperators() {
    }

    /**
     * Checks if the field intersects the set.
     * @param field the field to check.
     * @param set the set to check.
     * @return the $setIntersection expression {@link Document}.
     */
    public static Document setIntersection(final String field, final Object[] set) {
        return new Document("$setIntersection", Arrays.asList(field, set));
    }

    /**
     * Checks if the expression is a subset of the set.
     * @param expression the expression to see if it's in the set.
     * @param set the set to check against.
     * @return the $setIsSubset expression {@link Document}.
     */
    public static Document setIsSubset(final Document expression, final Object[] set) {
        return new Document("$setIsSubset", Arrays.asList(expression, set).toArray(new Object[0]));
    }

    /**
     * Checks if the field is a subset of the set and is safe if the field is
     * null.
     * @param field the field to see if it's in the set.
     * @param set the set to check against.
     * @return the $setIsSubset expression {@link Document}.
     */
    public static Document setIsSubsetNullSafe(final String field, final Object[] set) {
        final Object emptyAccess = Collections.emptyList().toArray();
        return setIsSubset(
            ifNull(
                field,
                emptyAccess
            ),
            set
        );
    }

    /**
     * Checks if any elements from the expression are {@code true}.
     * @param expression the expression to see if any elements are {@code true}.
     * @return the $anyElementTrue expression {@link Document}.
     */
    public static Document anyElementTrue(final Document expression) {
        return new Document("$anyElementTrue", expression);
    }
}