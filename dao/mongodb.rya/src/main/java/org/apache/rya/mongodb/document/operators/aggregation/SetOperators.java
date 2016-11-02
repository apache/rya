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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.mongodb.document.operators.query.ConditionalOperators.ifNull;

import java.util.Arrays;
import java.util.Collections;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
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
     * @return the $setIntersection expression {@link BasicDBObject}.
     */
    public static BasicDBObject setIntersection(final String field, final Object[] set) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        return (BasicDBObject) setIntersection(builder, field, set).get();
    }

    /**
     * Checks if the field intersects the set.
     * @param builder the {@link BasicDBObjectBuilder}. (not {@code null})
     * @param field the field to check.
     * @param set the set to check.
     * @return the $setIntersection expression {@link BasicDBObjectBuilder}.
     */
    public static BasicDBObjectBuilder setIntersection(final BasicDBObjectBuilder builder, final String field, final Object[] set) {
        checkNotNull(builder);
        builder.add("$setIntersection", Arrays.asList(field, set));
        return builder;
    }

    /**
     * Checks if the expression is a subset of the set.
     * @param expression the expression to see if it's in the set.
     * @param set the set to check against.
     * @return the $setIsSubset expression {@link BasicDBObject}.
     */
    public static BasicDBObject setIsSubset(final DBObject expression, final Object[] set) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        return (BasicDBObject) setIsSubset(builder, expression, set).get();
    }

    /**
     * Checks if the expression is a subset of the set.
     * @param builder the {@link BasicDBObjectBuilder}. (not {@code null})
     * @param expression the expression to see if it's in the set.
     * @param set the set to check against.
     * @return the $setIsSubset expression {@link BasicDBObjectBuilder}.
     */
    public static BasicDBObjectBuilder setIsSubset(final BasicDBObjectBuilder builder, final DBObject expression, final Object[] set) {
        checkNotNull(builder);
        builder.add("$setIsSubset", Arrays.asList(expression, set).toArray(new Object[0]));
        return builder;
    }

    /**
     * Checks if the field is a subset of the set and is safe if the field is
     * null.
     * @param field the field to see if it's in the set.
     * @param set the set to check against.
     * @return the $setIsSubset expression {@link BasicDBObject}.
     */
    public static BasicDBObject setIsSubsetNullSafe(final String field, final Object[] set) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        return (BasicDBObject) setIsSubsetNullSafe(builder, field, set).get();
    }

    /**
     * Checks if the field is a subset of the set and is safe if the field is
     * null.
     * @param builder the {@link BasicDBObjectBuilder}. (not {@code null})
     * @param field the field to see if it's in the set.
     * @param set the set to check against.
     * @return the $setIsSubset expression {@link BasicDBObjectBuilder}.
     */
    public static BasicDBObjectBuilder setIsSubsetNullSafe(final BasicDBObjectBuilder builder, final String field, final Object[] set) {
        checkNotNull(builder);
        final Object emptyAccess = Collections.emptyList().toArray();
        return setIsSubset(builder,
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
     * @return the $anyElementTrue expression {@link BasicDBObject}.
     */
    public static BasicDBObject anyElementTrue(final DBObject expression) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        return (BasicDBObject) anyElementTrue(builder, expression).get();
    }

    /**
     * Checks if any elements from the expression are {@code true}.
     * @param builder the {@link BasicDBObjectBuilder}. (not {@code null})
     * @param expression the expression to see if any elements are {@code true}.
     * @return the $anyElementTrue expression {@link BasicDBObjectBuilder}.
     */
    public static BasicDBObjectBuilder anyElementTrue(final BasicDBObjectBuilder builder, final DBObject expression) {
        checkNotNull(builder);
        builder.add("$anyElementTrue", expression);
        return builder;
    }
}