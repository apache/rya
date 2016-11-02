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

import static com.google.common.base.Preconditions.checkNotNull;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;

/**
 * Utility methods for array operators.
 */
public final class ArrayOperators {
    /**
     * Private constructor to prevent instantiation.
     */
    private ArrayOperators() {
    }

    /**
     * Creates an $size MongoDB expression.
     * @param expression the expression to get the size of.
     * @return the $size expression {@link BasicDBObject}.
     */
    public static BasicDBObject size(final Object expression) {
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        return (BasicDBObject) size(builder, expression).get();
    }

    /**
     * Creates an $size MongoDB expression.
     * @param builder the {@link BasicDBObjectBuilder}. (not {@code null})
     * @param expression the expression to get the size of.
     * @return the $size expression {@link BasicDBObjectBuilder}.
     */
    public static BasicDBObjectBuilder size(final BasicDBObjectBuilder builder, final Object expression) {
        checkNotNull(builder);
        builder.add("$size", expression);
        return builder;
    }
}