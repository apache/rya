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
package org.apache.rya.accumulo.utils;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.security.ColumnVisibility;

import com.google.common.base.Charsets;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Simplifies Accumulo visibility expressions.
 */
@DefaultAnnotation(NonNull.class)
public class VisibilitySimplifier {

    /**
     * Unions two visibility equations and then simplifies the result.
     *
     * @param vis1 - The first visibility equation that will be unioned. (not null)
     * @param vis2 - The other visibility equation that will be unioned. (not null)
     * @return A simplified form of the unioned visibility equations.
     */
    public static String unionAndSimplify(final String vis1, final String vis2) {
        requireNonNull(vis1);
        requireNonNull(vis2);

        if(vis1.isEmpty()) {
            return vis2;
        }

        if(vis2.isEmpty()) {
            return vis1;
        }

        return simplify("(" + vis1 + ")&(" + vis2 + ")");
    }

    /**
     * Simplifies an Accumulo visibility expression.
     *
     * @param visibility - The expression to simplify. (not null)
     * @return A simplified form of {@code visibility}.
     */
    public static String simplify(final String visibility) {
        requireNonNull(visibility);

        String last = visibility;
        String simplified = new String(new ColumnVisibility(visibility).flatten(), Charsets.UTF_8);

        while(!simplified.equals(last)) {
            last = simplified;
            simplified = new String(new ColumnVisibility(simplified).flatten(), Charsets.UTF_8);
        }

        return simplified;
    }
}