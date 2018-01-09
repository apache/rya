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
package org.apache.rya.api.model.visibility;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Charsets;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Simplifies Accumulo visibility expressions.
 * 
 * XXX
 * This class has been copied over because Rya has decided to use the Accumulo
 * implementation of visibilities to control who is able to access what data
 * within a Rya instance. Until we implement an Accumulo agnostic method for
 * handling those visibility expressions, we have chosen to pull the Accumulo
 * code into our API.
 *
 * Copied from rya accumulo's org.apache.accumulo.core.util.VisibilitySimplifier
 *   <dependancy>
 *     <groupId>org.apache.rya.accumulo</groupId>
 *     <artifactId>accumulo.rya</artifactId>
 *     <version>3.2.12-incubating-SNAPSHOT</version>
 *   </dependancy>
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