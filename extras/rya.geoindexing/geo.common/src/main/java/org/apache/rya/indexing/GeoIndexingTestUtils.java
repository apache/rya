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
package org.apache.rya.indexing;

import java.util.HashSet;
import java.util.Set;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;

/**
 * Utility methods to help test geo indexing methods.
 */
public final class GeoIndexingTestUtils {
    /**
     * Private constructor to prevent instantiation.
     */
    private GeoIndexingTestUtils () {
    }

    /**
     * Generates a set of items from the specified iterator.
     * @param iter a {@link CloseableIteration}.
     * @return the {@link Set} of items from the iterator or an empty set if
     * none were found.
     * @throws Exception
     */
    public static <X> Set<X> getSet(final CloseableIteration<X, ?> iter) throws Exception {
        final Set<X> set = new HashSet<X>();
        while (iter.hasNext()) {
            final X item = iter.next();
            set.add(item);
        }
        return set;
    }
}