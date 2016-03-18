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
package org.apache.rya.indexing.pcj.fluo.app;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.base.Optional;

/**
 * Searches a SPARQL query for {@link Filter}s.
 */
@ParametersAreNonnullByDefault
class FilterFinder {

    /**
     * Search a SPARQL query for the {@link Filter} that appears at the
     * {@code indexWithinQuery}'th time within the query.
     * <p>
     * The top most filter within the query will be at index 0, the next filter
     * encountered will be at index 1, ... and the last index that is encountered
     * will be at index <i>n</i>.
     *
     * @param sparql - The SPARQL query that to parse. (not null)
     * @param indexWithinQuery - The index of the filter to fetch. (not null)
     * @return The filter that was found within the query at the specified index;
     *   otherwise absent.
     * @throws Exception Thrown when the query could not be parsed or iterated over.
     */
    public Optional<Filter> findFilter(final String sparql, final int indexWithinQuery) throws Exception {
        checkNotNull(sparql);
        checkArgument(indexWithinQuery >= 0);

        // When a filter is encountered for the requested index, store it in atomic reference and quit searching.
        final AtomicReference<Filter> filterRef = new AtomicReference<>();
        final QueryModelVisitorBase<RuntimeException> filterFinder = new QueryModelVisitorBase<RuntimeException>() {
            private int i = 0;
            @Override
            public void meet(final Filter filter) {
                // Store and stop searching.
                if(i == indexWithinQuery) {
                    filterRef.set(filter);
                    return;
                }

                // Continue to the next filter.
                i++;
                super.meet(filter);
            }
        };

        // Parse the query and find the filter.
        final SPARQLParser parser = new SPARQLParser();
        final ParsedQuery parsedQuery = parser.parseQuery(sparql, null);
        parsedQuery.getTupleExpr().visit(filterFinder);
        return Optional.fromNullable(filterRef.get());
    }
}