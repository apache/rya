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
package org.apache.rya.indexing.external.tupleSet;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedQuery;

import com.google.common.base.Optional;

/**
 * Utilities that help applications inspect {@link ParsedQuery} objects.
 */
@ParametersAreNonnullByDefault
public class ParsedQueryUtil {

    /**
     * Finds the first {@link Projection} node within a {@link ParsedQuery}.
     *
     * @param query - The query that will be searched. (not null)
     * @return The first projection encountered if the query has one; otherwise absent.
     */
    public Optional<Projection> findProjection(final ParsedQuery query) {
        checkNotNull(query);

        // When a projection is encountered for the requested index, store it in atomic reference and quit searching.
        final AtomicReference<Projection> projectionRef = new AtomicReference<>();

        query.getTupleExpr().visit(new QueryModelVisitorBase<RuntimeException>() {
            @Override
            public void meet(Projection projection) {
                projectionRef.set(projection);
            }
        });

        return Optional.fromNullable( projectionRef.get() );
    }
}