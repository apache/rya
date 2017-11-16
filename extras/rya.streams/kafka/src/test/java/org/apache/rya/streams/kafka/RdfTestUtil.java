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
package org.apache.rya.streams.kafka;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.atomic.AtomicReference;

import org.openrdf.query.algebra.MultiProjection;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A set of utility functions that are useful when writing tests RDF functions.
 */
@DefaultAnnotation(NonNull.class)
public final class RdfTestUtil {

    private RdfTestUtil() { }

    /**
     * Fetch the {@link StatementPattern} from a SPARQL string.
     *
     * @param sparql - A SPARQL query that contains only a single Statement Patern. (not nul)
     * @return The {@link StatementPattern} that was in the query, if it could be found. Otherwise {@code null}
     * @throws Exception The statement pattern could not be found in the parsed SPARQL query.
     */
    public static @Nullable StatementPattern getSp(final String sparql) throws Exception {
        requireNonNull(sparql);

        final AtomicReference<StatementPattern> statementPattern = new AtomicReference<>();
        final ParsedQuery parsed = new SPARQLParser().parseQuery(sparql, null);
        parsed.getTupleExpr().visitChildren(new QueryModelVisitorBase<Exception>() {
            @Override
            public void meet(final StatementPattern node) throws Exception {
                statementPattern.set(node);
            }
        });
        return statementPattern.get();
    }

    /**
     * Get the first {@link Projection} node from a SPARQL query.
     *
     * @param sparql - The query that contains a single Projection node.
     * @return The first {@link Projection} that is encountered.
     * @throws Exception The query could not be parsed.
     */
    public static @Nullable Projection getProjection(final String sparql) throws Exception {
        requireNonNull(sparql);

        final AtomicReference<Projection> projection = new AtomicReference<>();
        final ParsedQuery parsed = new SPARQLParser().parseQuery(sparql, null);
        parsed.getTupleExpr().visit(new QueryModelVisitorBase<Exception>() {
            @Override
            public void meet(final Projection node) throws Exception {
                projection.set(node);
            }
        });

        return projection.get();
    }

    /**
     * Get the first {@link MultiProjection} node from a SPARQL query.
     *
     * @param sparql - The query that contains a single Projection node.
     * @return The first {@link MultiProjection} that is encountered.
     * @throws Exception The query could not be parsed.
     */
    public static @Nullable MultiProjection getMultiProjection(final String sparql) throws Exception {
        requireNonNull(sparql);

        final AtomicReference<MultiProjection> multiProjection = new AtomicReference<>();
        final ParsedQuery parsed = new SPARQLParser().parseQuery(sparql, null);
        parsed.getTupleExpr().visit(new QueryModelVisitorBase<Exception>() {
            @Override
            public void meet(final MultiProjection node) throws Exception {
                multiProjection.set(node);
            }
        });

        return multiProjection.get();
    }
}