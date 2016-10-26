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
/*
 * (c) Copyright 2009 Talis Information Ltd.
 * (c) Copyright 2010 Epimorphics Ltd.
 * All rights reserved.
 * [See end of file]
 */
package org.apache.rya.jena.jenasesame.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQException;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.PlanBase;
import org.apache.jena.sparql.engine.QueryEngineBase;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.util.Context;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryException;

/**
 * Query Engine for Jena Sesame.
 */
public class JenaSesameQueryEngine extends QueryEngineBase {
    private final JenaSesameDatasetGraph jsdg;
    private Query query = null;

    /**
     * Creates a new instance of {@link JenaSesameQueryEngine}.
     * @param query the {@link Query}. (not {@code null})
     * @param dataset the {@link JenaSesameDatasetGraph}. (not {@code null})
     * @param initial the {@link Binding}.
     * @param context the {@link Context}.
     */
    public JenaSesameQueryEngine(final Query query, final JenaSesameDatasetGraph dataset, final Binding initial, final Context context) {
        // Hide the dataset for now.
        super(checkNotNull(query), checkNotNull(dataset), initial, context);
        this.query = query;
        this.jsdg = dataset;
    }

    @Override
    public QueryIterator eval(final Op op, final DatasetGraph dsg, final Binding initial, final Context context) {
        // Ignore op!
        return null;
    }

    @Override
    protected Op modifyOp(final Op op) {
        // Ignore op!
        return op;
    }

    // This is the one that matters!
    @Override
    public Plan getPlan() {
        // Create query execution.
        try {
            final TupleQuery tupleQuery = jsdg.getConnection().prepareTupleQuery(QueryLanguage.SPARQL, query.toString());
            final TupleQueryResult result = tupleQuery.evaluate();
            final QueryIterator queryIter = new QueryIteratorSesame(result);
            final Closeable closeable = new Closeable() {
                @Override
                public void close() {
                    try {
                        result.close();
                    } catch (final QueryEvaluationException e) {
                        throw new ARQException(e);
                    }
                }
            };

            return new PlanBase(null, closeable) {
                @Override
                protected QueryIterator iteratorOnce() {
                    return queryIter;
                }
            };
        } catch (final RepositoryException | MalformedQueryException | QueryEvaluationException e) {
            throw new ARQException(e);
        }
    }
}

/*
 * (c) Copyright 2009 Talis Information Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */