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
package org.apache.rya.api.persist.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.rio.RDFHandler;

import info.aduna.iteration.CloseableIteration;

/**
 * Wraps Rya DAO queries into a simpler interface that just passes in the
 * statement to query for and a handler for dealing with each statement in the
 * query result. This handles iterating over the query, throwing any exceptions,
 * and closing the query iterator when done. The same wrapper can be re-used
 * for multiple queries.
 */
public class RyaDaoQueryWrapper {
    private final RyaDAO<?> ryaDao;
    private final RdfCloudTripleStoreConfiguration conf;

    /**
     * Creates a new instance of {@link RyaDaoQueryWrapper}.
     * @param ryaDao the {@link RyaDAO}. (not {@code null})
     * @param conf the {@link RdfCloudTripleStoreConfiguration}.
     * (not {@code null})
     */
    public RyaDaoQueryWrapper(final RyaDAO<?> ryaDao, final RdfCloudTripleStoreConfiguration conf) {
        this.ryaDao = checkNotNull(ryaDao);
        this.conf = checkNotNull(conf);
    }

    /**
     * Creates a new instance of {@link RyaDaoQueryWrapper}.
     * @param ryaDao the {@link RyaDAO}. (not {@code null})
     */
    public RyaDaoQueryWrapper(final RyaDAO<?> ryaDao) {
        this(checkNotNull(ryaDao), ryaDao.getConf());
    }

    /**
     * Handles all results of a query. Closes the query iterator when done.
     * @param subject the subject {@link Resource} to query for.
     * @param predicate the predicate {@link URI} to query for.
     * @param object the object {@link Value} to query for.
     * @param rdfStatementHandler the {@link RDFHandler} to use for handling
     * each statement returned. (not {@code null})
     * @param contexts the context {@link Resource}s to query for.
     * @throws QueryEvaluationException
     */
    public void queryAll(final Resource subject, final URI predicate, final Value object, final RDFHandler rdfStatementHandler, final Resource... contexts) throws QueryEvaluationException {
        checkNotNull(rdfStatementHandler);
        final CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDao, subject, predicate, object, conf, contexts);
        try {
            while (iter.hasNext()) {
                final Statement statement = iter.next();
                try {
                    rdfStatementHandler.handleStatement(statement);
                } catch (final Exception e) {
                    throw new QueryEvaluationException("Error handling statement.", e);
                }
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Handles all results of a query. Closes the query iterator when done.
     * @param statement the {@link Statement} to query for. (not {@code null})
     * @param rdfStatementHandler the {@link RDFHandler} to use for handling
     * each statement returned. (not {@code null})
     * @throws QueryEvaluationException
     */
    public void queryAll(final Statement statement, final RDFHandler rdfStatementHandler) throws QueryEvaluationException {
        final Resource subject = statement.getSubject();
        final URI predicate = statement.getPredicate();
        final Value object = statement.getObject();
        final Resource context = statement.getContext();
        queryAll(subject, predicate, object, rdfStatementHandler, context);
    }

    /**
     * Handles all results of a query. Closes the query iterator when done.
     * @param ryaStatement the {@link RyaStatement} to query for.
     * (not {@code null})
     * @param rdfStatementHandler the {@link RDFHandler} to use for handling
     * each statement returned. (not {@code null})
     * @throws QueryEvaluationException
     */
    public void queryAll(final RyaStatement ryaStatement, final RDFHandler rdfStatementHandler) throws QueryEvaluationException {
        checkNotNull(ryaStatement);
        final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
        queryAll(statement, rdfStatementHandler);
    }

    /**
     * Handles only the first result of a query. Closes the query iterator when
     * done.
     * @param subject the subject {@link Resource} to query for.
     * @param predicate the predicate {@link URI} to query for.
     * @param object the object {@link Value} to query for.
     * @param rdfStatementHandler the {@link RDFHandler} to use for handling the
     * first statement returned. (not {@code null})
     * @param contexts the context {@link Resource}s to query for.
     * @throws QueryEvaluationException
     */
    public void queryFirst(final Resource subject, final URI predicate, final Value object, final RDFHandler rdfStatementHandler, final Resource... contexts) throws QueryEvaluationException {
        checkNotNull(rdfStatementHandler);
        final CloseableIteration<Statement, QueryEvaluationException> iter = RyaDAOHelper.query(ryaDao, subject, predicate, object, conf, contexts);
        try {
            if (iter.hasNext()) {
                final Statement statement = iter.next();
                try {
                    rdfStatementHandler.handleStatement(statement);
                } catch (final Exception e) {
                    throw new QueryEvaluationException("Error handling statement.", e);
                }
            }
        } finally {
            if (iter != null) {
                iter.close();
            }
        }
    }

    /**
     * Handles only the first result of a query. Closes the query iterator when
     * done.
     * @param statement the {@link Statement} to query for. (not {@code null})
     * @param rdfStatementHandler the {@link RDFHandler} to use for handling the
     * first statement returned. (not {@code null})
     * @throws QueryEvaluationException
     */
    public void queryFirst(final Statement statement, final RDFHandler rdfStatementHandler) throws QueryEvaluationException {
        checkNotNull(statement);
        final Resource subject = statement.getSubject();
        final URI predicate = statement.getPredicate();
        final Value object = statement.getObject();
        final Resource context = statement.getContext();
        queryFirst(subject, predicate, object, rdfStatementHandler, context);
    }

    /**
     * Handles only the first result of a query. Closes the query iterator when
     * done.
     * @param ryaStatement the {@link RyaStatement} to query for.
     * (not {@code null})
     * @param rdfStatementHandler the {@link RDFHandler} to use for handling the
     * first statement returned. (not {@code null})
     * @throws QueryEvaluationException
     */
    public void queryFirst(final RyaStatement ryaStatement, final RDFHandler rdfStatementHandler) throws QueryEvaluationException {
        checkNotNull(ryaStatement);
        final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
        queryFirst(statement, rdfStatementHandler);
    }
}