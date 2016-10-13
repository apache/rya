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
package org.apache.rya.jena.jenasesame.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.shared.JenaException;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.DatasetPrefixStorage;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.apache.log4j.Logger;
import org.apache.rya.jena.legacy.graph.BulkUpdateHandler;
import org.apache.rya.jena.legacy.graph.query.QueryHandler;
import org.apache.rya.jena.legacy.graph.query.SimpleQueryHandler;
import org.apache.rya.jena.legacy.sparql.graph.GraphBase2;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

/**
 * Graph Repository.
 */
public class GraphRepository extends GraphBase2 {
    private static final Logger log = Logger.getLogger(GraphRepository.class);

    private RepositoryConnection connection;
    private ValueFactory valueFactory;
    private Resource[] contexts;
    private final BulkUpdateHandler bulkUpdateHandler = new BulkUpdateHandlerNoIterRemove(this);

    /**
     * Creates a new instance of {@link GraphRepository}.
     * @param connection the {@link RepositoryConnection}. (not {@code null})
     * @param context the {@link Resource}.
     */
    public GraphRepository(final RepositoryConnection connection, final Resource context) {
        init(connection, context);
    }

    /**
     * Creates a new instance of {@link GraphRepository}.
     * @param connection the {@link RepositoryConnection}. (not {@code null})
     */
    public GraphRepository(final RepositoryConnection connection) {
        init(connection);
    }

    private void init(final RepositoryConnection connection, final Resource... contexts) {
        this.contexts = checkNotNull(contexts);
        this.connection = connection;
        this.valueFactory = connection.getValueFactory();
    }

    @Override
    public void performAdd(final Triple t) {
        final Node s = t.getSubject();
        final Node p = t.getPredicate();
        final Node o = t.getObject();

        final Resource subj   = Convert.nodeToResource(valueFactory, s);
        final URI pred        = Convert.nodeToURI(valueFactory, p);
        final Value obj       = Convert.nodeToValue(valueFactory, o);

        try {
            final Statement stmt = valueFactory.createStatement(subj, pred, obj);
            connection.add(stmt, contexts);
        } catch (final RepositoryException e) {
            log.error("Failed to add statement", e);
            throw new JenaException(e);
        }
    }

    @Override
    public BulkUpdateHandler getBulkUpdateHandler() {
        return bulkUpdateHandler;
    }

    @Override
    public void performDelete(final Triple t) {
        final Node s = t.getSubject();
        final Node p = t.getPredicate();
        final Node o = t.getObject();

        final Resource subj   = Convert.nodeToResource(valueFactory, s);
        final URI pred        = Convert.nodeToURI(valueFactory, p);
        final Value obj       = Convert.nodeToValue(valueFactory, o);

        try {
            final Statement stmt = valueFactory.createStatement(subj, pred, obj);
            connection.remove(stmt, contexts);
        } catch (final RepositoryException e) {
            log.error("Failed to delete statement.", e);
            throw new JenaException(e);
        }
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(final Triple m) {
        final Node s = m.getMatchSubject();
        final Node p = m.getMatchPredicate();
        final Node o = m.getMatchObject();

        final Resource subj   = s == null ? null : Convert.nodeToResource(valueFactory, s);
        final URI pred        = p == null ? null : Convert.nodeToURI(valueFactory, p);
        final Value obj       = o == null ? null : Convert.nodeToValue(valueFactory, o);

        try {
            final RepositoryResult<Statement> iter = connection.getStatements(subj, pred, obj, true, contexts);
            return new RepositoryResultIterator(iter);
        } catch (final RepositoryException e) {
            log.error("Failed to get statements.", e);
            throw new JenaException(e);
        }
    }

    private static class RepositoryResultIterator extends NiceIterator<Triple> {
        private final RepositoryResult<Statement> iter;

        /**
         * Creates a new instance of {@link RepositoryResultIterator}.
         * @param iter the {@link RepositoryResult} collection of
         * {@link Statement}s. (not {@code null})
         */
        public RepositoryResultIterator(final RepositoryResult<Statement> iter) {
            this.iter = checkNotNull(iter);
        }

        @Override
        public void close() {
            try {
                iter.close();
            } catch (final RepositoryException e) {
                throw new JenaException(e);
            }
        }

        @Override
        public boolean hasNext() {
            try {
                return iter.hasNext();
            } catch (final RepositoryException e) {
                throw new JenaException(e);
            }
        }

        @Override
        public Triple next() {
            try {
                final Statement stmt = iter.next();
                return Convert.statementToTriple(stmt);
            } catch (final RepositoryException e) {
                throw new JenaException(e);
            }
        }

        @Override
        public void remove() {
            try {
                iter.remove();
            } catch (final RepositoryException e) {
                throw new JenaException(e);
            }
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (final RepositoryException e) {
            log.error("Failed to close connection.", e);
            throw new JenaException(e);
        }
        super.close();
    }

    @Override
    protected PrefixMapping createPrefixMapping() {
        final DatasetPrefixStorage dps = new JenaSesameDatasetPrefixStorage(connection);
        return dps.getPrefixMapping();
    }

    @Override
    public QueryHandler queryHandler() {
        return new SimpleQueryHandler(this);
    }

    @Override
    public void clear() {
    }

    @Override
    public void remove(final Node s, final Node p, final Node o) {
    }
}