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

import java.util.Iterator;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.shared.Lock;
import org.apache.jena.shared.LockMRSW;
import org.apache.jena.sparql.ARQException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.Context;
import org.apache.log4j.Logger;
import org.openrdf.model.Resource;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

/**
 * Jena Sesame Dataset Graph.
 */
public class JenaSesameDatasetGraph implements DatasetGraph {
    private static final Logger log = Logger.getLogger(JenaSesameDatasetGraph.class);

    private final Lock lock = new LockMRSW();
    private final RepositoryConnection connection;

    /**
     * Creates a new instance of {@link JenaSesameDatasetGraph}.
     * @param connection the {@link RepositoryConnection}. (not {@code null})
     */
    public JenaSesameDatasetGraph(final RepositoryConnection connection) {
        this.connection = checkNotNull(connection);
    }

    /**
     * @return the {@link RepositoryConnection}.
     */
    public RepositoryConnection getConnection() {
        return connection;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (final RepositoryException e) {
            throw new ARQException(e);
        }
    }

    @Override
    public boolean containsGraph(final Node graphNode) {
        return false;
    }

    @Override
    public Graph getDefaultGraph() {
        return new GraphRepository(connection);
    }

    @Override
    public Graph getGraph(final Node graphNode) {
        return new GraphRepository(connection, Convert.nodeToResource(connection.getValueFactory(), graphNode));
    }

    @Override
    public Lock getLock() {
        return lock;
    }

    @Override
    public Iterator<Node> listGraphNodes() {
        try {
            final RepositoryResult<Resource> named = connection.getContextIDs();
            // Mask bnodes.
            // Map to Jena terms
        } catch (final RepositoryException e) {
            log.error("Encountered error listing graph nodes.", e);
        }
        return null; // connection.getContextIDs();
    }

    @Override
    public long size() {
        return -1;
    }

    @Override
    public void add(final Quad quad) {
    }

    @Override
    public void add(final Node g, final Node s, final Node p, final Node o) {
    }

    @Override
    public void addGraph(final Node graphName, final Graph graph) {
    }

    @Override
    public boolean contains(final Quad quad) {
        return false;
    }

    @Override
    public boolean contains(final Node g, final Node s, final Node p, final Node o) {
        return false;
    }

    @Override
    public void delete(final Quad quad) {
    }

    @Override
    public void delete(final Node g, final Node s, final Node p, final Node o) {
    }

    @Override
    public void deleteAny(final Node g, final Node s, final Node p, final Node o) {
    }

    @Override
    public Iterator<Quad> find(final Quad quad) {
        return null;
    }

    @Override
    public Iterator<Quad> find(final Node g, final Node s, final Node p, final Node o) {
        return null;
    }

    @Override
    public Iterator<Quad> find() {
        return null;
    }

    @Override
    public Iterator<Quad> findNG(final Node g, final Node s, final Node p, final Node o) {
        return null;
    }

    @Override
    public Context getContext() {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public void removeGraph(final Node graphName) {
    }

    @Override
    public void setDefaultGraph(final Graph g) {
    }

    @Override
    public void clear() {
    }

    @Override
    public void begin(final ReadWrite readWrite) {
    }

    @Override
    public void commit() {
    }

    @Override
    public void abort() {
    }

    @Override
    public boolean isInTransaction() {
        return false;
    }

    @Override
    public void end() {
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }
}