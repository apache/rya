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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jena.shared.JenaException;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.ARQNotImplemented;
import org.apache.jena.sparql.core.DatasetPrefixStorage;
import org.openrdf.model.Namespace;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;

/**
 * Jena Sesame Prefix Storage.
 */
public class JenaSesameDatasetPrefixStorage implements DatasetPrefixStorage {
    private final PrefixMapping prefixMapping = new GraphPrefixesProjection(null, this);
    private final RepositoryConnection connection;

    /**
     * Creates a new instance of {@link JenaSesameDatasetPrefixStorage}.
     * @param connection the {@link RepositoryConnection}. (not {@code null})
     */
    public JenaSesameDatasetPrefixStorage(final RepositoryConnection connection) {
        this.connection = checkNotNull(connection);
    }

    private Set<String> getURIs() {
        RepositoryResult<Namespace> repositoryNamespaces = null;
        try {
            repositoryNamespaces = connection.getNamespaces();
            final Set<String> namespaceUris = new HashSet<>();
            while (repositoryNamespaces.hasNext()) {
                final Namespace ns = repositoryNamespaces.next();
                namespaceUris.add(ns.getName());
            }
            return namespaceUris;
        } catch (final RepositoryException e) {
            throw new JenaException(e);
        } finally {
            if (repositoryNamespaces != null) {
                try {
                    repositoryNamespaces.close();
                } catch (final RepositoryException e) {
                    throw new JenaException(e);
                }
            }
        }
    }

    private Set<Namespace> getNamespaces() {
        RepositoryResult<Namespace> repositoryNamespaces = null;
        try {
            repositoryNamespaces = connection.getNamespaces();
            final Set<Namespace> namespaces = new HashSet<>();
            while (repositoryNamespaces.hasNext()) {
                final Namespace ns = repositoryNamespaces.next();
                namespaces.add(ns);
            }
            return namespaces;
        } catch (final RepositoryException e) {
            throw new JenaException(e);
        } finally {
            if (repositoryNamespaces != null) {
                try {
                    repositoryNamespaces.close();
                } catch (final RepositoryException e) {
                    throw new JenaException(e);
                }
            }
        }
    }

    @Override
    public PrefixMapping getPrefixMapping() {
        return prefixMapping;
    }

    @Override
    public PrefixMapping getPrefixMapping(final String graphName) {
        return prefixMapping;
    }

    @Override
    public Set<String> graphNames() {
        return getURIs();
    }

    @Override
    public void insertPrefix(final String graphName, final String prefix, final String uri) {
        throw new UnsupportedOperationException("Not modifiable");
    }

    @Override
    public void loadPrefixMapping(final String graphName, final PrefixMapping pmap) {
        throw new ARQNotImplemented("loadPrefixMapping");
    }

    @Override
    public String readByURI(final String graphName, final String uriStr) {
        // Crude.
        final Set<Namespace> namespaces = getNamespaces();
        for (final Namespace ns : namespaces) {
            if (ns.getName().equals(uriStr)) {
                return ns.getPrefix();
            }
        }
        return null;
    }

    @Override
    public String readPrefix(final String graphName, final String prefix) {
        try {
            return connection.getNamespace(prefix);
        } catch (final RepositoryException e) {
            throw new JenaException(e);
        }
    }

    @Override
    public Map<String, String> readPrefixMap(final String graphName) {
        final Set<Namespace> namespaces = getNamespaces();
        // prefix, uri.
        final Map<String, String> prefixMap = new HashMap<>();
        for (final Namespace ns : namespaces) {
            prefixMap.put(ns.getPrefix(), ns.getName());
        }
        return prefixMap;
    }

    @Override
    public void removeFromPrefixMap(final String graphName, final String prefix) {
        try {
            connection.removeNamespace(prefix);
        } catch (final RepositoryException e) {
            throw new JenaException(e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void sync() {
    }
}