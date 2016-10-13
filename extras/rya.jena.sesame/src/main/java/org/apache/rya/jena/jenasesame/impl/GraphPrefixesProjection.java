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

import java.util.Map;
import java.util.Map.Entry;

import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.apache.jena.sparql.core.DatasetPrefixStorage;

/**
 * View of a dataset's prefixes for a particular graph.
 */
public class GraphPrefixesProjection extends PrefixMappingImpl {
    private final String graphName;
    private final DatasetPrefixStorage prefixes;

    /**
     * Creates a new instance of {@link GraphPrefixesProjection}.
     * @param graphName the graph name.
     * @param prefixes the {@link DatasetPrefixStorage}. (not {@code null})
     */
    public GraphPrefixesProjection(final String graphName, final DatasetPrefixStorage prefixes) {
        this.graphName = graphName;
        this.prefixes = checkNotNull(prefixes);
    }

    @Override
    public String getNsURIPrefix(final String uri) {
        String prefix = super.getNsURIPrefix(uri);
        if (prefix !=  null) {
            return prefix;
        }
        // Do a reverse read.
        prefix = prefixes.readByURI(graphName, uri);
        if (prefix != null) {
            super.set(prefix, uri);
        }
        return prefix;
    }

    @Override
    public Map<String, String> getNsPrefixMap() {
        final Map<String, String> prefixMap =  prefixes.readPrefixMap(graphName);
        // Force into the cache
        for (final Entry<String, String> e : prefixMap.entrySet()) {
            super.set(e.getKey(), e.getValue());
        }
        return prefixMap;
    }

    @Override
    protected void set(final String prefix, final String uri) {
        // Delete old one if present and different.
        final String currentUri = get(prefix);
        if (currentUri != null) {
            if (currentUri.equals(uri)) {
                // Already there - no-op
                return;
            }
            // Remove from cache.
            prefixes.removeFromPrefixMap(graphName, prefix);
        }
        // Persist
        prefixes.insertPrefix(graphName, prefix, uri);
        // Add to caches.
        super.set(prefix, uri);
    }

    @Override
    protected String get(final String prefix) {
        String uri = super.get(prefix);
        if (uri != null) {
            return uri;
        }
        // In case it has been updated.
        uri = prefixes.readPrefix(graphName, prefix);
        if (uri != null) {
            super.set(prefix, uri);
        }
        return uri;
    }

    @Override
    public PrefixMapping removeNsPrefix(final String prefix) {
        final String uri = super.getNsPrefixURI(prefix);
        if (uri != null) {
            prefixes.removeFromPrefixMap(graphName, prefix);
        }
        super.removeNsPrefix(prefix);
        return this;
    }
}