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