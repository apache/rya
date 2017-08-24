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
package org.apache.rya.indexing.pcj.fluo.app.export.rya;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.Set;

import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.CreatePCJ.QueryType;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;

import com.google.common.collect.Sets;

/**
 * Incrementally exports SPARQL query results to Accumulo PCJ tables as they are defined by Rya.
 */
public class RyaBindingSetExporter implements IncrementalBindingSetExporter {

    private final PrecomputedJoinStorage pcjStorage;

    /**
     * Constructs an instance of {@link RyaBindingSetExporter}.
     *
     * @param pcjStorage - The PCJ storage the new results will be exported to. (not null)
     */
    public RyaBindingSetExporter(final PrecomputedJoinStorage pcjStorage) {
        this.pcjStorage = checkNotNull(pcjStorage);
    }

    @Override
    public void export(final String queryId, final VisibilityBindingSet result) throws ResultExportException {
        requireNonNull(queryId);
        requireNonNull(result);

        try {
            pcjStorage.addResults(queryId, Collections.singleton(result));
        } catch (PCJStorageException e) {
            throw new ResultExportException("Unable to successfully export the result: " + result, e);
        }
    }

    @Override
    public void close() throws Exception {
        pcjStorage.close();
    }

    @Override
    public Set<QueryType> getQueryTypes() {
        return Sets.newHashSet(QueryType.PROJECTION);
    }

    @Override
    public ExportStrategy getExportStrategy() {
        return ExportStrategy.RYA;
    }
}