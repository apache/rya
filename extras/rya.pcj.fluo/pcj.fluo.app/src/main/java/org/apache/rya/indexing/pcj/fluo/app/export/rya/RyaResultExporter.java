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

import java.util.Collections;

import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporter;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;

/**
 * Incrementally exports SPARQL query results to Accumulo PCJ tables as they are defined by Rya.
 */
public class RyaResultExporter implements IncrementalResultExporter {

    private final PrecomputedJoinStorage pcjStorage;

    /**
     * Constructs an instance of {@link RyaResultExporter}.
     *
     * @param pcjStorage - The PCJ storage the new results will be exported to. (not null)
     */
    public RyaResultExporter(final PrecomputedJoinStorage pcjStorage) {
        this.pcjStorage = checkNotNull(pcjStorage);
    }

    @Override
    public void export(
            final TransactionBase fluoTx,
            final String queryId,
            final VisibilityBindingSet result) throws ResultExportException {
        checkNotNull(fluoTx);
        checkNotNull(queryId);
        checkNotNull(result);

        final String pcjId = fluoTx.gets(queryId, FluoQueryColumns.RYA_PCJ_ID);

        try {
            pcjStorage.addResults(pcjId, Collections.singleton(result));
        } catch (final PCJStorageException e) {
            throw new ResultExportException("A result could not be exported to Rya.", e);
        }
    }
}