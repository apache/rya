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

import org.apache.accumulo.core.client.Connector;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporter;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;

import io.fluo.api.data.Bytes;
import io.fluo.api.types.TypedTransactionBase;

/**
 * Incrementally exports SPARQL query results to Accumulo PCJ tables as they are defined by Rya.
 */
public class RyaResultExporter implements IncrementalResultExporter {

    private final Connector accumuloConn;
    private final PcjTables pcjTables;

    /**
     * Constructs an instance of {@link RyaResultExporter}.
     *
     * @param accumuloConn - A connection to the Accumulo instance that hosts Rya PCJ tables. (not null)
     * @param pcjTables - A utility used to interact with Rya's PCJ tables. (not null)
     */
    public RyaResultExporter(final Connector accumuloConn, final PcjTables pcjTables) {
        this.accumuloConn = checkNotNull(accumuloConn);
        this.pcjTables = checkNotNull(pcjTables);
    }

    @Override
    public void export(
            final TypedTransactionBase fluoTx,
            final String queryId,
            final VisibilityBindingSet result) throws ResultExportException {
        checkNotNull(fluoTx);
        checkNotNull(queryId);
        checkNotNull(result);

        // Get the name of the table the PCJ results will be written to.
        final String pcjTableName = fluoTx.get(Bytes.of(queryId), FluoQueryColumns.QUERY_RYA_EXPORT_TABLE_NAME).toString();

        // Write the result to the PCJ table.
        try {
            pcjTables.addResults(accumuloConn, pcjTableName, Collections.singleton(result));
        } catch (final PcjException e) {
            throw new ResultExportException("A result could not be exported to the PCJ table in Accumulo.", e);
        }
    }
}