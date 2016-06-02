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
package org.apache.rya.indexing.pcj.fluo.app.observers;

import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.NODEID_BS_DELIM;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporter.ResultExportException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporterFactory.IncrementalExporterFactoryException;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaResultExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetStringConverter;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.types.Encoder;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypedObserver;
import io.fluo.api.types.TypedTransactionBase;

/**
 * Performs incremental result exporting to the configured destinations.
 */
public class QueryResultObserver extends TypedObserver {
    private static final Logger log = Logger.getLogger(QueryResultObserver.class);

    private static final FluoQueryMetadataDAO QUERY_DAO = new FluoQueryMetadataDAO();
    private static final Encoder ENCODER = new StringEncoder();
    private static final VisibilityBindingSetStringConverter CONVERTER = new VisibilityBindingSetStringConverter();

    /**
     * Builders for each type of result exporter we support.
     */
    private static final ImmutableSet<IncrementalResultExporterFactory> factories =
            ImmutableSet.<IncrementalResultExporterFactory>builder()
                .add(new RyaResultExporterFactory())
                .build();

    /**
     * The exporters that are configured.
     */
    private ImmutableSet<IncrementalResultExporter> exporters = null;

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.QUERY_BINDING_SET, NotificationType.STRONG);
    }

    /**
     * Before running, determine which exporters are configured and set them up.
     */
    @Override
    public void init(final Context context) {
        final ImmutableSet.Builder<IncrementalResultExporter> exportersBuilder = ImmutableSet.builder();

        for(final IncrementalResultExporterFactory builder : factories) {
            try {
                final Optional<IncrementalResultExporter> exporter = builder.build(context);
                if(exporter.isPresent()) {
                    exportersBuilder.add(exporter.get());
                }
            } catch (final IncrementalExporterFactoryException e) {
                log.error("Could not initialize a result exporter.", e);
            }
        }

        exporters = exportersBuilder.build();
    }

    @Override
    public void process(final TypedTransactionBase tx, final Bytes row, final Column col) {
        // Read the SPARQL query and it Binding Set from the row id.
        final String[] queryAndBindingSet = ENCODER.decodeString(row).split(NODEID_BS_DELIM);
        final String queryId = queryAndBindingSet[0];
        final String bindingSetString = ENCODER.decodeString(tx.get(row, col));

        // Fetch the query's Variable Order from the Fluo table.
        final QueryMetadata queryMetadata = QUERY_DAO.readQueryMetadata(tx, queryId);
        final VariableOrder varOrder = queryMetadata.getVariableOrder();

        // Export the result using each of the provided exporters.
        final VisibilityBindingSet result = (VisibilityBindingSet) CONVERTER.convert(bindingSetString, varOrder);
        for(final IncrementalResultExporter exporter : exporters) {
            try {
                exporter.export(tx, queryId, result);
            } catch (final ResultExportException e) {
                log.error("Could not export a binding set for query '" + queryId + "'. Binding Set: " + bindingSetString);
            }
        }
    }
}