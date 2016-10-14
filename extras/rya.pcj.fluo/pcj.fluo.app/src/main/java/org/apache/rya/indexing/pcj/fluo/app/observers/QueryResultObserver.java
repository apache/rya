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

import java.util.HashMap;
import java.util.Map;

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
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.rya.accumulo.utils.VisibilitySimplifier;

/**
 * Performs incremental result exporting to the configured destinations.
 */
public class QueryResultObserver extends AbstractObserver {
    private static final Logger log = Logger.getLogger(QueryResultObserver.class);

    private static final FluoQueryMetadataDAO QUERY_DAO = new FluoQueryMetadataDAO();
    private static final VisibilityBindingSetStringConverter CONVERTER = new VisibilityBindingSetStringConverter();

    /**
     * Simplifies Visibility expressions prior to exporting PCJ results.
     */
    private static final VisibilitySimplifier SIMPLIFIER = new VisibilitySimplifier();

    /**
     * We expect to see the same expressions a lot, so we cache the simplified forms.
     */
    private final Map<String, String> simplifiedVisibilities = new HashMap<>();

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
    public void process(final TransactionBase tx, final Bytes brow, final Column col) {
        final String row = brow.toString();
        
        // Read the SPARQL query and it Binding Set from the row id.
        final String[] queryAndBindingSet = row.split(NODEID_BS_DELIM);
        final String queryId = queryAndBindingSet[0];
        final String bindingSetString = tx.gets(row, col);

        // Fetch the query's Variable Order from the Fluo table.
        final QueryMetadata queryMetadata = QUERY_DAO.readQueryMetadata(tx, queryId);
        final VariableOrder varOrder = queryMetadata.getVariableOrder();

        // Create the result that will be exported.
        final VisibilityBindingSet result = CONVERTER.convert(bindingSetString, varOrder);

        // Simplify the result's visibilities.
        final String visibility = result.getVisibility();
        if(!simplifiedVisibilities.containsKey(visibility)) {
            final String simplified = SIMPLIFIER.simplify( visibility );
            simplifiedVisibilities.put(visibility, simplified);
        }

        result.setVisibility( simplifiedVisibilities.get(visibility) );

        // Export the result using each of the provided exporters.
        for(final IncrementalResultExporter exporter : exporters) {
            try {
                exporter.export(tx, queryId, result);
            } catch (final ResultExportException e) {
                log.error("Could not export a binding set for query '" + queryId + "'. Binding Set: " + bindingSetString);
            }
        }
    }
}
