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

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.utils.VisibilitySimplifier;
import org.apache.rya.indexing.pcj.fluo.app.VisibilityBindingSetSerDe;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter.ResultExportException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory.IncrementalExporterFactoryException;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaBindingSetExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaBindingSetExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * Performs incremental result exporting to the configured destinations.
 */
public class QueryResultObserver extends AbstractObserver {
    private static final Logger log = Logger.getLogger(QueryResultObserver.class);

    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();

    /**
     * We expect to see the same expressions a lot, so we cache the simplified forms.
     */
    private final Map<String, String> simplifiedVisibilities = new HashMap<>();

    /**
     * Builders for each type of result exporter we support.
     */
    private static final ImmutableSet<IncrementalBindingSetExporterFactory> factories =
            ImmutableSet.<IncrementalBindingSetExporterFactory>builder()
                .add(new RyaBindingSetExporterFactory())
                .add(new KafkaBindingSetExporterFactory())
                .build();

    /**
     * The exporters that are configured.
     */
    private ImmutableSet<IncrementalBindingSetExporter> exporters = null;

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.QUERY_BINDING_SET, NotificationType.STRONG);
    }

    /**
     * Before running, determine which exporters are configured and set them up.
     */
    @Override
    public void init(final Context context) {
        final ImmutableSet.Builder<IncrementalBindingSetExporter> exportersBuilder = ImmutableSet.builder();

        for(final IncrementalBindingSetExporterFactory builder : factories) {
            try {
                log.debug("QueryResultObserver.init(): for each exportersBuilder=" + builder);

                final Optional<IncrementalBindingSetExporter> exporter = builder.build(context);
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
    public void process(final TransactionBase tx, final Bytes brow, final Column col) throws Exception {
        final String row = brow.toString();

        // Read the SPARQL query and it Binding Set from the row id.
        final String queryId = row.split(NODEID_BS_DELIM)[0];

        // Read the Child Binding Set that will be exported.
        final Bytes valueBytes = tx.get(brow, col);
        final VisibilityBindingSet result = BS_SERDE.deserialize(valueBytes);

        // Simplify the result's visibilities.
        final String visibility = result.getVisibility();
        if(!simplifiedVisibilities.containsKey(visibility)) {
            final String simplified = VisibilitySimplifier.simplify( visibility );
            simplifiedVisibilities.put(visibility, simplified);
        }
        result.setVisibility( simplifiedVisibilities.get(visibility) );

        // Export the result using each of the provided exporters.
        for(final IncrementalBindingSetExporter exporter : exporters) {
            try {
                exporter.export(tx, queryId, result);
            } catch (final ResultExportException e) {
                log.error("Could not export a binding set for query '" + queryId + "'. Binding Set: " + result, e);
            }
        }
    }

    @Override
    public void close() {
        if(exporters != null) {
            for(final IncrementalBindingSetExporter exporter : exporters) {
                try {
                    exporter.close();
                } catch(final Exception e) {
                    log.warn("Problem encountered while closing one of the exporters.", e);
                }
            }
        }
    }
}