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

import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.QUERY_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns.QUERY_BINDING_SET;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.export.ExporterManager;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporterFactory.IncrementalExporterFactoryException;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaBindingSetExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaRyaSubGraphExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.PeriodicBindingSetExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaBindingSetExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.export.rya.RyaSubGraphExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataCache;
import org.apache.rya.indexing.pcj.fluo.app.query.MetadataCacheSupplier;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * Performs incremental result exporting to the configured destinations.
 */
public class QueryResultObserver extends AbstractObserver {

    private static final Logger log = LoggerFactory.getLogger(QueryResultObserver.class);
    private final FluoQueryMetadataCache queryDao = MetadataCacheSupplier.getOrCreateCache();
    /**
     * Builders for each type of {@link IncrementalBindingSetExporter} we support.
     */
    private static final ImmutableSet<IncrementalResultExporterFactory> FACTORIES =
            ImmutableSet.<IncrementalResultExporterFactory>builder()
                .add(new RyaBindingSetExporterFactory())
                .add(new KafkaBindingSetExporterFactory())
                .add(new KafkaRyaSubGraphExporterFactory())
                .add(new RyaSubGraphExporterFactory())
                .add(new PeriodicBindingSetExporterFactory())
                .build();

    private ExporterManager exporterManager;

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(QUERY_BINDING_SET, NotificationType.STRONG);
    }

    /**
     * Before running, determine which exporters are configured and set them up.
     */
    @Override
    public void init(final Context context) {

        final ExporterManager.Builder managerBuilder = ExporterManager.builder();

        for(final IncrementalResultExporterFactory builder : FACTORIES) {
            try {
                log.debug("Attempting to build exporter from factory: {}", builder);
                final Optional<IncrementalResultExporter> exporter = builder.build(context);
                if(exporter.isPresent()) {
                    log.info("Adding exporter: {}", exporter.get());
                    managerBuilder.addIncrementalResultExporter(exporter.get());
                }
            } catch (final IncrementalExporterFactoryException e) {
                log.error("Could not initialize a result exporter.", e);
            }
        }

        exporterManager = managerBuilder.build();
    }


    @Override
    public void process(final TransactionBase tx, final Bytes brow, final Column col) throws Exception {

        // Read the queryId from the row and get the QueryMetadata.
        final String queryId = BindingSetRow.makeFromShardedRow(Bytes.of(QUERY_PREFIX), brow).getNodeId();
        final QueryMetadata metadata = queryDao.readQueryMetadata(tx, queryId);

        // Read the Child Binding Set that will be exported.
        final Bytes valueBytes = tx.get(brow, col);

        exporterManager.export(metadata.getQueryType(), metadata.getExportStrategies(), queryId, valueBytes);
    }

    @Override
    public void close() {
        try {
            exporterManager.close();
        } catch (final Exception e) {
           log.warn("Encountered problems closing the ExporterManager.");
        }
    }
}