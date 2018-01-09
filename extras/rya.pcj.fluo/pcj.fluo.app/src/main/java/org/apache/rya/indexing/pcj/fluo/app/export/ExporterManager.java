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
package org.apache.rya.indexing.pcj.fluo.app.export;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.fluo.api.data.Bytes;
import org.apache.rya.accumulo.utils.VisibilitySimplifier;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.CreatePCJ.QueryType;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter.ResultExportException;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.RyaSubGraphKafkaSerDe;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoQueryUtils;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetSerDe;

import com.google.common.base.Preconditions;

/**
 * This class manages all of the {@link IncrementalResultExporter}s for the Rya Fluo Application.
 * It maps the {@link FluoQuery}'s {@link QueryType} and Set of {@link ExportStrategy} objects
 * to the correct IncrementalResultExporter. 
 *
 */
public class ExporterManager implements AutoCloseable {

    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();
    private static final RyaSubGraphKafkaSerDe SG_SERDE = new RyaSubGraphKafkaSerDe();
    private final Map<String, String> simplifiedVisibilities = new HashMap<>();
    
    private final Map<QueryType, Map<ExportStrategy, IncrementalResultExporter>> exporters;
    
    private ExporterManager(final Map<QueryType, Map<ExportStrategy, IncrementalResultExporter>> exporters) {
        this.exporters = Preconditions.checkNotNull(exporters);
    }
    
    /**
     * @return {@link Builder} for constructing an instance of an ExporterManager.
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Maps the data to the correct {@link IncrementalResultExporter} using the provided
     * QueryType and ExportStrategies to be exported.
     * @param type - QueryType that produced the result
     * @param strategies - ExportStrategies used to export the result
     * @param queryId - Fluo Query Id for the query that produced the result
     * @param data - Serialized result to be exported
     * @throws ResultExportException 
     */
    public void export(final QueryType type, final Set<ExportStrategy> strategies, final String queryId, final Bytes data) throws ResultExportException {
        
        final String pcjId = FluoQueryUtils.convertFluoQueryIdToPcjId(queryId);
        
        if(type == QueryType.CONSTRUCT) {
            exportSubGraph(exporters.get(type), strategies, pcjId, data);
        } else {
            exportBindingSet(exporters.get(type), strategies, pcjId, data);
        }
        
    }
    
    /**
     * Exports BindingSet using the exporters for a given {@link QueryType}.
     * @param exporters - exporters corresponding to a given queryType
     * @param strategies - export strategies used to export results (possibly a subset of those in the exporters map)
     * @param pcjId - id of the query whose results are being exported
     * @param data - serialized BindingSet result
     * @throws ResultExportException
     */
    private void exportBindingSet(final Map<ExportStrategy, IncrementalResultExporter> exporters, final Set<ExportStrategy> strategies, final String pcjId, final Bytes data) throws ResultExportException {
        VisibilityBindingSet bs;
        try {
            bs = BS_SERDE.deserialize(data);
            simplifyVisibilities(bs);
        } catch (final Exception e) {
            throw new ResultExportException("Unable to deserialize the given BindingSet.", e);
        }
            
        try{
            for(final ExportStrategy strategy: strategies) {
                final IncrementalBindingSetExporter exporter = (IncrementalBindingSetExporter) exporters.get(strategy);
                exporter.export(pcjId, bs);
            }
        } catch (final Exception e) {
            throw new ResultExportException("Unable to export the given BindingSet " + bs + " with the given set of ExportStrategies " + strategies, e);
        }
    }
    
    /**
     * Exports RyaSubGraph using the exporters for a given {@link QueryType}.
     * @param exporters - exporters corresponding to a given queryType
     * @param strategies - export strategies used to export results (possibly a subset of those in the exporters map)
     * @param pcjId - id of the query whose results are being exported
     * @param data - serialized RyaSubGraph result
     * @throws ResultExportException
     */
    private void exportSubGraph(final Map<ExportStrategy, IncrementalResultExporter> exporters, final Set<ExportStrategy> strategies, final String pcjId, final Bytes data) throws ResultExportException {
        final RyaSubGraph subGraph = SG_SERDE.fromBytes(data.toArray());
        
        try {
            simplifyVisibilities(subGraph);
        } catch (final UnsupportedEncodingException e) {
            throw new ResultExportException("Undable to deserialize provided RyaSubgraph", e);
        }
        
        try {
            for (final ExportStrategy strategy : strategies) {
                final IncrementalRyaSubGraphExporter exporter = (IncrementalRyaSubGraphExporter) exporters.get(strategy);
                exporter.export(pcjId, subGraph);
            }
        } catch (final Exception e) {
            throw new ResultExportException(
                    "Unable to export the given subgraph " + subGraph + " using all of the ExportStrategies " + strategies);
        }
    }
    
    private void simplifyVisibilities(final VisibilityBindingSet result) {
        // Simplify the result's visibilities.
        final String visibility = result.getVisibility();
        if(!simplifiedVisibilities.containsKey(visibility)) {
            final String simplified = VisibilitySimplifier.simplify( visibility );
            simplifiedVisibilities.put(visibility, simplified);
        }
        result.setVisibility( simplifiedVisibilities.get(visibility) );
    }
    
    private void simplifyVisibilities(final RyaSubGraph subgraph) throws UnsupportedEncodingException {
        final Set<RyaStatement> statements = subgraph.getStatements();
        if (statements.size() > 0) {
            final byte[] visibilityBytes = statements.iterator().next().getColumnVisibility();
            // Simplify the result's visibilities and cache new simplified
            // visibilities
            final String visibility = new String(visibilityBytes, "UTF-8");
            if (!simplifiedVisibilities.containsKey(visibility)) {
                final String simplified = VisibilitySimplifier.simplify(visibility);
                simplifiedVisibilities.put(visibility, simplified);
            }

            for (final RyaStatement statement : statements) {
                statement.setColumnVisibility(simplifiedVisibilities.get(visibility).getBytes("UTF-8"));
            }
            
            subgraph.setStatements(statements);
        }
    }
    
    public static class Builder {
        
        private final Map<QueryType, Map<ExportStrategy, IncrementalResultExporter>> exporters = new HashMap<>();
        
        /**
         * Add an {@link IncrementalResultExporter} to be used by this ExporterManager for exporting results
         * @param exporter - IncrementalResultExporter for exporting query results
         * @return - Builder for chaining method calls
         */
        public Builder addIncrementalResultExporter(final IncrementalResultExporter exporter) {
            
            final Set<QueryType> types = exporter.getQueryTypes();
            final ExportStrategy strategy = exporter.getExportStrategy();
            
            for (final QueryType type : types) {
                if (!exporters.containsKey(type)) {
                    final Map<ExportStrategy, IncrementalResultExporter> exportMap = new HashMap<>();
                    exportMap.put(strategy, exporter);
                    exporters.put(type, exportMap);
                } else {
                    final Map<ExportStrategy, IncrementalResultExporter> exportMap = exporters.get(type);
                    if (!exportMap.containsKey(strategy)) {
                        exportMap.put(strategy, exporter);
                    }
                }
            }
            
            return this;
        }
        
        /**
         * @return - ExporterManager for managing IncrementalResultExporters and exporting results
         */
        public ExporterManager build() {
            return new ExporterManager(exporters);
        }
        
    }

    @Override
    public void close() throws Exception {
        
        final Collection<Map<ExportStrategy, IncrementalResultExporter>> values = exporters.values();
        
        for(final Map<ExportStrategy, IncrementalResultExporter> map: values) {
            for(final IncrementalResultExporter exporter: map.values()) {
                exporter.close();
            }
        }
    }
}
