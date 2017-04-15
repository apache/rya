package org.apache.rya.indexing.pcj.fluo.app.observers;
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
import static com.google.common.base.Preconditions.checkNotNull;

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
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.utils.VisibilitySimplifier;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.api.resolver.triple.impl.WholeRowTripleResolver;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory.IncrementalExporterFactoryException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaSubGraphExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaSubGraphExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KafkaRyaSubGraphExporterFactory;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.RyaSubGraphKafkaSerDe;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

/**
 * Monitors the Column {@link FluoQueryColumns#CONSTRUCT_STATEMENTS} for new
 * Construct Query {@link RyaStatement}s and exports the results using the
 * {@link IncrementalRyaSubGraphExporter}s that are registered with this
 * Observer.
 *
 */
public class ConstructQueryResultObserver extends AbstractObserver {

    private static final WholeRowTripleResolver TRIPLE_RESOLVER = new WholeRowTripleResolver();
    private static final Logger log = Logger.getLogger(ConstructQueryResultObserver.class);
    private static final RyaSubGraphKafkaSerDe serializer = new RyaSubGraphKafkaSerDe();

    /**
     * We expect to see the same expressions a lot, so we cache the simplified
     * forms.
     */
    private final Map<String, String> simplifiedVisibilities = new HashMap<>();

    /**
     * Builders for each type of result exporter we support.
     */
    private static final ImmutableSet<IncrementalRyaSubGraphExporterFactory> factories = ImmutableSet
            .<IncrementalRyaSubGraphExporterFactory> builder().add(new KafkaRyaSubGraphExporterFactory()).build();

    /**
     * The exporters that are configured.
     */
    private ImmutableSet<IncrementalRyaSubGraphExporter> exporters = null;

    /**
     * Before running, determine which exporters are configured and set them up.
     */
    @Override
    public void init(final Context context) {
        final ImmutableSet.Builder<IncrementalRyaSubGraphExporter> exportersBuilder = ImmutableSet.builder();

        for (final IncrementalRyaSubGraphExporterFactory builder : factories) {
            try {
                log.debug("ConstructQueryResultObserver.init(): for each exportersBuilder=" + builder);

                final Optional<IncrementalRyaSubGraphExporter> exporter = builder.build(context);
                if (exporter.isPresent()) {
                    exportersBuilder.add(exporter.get());
                }
            } catch (final IncrementalExporterFactoryException e) {
                log.error("Could not initialize a result exporter.", e);
            }
        }

        exporters = exportersBuilder.build();
    }

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.CONSTRUCT_STATEMENTS, NotificationType.STRONG);
    }

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
        Bytes bytes = tx.get(row, col);
        RyaSubGraph subgraph = serializer.fromBytes(bytes.toArray());
        Set<RyaStatement> statements = subgraph.getStatements();
        if (statements.size() > 0) {
            byte[] visibility = statements.iterator().next().getColumnVisibility();
            visibility = simplifyVisibilities(visibility);
            for(RyaStatement statement: statements) {
                statement.setColumnVisibility(visibility);
            }
            subgraph.setStatements(statements);

            for (IncrementalRyaSubGraphExporter exporter : exporters) {
                exporter.export(row.toString(), subgraph);
            }
        }
        //add generated triples back into Fluo for chaining queries together
        insertTriples(tx, subgraph.getStatements());
    }
    
    @Override
    public void close() {
        if(exporters != null) {
            for(final IncrementalRyaSubGraphExporter exporter : exporters) {
                try {
                    exporter.close();
                } catch(final Exception e) {
                    log.warn("Problem encountered while closing one of the exporters.", e);
                }
            }
        }
    }

    private byte[] simplifyVisibilities(byte[] visibilityBytes) throws UnsupportedEncodingException {
        // Simplify the result's visibilities and cache new simplified
        // visibilities
        String visibility = new String(visibilityBytes, "UTF-8");
        if (!simplifiedVisibilities.containsKey(visibility)) {
            String simplified = VisibilitySimplifier.simplify(visibility);
            simplifiedVisibilities.put(visibility, simplified);
        }
        return simplifiedVisibilities.get(visibility).getBytes("UTF-8");
    }
    
    private void insertTriples(TransactionBase tx, final Collection<RyaStatement> triples) {

        for (final RyaStatement triple : triples) {
            Optional<byte[]> visibility = Optional.fromNullable(triple.getColumnVisibility());
            try {
                tx.set(Bytes.of(spoFormat(triple)), FluoQueryColumns.TRIPLES, Bytes.of(visibility.or(new byte[0])));
            } catch (final TripleRowResolverException e) {
                log.error("Could not convert a Triple into the SPO format: " + triple);
            }
        }
    }
    

    /**
     * Converts a triple into a byte[] holding the Rya SPO representation of it.
     *
     * @param triple - The triple to convert. (not null)
     * @return The Rya SPO representation of the triple.
     * @throws TripleRowResolverException The triple could not be converted.
     */
    public static byte[] spoFormat(final RyaStatement triple) throws TripleRowResolverException {
        checkNotNull(triple);
        final Map<TABLE_LAYOUT, TripleRow> serialized = TRIPLE_RESOLVER.serialize(triple);
        final TripleRow spoRow = serialized.get(TABLE_LAYOUT.SPO);
        return spoRow.getRow();
    }

}
