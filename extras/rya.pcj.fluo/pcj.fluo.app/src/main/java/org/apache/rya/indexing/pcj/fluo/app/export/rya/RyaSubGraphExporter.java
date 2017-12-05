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
import static org.apache.rya.indexing.pcj.fluo.app.util.TriplePrefixUtils.addTriplePrefixAndConvertToBytes;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.CreatePCJ.QueryType;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.api.resolver.triple.impl.WholeRowTripleResolver;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter.ResultExportException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaSubGraphExporter;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * This exporter is used to import {@link RyaSubGraph}s back into Fluo. By ingesting
 * RyaSubGraphs back into Fluo, queries can be chained together.
 *
 */
public class RyaSubGraphExporter implements IncrementalRyaSubGraphExporter {

    private static final Logger log = Logger.getLogger(RyaSubGraphExporter.class);
    private static final WholeRowTripleResolver TRIPLE_RESOLVER = new WholeRowTripleResolver();
    private final FluoClient fluo;

    public RyaSubGraphExporter(FluoClient fluo) {
        this.fluo = Preconditions.checkNotNull(fluo);
    }

    @Override
    public Set<QueryType> getQueryTypes() {
        return Sets.newHashSet(QueryType.CONSTRUCT);
    }

    @Override
    public ExportStrategy getExportStrategy() {
        return ExportStrategy.RYA;
    }

    @Override
    public void close() throws Exception {
        fluo.close();
    }

    @Override
    public void export(String constructID, RyaSubGraph subgraph) throws ResultExportException {
        insertTriples(fluo.newTransaction(), subgraph.getStatements());
    }

    private void insertTriples(TransactionBase tx, final Collection<RyaStatement> triples) {
        for (final RyaStatement triple : triples) {
            Optional<byte[]> visibility = Optional.fromNullable(triple.getColumnVisibility());
            try {
                tx.set(spoFormat(triple), FluoQueryColumns.TRIPLES, Bytes.of(visibility.or(new byte[0])));
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
    private static Bytes spoFormat(final RyaStatement triple) throws TripleRowResolverException {
        checkNotNull(triple);
        final Map<TABLE_LAYOUT, TripleRow> serialized = TRIPLE_RESOLVER.serialize(triple);
        final TripleRow spoRow = serialized.get(TABLE_LAYOUT.SPO);
        return addTriplePrefixAndConvertToBytes(spoRow.getRow());
    }
}
