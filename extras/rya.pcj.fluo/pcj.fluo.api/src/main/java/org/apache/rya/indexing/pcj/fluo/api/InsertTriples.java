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
package org.apache.rya.indexing.pcj.fluo.api;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.StringTypeLayer;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

import com.google.common.base.Optional;

import io.fluo.api.client.FluoClient;
import io.fluo.api.types.Encoder;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypedTransaction;
import mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;
import mvm.rya.api.resolver.triple.impl.WholeRowTripleResolver;

/**
 * Insert a batch of Triples into. This will trigger observers that will update
 * the final results of any PCJs that are being managed by this application.
 */
public class InsertTriples {
    private static final Logger log = Logger.getLogger(InsertTriples.class);

    /**
     * Wraps Fluo {@link Transaction}s so that we can write String values to them.
     */
    private static final StringTypeLayer STRING_TYPED_LAYER = new StringTypeLayer();

    /**
     * Converts triples into the byte[] used as the row ID in Accumulo.
     */
    private static final WholeRowTripleResolver TRIPLE_RESOLVER = new WholeRowTripleResolver();

    private static final Encoder ENCODER = new StringEncoder();

    /**
     * Inserts a triple into Fluo.
     *
     * @param fluo - A connection to the Fluo table that will be updated. (not null)
     * @param triple - The triple to insert. (not null)
     * @param visibility - The visibility/permissions required to view this triple once stored. (not null)
     */
    public void insert(final FluoClient fluo, final RyaStatement triple, final Optional<String> visibility) {
        insert(fluo, Collections.singleton(triple), visibility);
    }

    /**
     * Insert a batch of triples into Fluo.
     *
     * @param fluo - A connection to the Fluo table that will be updated. (not null)
     * @param triples - The triples to insert. (not null)
     * @param visibility - The visibility/permissions required to view the triples once stored.
     * Note: The same visibility will be applied to each triple.(not null)
     */
    public void insert(final FluoClient fluo, final Collection<RyaStatement> triples, final Optional<String> visibility) {
        checkNotNull(fluo);
        checkNotNull(triples);
        checkNotNull(visibility);

        try(TypedTransaction tx = STRING_TYPED_LAYER.wrap(fluo.newTransaction())) {
            for(final RyaStatement triple : triples) {
                try {
                    tx.mutate().row(spoFormat(triple)).col(FluoQueryColumns.TRIPLES).set(ENCODER.encode(visibility.or("")));
                } catch (final TripleRowResolverException e) {
                    log.error("Could not convert a Triple into the SPO format: " + triple);
                }
            }

            tx.commit();
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