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

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.data.Bytes;
import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.api.resolver.triple.impl.WholeRowTripleResolver;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;

/**
 * Insert a batch of {@link RyaStatement}s into the fluo application. This will trigger observers that will update
 * the final results of any PCJs that are being managed by this application.
 */
public class InsertStatements {
    private static final Logger log = Logger.getLogger(InsertStatements.class);

    /**
     * Converts triples into the byte[] used as the row ID in Accumulo.
     */
    private static final WholeRowTripleResolver TRIPLE_RESOLVER = new WholeRowTripleResolver();

    /**
     * Inserts a {@link RyaStatement} into Fluo.
     *
     * @param fluo - A connection to the Fluo table that will be updated. (not null)
     * @param statement - The statement to insert. (not null)
     */
    public void insert(final FluoClient fluo, final RyaStatement statement) {
        requireNonNull(fluo);
        requireNonNull(statement);

        // Check the RyaStatement to see if it contains a visibility value.
        Optional<String> visibility = Optional.absent();
        byte[] visBytes = statement.getColumnVisibility();
        if(visBytes != null) {
            String visString = new String(visBytes, Charsets.UTF_8);
            visibility = Optional.of( visString );
        }

        // Perform the insert.
        insert(fluo, Collections.singleton(statement), visibility);
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
        requireNonNull(fluo);
        requireNonNull(triples);
        requireNonNull(visibility);

        try(Transaction tx = fluo.newTransaction()) {
            for(final RyaStatement triple : triples) {
                try {
                    tx.set(Bytes.of(spoFormat(triple)), FluoQueryColumns.TRIPLES, Bytes.of(visibility.or("")));
                } catch (final TripleRowResolverException e) {
                    log.error("Could not convert a Triple into the SPO format: " + triple);
                }
            }

            tx.commit();
        }
    }

    /**
     * Insert a batch of RyaStatements into Fluo.
     *
     * @param fluo - A connection to the Fluo table that will be updated. (not null)
     * @param triples - The triples to insert. (not null)
     */
    public void insert(final FluoClient fluo, final Collection<RyaStatement> triples) {
        requireNonNull(fluo);
        requireNonNull(triples);

        try(Transaction tx = fluo.newTransaction()) {
            for(final RyaStatement triple : triples) {
                Optional<byte[]> visibility = Optional.fromNullable(triple.getColumnVisibility());
                try {
                    tx.set(Bytes.of(spoFormat(triple)), FluoQueryColumns.TRIPLES, Bytes.of(visibility.or(new byte[0])));
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
        requireNonNull(triple);
        final Map<TABLE_LAYOUT, TripleRow> serialized = TRIPLE_RESOLVER.serialize(triple);
        final TripleRow spoRow = serialized.get(TABLE_LAYOUT.SPO);
        return spoRow.getRow();
    }
}
