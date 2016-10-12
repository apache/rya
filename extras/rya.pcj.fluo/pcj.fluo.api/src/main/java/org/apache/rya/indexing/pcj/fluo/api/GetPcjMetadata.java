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
import java.util.HashMap;
import java.util.Map;

import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.data.Bytes;

/**
 * Get {@link PcjMetadata} for queries that are managed by the Fluo app.
 */
public class GetPcjMetadata {

    private final ListQueryIds listQueryIds = new ListQueryIds();

    /**
     * Get the {@link PcjMetadata} of all queries that are being maintained by
     * the Fluo app.
     *
     * @param accumulo - The PCJ Storage that will be searched. (not null)
     * @param fluo - The Fluo instance that will be searched. (not null)
     * @return A map where the query ID is the key and its metadata is the value.
     * @throws NotInFluoException A query Id does not have a PCJ export able
     *   associated with it in the Fluo table.
     * @throws NotInAccumuloException A PCJ export table that was found either
     *   does not exist in Accumulo or it is not a PCJ table.
     */
    public Map<String, PcjMetadata> getMetadata(final PrecomputedJoinStorage pcjStorage, final FluoClient fluo) throws NotInFluoException, NotInAccumuloException {
        requireNonNull(pcjStorage);
        requireNonNull(fluo);

        final Map<String, PcjMetadata> metadata = new HashMap<>();

        final Collection<String> queryIds = listQueryIds.listQueryIds(fluo);
        for(final String queryId : queryIds) {
            metadata.put(queryId, getMetadata(pcjStorage, fluo, queryId));
        }

        return metadata;
    }

    /**
     * Get the {@link PcjMetadata} of a query that is being maintained by the
     * Fluo app.
     *
     * @param pcjStorage - The PCJ Storage that will be searched. (not null)
     * @param fluo - The Fluo instance that will be searched. (not null)
     * @param queryId - The Query Id whose metadata will be fetched. (not null)
     * @return The {@link PcjMetadata} of the query.
     * @throws NotInFluoException The query Id does not have a PCJ export able
     *   associated with it in the Fluo table.
     * @throws NotInAccumuloException The PCJ export table that was found either
     *   does not exist in Accumulo or it is not a PCJ table.
     */
    public PcjMetadata getMetadata(final PrecomputedJoinStorage pcjStorage, final FluoClient fluo, final String queryId) throws NotInFluoException, NotInAccumuloException {
        requireNonNull(pcjStorage);
        requireNonNull(fluo);
        requireNonNull(queryId);

        // Lookup the Rya PCJ ID associated with the query.
        String pcjId = null;
        try(Snapshot snap = fluo.newSnapshot() ) {
            pcjId = snap.gets(queryId, FluoQueryColumns.RYA_PCJ_ID);
            if(pcjId == null) {
                throw new NotInFluoException("Could not get the PcjMetadata for queryId '" + queryId +
                        "' because a Rya PCJ ID not stored in the Fluo table.");
            }
        }

        // Fetch the metadata from the storage.
        try {
            return pcjStorage.getPcjMetadata(pcjId);
        } catch (final PcjException e) {
            throw new NotInAccumuloException("Could not get the PcjMetadata for queryId '" + queryId +
                    "' because the metadata was missing from the Rya storage.", e);
        }
    }

    /**
     * Indicates PCJ Metadata could not be fetched for a query ID because the
     * Accumulo export table name was not stored in the Fluo table.
     */
    public static final class NotInFluoException extends Exception {
        private static final long serialVersionUID = 1L;

        public NotInFluoException(final String message) {
            super(message);
        }
    }

    /**
     * Indicates PCJ Metadata could not be fetched for a query ID because the
     * metadata was missing in Accumulo.
     */
    public static final class NotInAccumuloException extends Exception {
        private static final long serialVersionUID = 1L;

        public NotInAccumuloException(final String message, final Exception cause) {
            super(message, cause);
        }
    }
}
