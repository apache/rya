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
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.rya.indexing.pcj.fluo.app.StringTypeLayer;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;

import io.fluo.api.client.FluoClient;
import io.fluo.api.data.Bytes;
import io.fluo.api.types.TypedSnapshot;

/**
 * Get {@link PcjMetadata} for queries that are managed by the Fluo app.
 */
public class GetPcjMetadata {

    private final ListQueryIds listQueryIds = new ListQueryIds();

    /**
     * Get the {@link PcjMetadata} of all queries that are being maintained by
     * the Fluo app.
     *
     * @param accumulo - The Accumulo instance that will be searched. (not null)
     * @param fluo - The Fluo instance that will be searched. (not null)
     * @return A map where the query ID is the key and its metadata is the value.
     * @throws NotInFluoException A query Id does not have a PCJ export able
     *   associated with it in the Fluo table.
     * @throws NotInAccumuloException A PCJ export table that was found either
     *   does not exist in Accumulo or it is not a PCJ table.
     */
    public Map<String, PcjMetadata> getMetadata(final Connector accumulo, final FluoClient fluo) throws NotInFluoException, NotInAccumuloException {
        checkNotNull(accumulo);
        checkNotNull(fluo);

        final Map<String, PcjMetadata> metadata = new HashMap<>();

        final Collection<String> queryIds = listQueryIds.listQueryIds(fluo);
        for(final String queryId : queryIds) {
            metadata.put(queryId, getMetadata(accumulo, fluo, queryId));
        }

        return metadata;
    }

    /**
     * Get the {@link PcjMetadata} of a query that is being maintained by the
     * Fluo app.
     *
     * @param accumulo - The Accumulo instance that will be searched. (not null)
     * @param fluo - The Fluo instance that will be searched. (not null)
     * @param queryId - The Query Id whose metadata will be fetched. (not null)
     * @return The {@link PcjMetadata} of the query.
     * @throws NotInFluoException The query Id does not have a PCJ export able
     *   associated with it in the Fluo table.
     * @throws NotInAccumuloException The PCJ export table that was found either
     *   does not exist in Accumulo or it is not a PCJ table.
     */
    public PcjMetadata getMetadata(final Connector accumulo, final FluoClient fluo, final String queryId) throws NotInFluoException, NotInAccumuloException {
        checkNotNull(accumulo);
        checkNotNull(fluo);
        checkNotNull(queryId);

        // Lookup the Accumulo export table name in the Fluo table.
        String pcjTableName = null;
        try(TypedSnapshot snap = new StringTypeLayer().wrap( fluo.newSnapshot() ) ) {
            final Bytes pcjTableNameBytes = snap.get(Bytes.of(queryId), FluoQueryColumns.QUERY_RYA_EXPORT_TABLE_NAME);
            if(pcjTableNameBytes == null) {
                throw new NotInFluoException("Could not get the PcjMetadata for queryId '" + queryId +
                        "' because a PCJ export table name was not stored in the Fluo table.");
            }
            pcjTableName = pcjTableNameBytes.toString();
        }

        // Fetch the metadata from the Accumulo table.
        try {
            return new PcjTables().getPcjMetadata(accumulo, pcjTableName);
        } catch (final PcjException e) {
            throw new NotInAccumuloException("Could not get the PcjMetadata for queryId '" + queryId +
                    "' because the metadata was missing from the Accumulo table.", e);
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