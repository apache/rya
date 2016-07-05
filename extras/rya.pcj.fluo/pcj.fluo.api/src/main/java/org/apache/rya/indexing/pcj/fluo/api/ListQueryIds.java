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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.rya.indexing.pcj.fluo.app.StringTypeLayer;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

import io.fluo.api.client.FluoClient;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.types.TypedSnapshot;

/**
 * Finds all queries that are being managed by this instance of Fluo that
 * are also being exported to the provided instance of Accumulo.
 */
public class ListQueryIds {

    /**
     * Finds all queries that are being managed by this instance of Fluo that
     * are also being exported to the provided instance of Accumulo.
     *
     * @param fluo - The Fluo instance that will be searched. (not null)
     * @return An ascending alphabetically sorted list of the Query IDs being
     *   managed by the Fluo app and exported to an instance of Accumulo.
     */
    public List<String> listQueryIds(final FluoClient fluo) {
        checkNotNull(fluo);

        final List<String> queryIds = new ArrayList<>();

        try(TypedSnapshot snap = new StringTypeLayer().wrap( fluo.newSnapshot() )) {
            // Create an iterator that iterates over the QUERY_ID column.
            final ScannerConfiguration scanConfig = new ScannerConfiguration();
            scanConfig.fetchColumn(FluoQueryColumns.QUERY_ID.getFamily(), FluoQueryColumns.QUERY_ID.getQualifier());
            final RowIterator rows = snap.get(scanConfig);

            // Fetch the Query IDs that is stored in the Fluo table.
            while(rows.hasNext()) {
                final Entry<Bytes, ColumnIterator> entry = rows.next();
                final Bytes sparql = entry.getKey();
                final String queryId = snap.get(sparql, FluoQueryColumns.QUERY_ID).toString();
                queryIds.add(queryId);
            }
        }

        // Sort them alphabetically.
        Collections.sort(queryIds);
        return queryIds;
    }
}