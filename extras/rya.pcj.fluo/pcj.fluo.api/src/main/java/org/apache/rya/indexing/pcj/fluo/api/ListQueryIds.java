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

import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.data.RowColumnValue;

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

        try(Snapshot snap = fluo.newSnapshot() ) {
            // Create an iterator that iterates over the QUERY_ID column.
            final CellScanner cellScanner = snap.scanner().fetch( FluoQueryColumns.QUERY_ID).build();

            for (RowColumnValue rcv : cellScanner) {
            	queryIds.add(rcv.getsValue());
            	//TODO this was doing a snap.get that seemed unnecessary 
			}
        }

        // Sort them alphabetically.
        Collections.sort(queryIds);
        return queryIds;
    }
}