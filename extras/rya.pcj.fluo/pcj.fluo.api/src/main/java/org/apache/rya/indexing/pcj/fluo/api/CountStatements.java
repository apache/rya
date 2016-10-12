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

import java.math.BigInteger;
import java.util.Iterator;

import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.ColumnScanner;

/**
 * Counts the number of RDF Statements that have been loaded into the Fluo app
 * that have not been processed yet.
 */
public class CountStatements {

    /**
     * Get the number of RDF Statements that have been loaded into the Fluo app
     * that have not been processed yet.
     *
     * @param fluo - The connection to Fluo that will be used to fetch the metadata. (not null)
     * @return The number of RDF Statements that have been loaded into the Fluo
     *   app that have not been processed yet.
     */
    public BigInteger countStatements(final FluoClient fluo) {
        checkNotNull(fluo);

        try(Snapshot sx = fluo.newSnapshot()) {
            // Limit the scan to the Triples binding set column.
            final Iterator<ColumnScanner> rows = sx.scanner().fetch(FluoQueryColumns.TRIPLES).byRow().build().iterator();
 
            BigInteger count = BigInteger.valueOf(0L);
            while(rows.hasNext()) {
                rows.next();
                count = count.add( BigInteger.ONE );
            }

            return count;
        }
    }
}