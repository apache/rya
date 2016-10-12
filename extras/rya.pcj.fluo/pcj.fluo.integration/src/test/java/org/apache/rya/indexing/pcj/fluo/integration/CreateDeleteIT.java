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
package org.apache.rya.indexing.pcj.fluo.integration;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Span;
import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.api.DeletePcj;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.BindingImpl;

import com.google.common.collect.Sets;

public class CreateDeleteIT extends ITBase {

    /**
     * Ensure historic matches are included in the result.
     */
    @Test
    public void historicResults() throws Exception {
        // A query that finds people who talk to Eve and work at Chipotle.
        final String sparql = "SELECT ?x " + "WHERE { " + "?x <http://talksTo> <http://Eve>. "
                + "?x <http://worksAt> <http://Chipotle>." + "}";

        // Triples that are loaded into Rya before the PCJ is created.
        final Set<Statement> historicTriples = Sets.newHashSet(
                makeStatement("http://Alice", "http://talksTo", "http://Eve"),
                makeStatement("http://Bob", "http://talksTo", "http://Eve"),
                makeStatement("http://Charlie", "http://talksTo", "http://Eve"),

                makeStatement("http://Eve", "http://helps", "http://Kevin"),

                makeStatement("http://Bob", "http://worksAt", "http://Chipotle"),
                makeStatement("http://Charlie", "http://worksAt", "http://Chipotle"),
                makeStatement("http://Eve", "http://worksAt", "http://Chipotle"),
                makeStatement("http://David", "http://worksAt", "http://Chipotle"));

        // The expected results of the SPARQL query once the PCJ has been
        // computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(new BindingImpl("x", new URIImpl("http://Bob"))));
        expected.add(makeBindingSet(new BindingImpl("x", new URIImpl("http://Charlie"))));

        // Load the historic data into Rya.
        for (final Statement triple : historicTriples) {
            ryaConn.add(triple);
        }

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(sparql);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, ryaRepo);

        // Verify the end results of the query match the expected results.
        fluo.waitForObservers();

        final Set<BindingSet> results = getQueryBindingSetValues(fluoClient, sparql);
        assertEquals(expected, results);

        List<Bytes> rows = getFluoTableEntries(fluoClient);
        assertEquals(17, rows.size());

        // Delete the PCJ from the Fluo application.
        new DeletePcj(1).deletePcj(fluoClient, pcjId);

        // Ensure all data related to the query has been removed.
        List<Bytes> empty_rows = getFluoTableEntries(fluoClient);
        assertEquals(0, empty_rows.size());
    }

    private List<Bytes> getFluoTableEntries(FluoClient fluoClient) {
        try (Snapshot snapshot = fluoClient.newSnapshot()) {
            List<Bytes> rows = new ArrayList<>();
            RowScanner rscanner = snapshot.scanner().over(Span.prefix("")).byRow().build();

            for(ColumnScanner cscanner: rscanner) {
            	rows.add(cscanner.getRow());
            }
            
            return rows;
        }
    }
}