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

import java.util.HashSet;
import java.util.Set;

import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.junit.Test;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.BindingImpl;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import org.apache.rya.api.domain.RyaStatement;

/**
 * Performs integration tests over the Fluo application geared towards Rya PCJ exporting.
 * <p>
 * These tests are being ignore so that they will not run as unit tests while building the application.
 */
public class RyaExportIT extends ITBase {

    @Test
    public void resultsExported() throws Exception {
        final String sparql =
                "SELECT ?customer ?worker ?city " +
                "{ " +
                  "FILTER(?customer = <http://Alice>) " +
                  "FILTER(?city = <http://London>) " +
                  "?customer <http://talksTo> ?worker. " +
                  "?worker <http://livesIn> ?city. " +
                  "?worker <http://worksAt> <http://Chipotle>. " +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                makeRyaStatement("http://Alice", "http://talksTo", "http://Bob"),
                makeRyaStatement("http://Bob", "http://livesIn", "http://London"),
                makeRyaStatement("http://Bob", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Alice", "http://talksTo", "http://Charlie"),
                makeRyaStatement("http://Charlie", "http://livesIn", "http://London"),
                makeRyaStatement("http://Charlie", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Alice", "http://talksTo", "http://David"),
                makeRyaStatement("http://David", "http://livesIn", "http://London"),
                makeRyaStatement("http://David", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Alice", "http://talksTo", "http://Eve"),
                makeRyaStatement("http://Eve", "http://livesIn", "http://Leeds"),
                makeRyaStatement("http://Eve", "http://worksAt", "http://Chipotle"),

                makeRyaStatement("http://Frank", "http://talksTo", "http://Alice"),
                makeRyaStatement("http://Frank", "http://livesIn", "http://London"),
                makeRyaStatement("http://Frank", "http://worksAt", "http://Chipotle"));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();
        expected.add(makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Bob")),
                new BindingImpl("city", new URIImpl("http://London"))));
        expected.add(makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://Charlie")),
                new BindingImpl("city", new URIImpl("http://London"))));
        expected.add(makeBindingSet(
                new BindingImpl("customer", new URIImpl("http://Alice")),
                new BindingImpl("worker", new URIImpl("http://David")),
                new BindingImpl("city", new URIImpl("http://London"))));

        // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(sparql);

        // Tell the Fluo app to maintain the PCJ.
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, ryaRepo);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Fetch the exported results from Accumulo once the observers finish working.
        fluo.waitForObservers();

        // Fetch expected results from the PCJ table that is in Accumulo.
        final Set<BindingSet> results = Sets.newHashSet( pcjStorage.listResults(pcjId) );

        // Verify the end results of the query match the expected results.
        assertEquals(expected, results);
    }
}
