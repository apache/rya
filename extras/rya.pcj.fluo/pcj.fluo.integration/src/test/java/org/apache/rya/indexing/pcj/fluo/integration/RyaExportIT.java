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

import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.pcj.fluo.api.CreateFluoPcj;
import org.apache.rya.indexing.pcj.fluo.api.InsertTriples;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

/**
 * Performs integration tests over the Fluo application geared towards Rya PCJ exporting.
 */
public class RyaExportIT extends RyaExportITBase {

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
        final ValueFactory vf = new ValueFactoryImpl();
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://talksTo"), new RyaURI("http://Bob")),
                new RyaStatement(new RyaURI("http://Bob"), new RyaURI("http://livesIn"), new RyaURI("http://London")),
                new RyaStatement(new RyaURI("http://Bob"), new RyaURI("http://worksAt"), new RyaURI("http://Chipotle")),

                new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://talksTo"), new RyaURI("http://Charlie")),
                new RyaStatement(new RyaURI("http://Charlie"), new RyaURI("http://livesIn"), new RyaURI("http://London")),
                new RyaStatement(new RyaURI("http://Charlie"), new RyaURI("http://worksAt"), new RyaURI("http://Chipotle")),

                new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://talksTo"), new RyaURI("http://David")),
                new RyaStatement(new RyaURI("http://David"), new RyaURI("http://livesIn"), new RyaURI("http://London")),
                new RyaStatement(new RyaURI("http://David"), new RyaURI("http://worksAt"), new RyaURI("http://Chipotle")),

                new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://talksTo"), new RyaURI("http://Eve")),
                new RyaStatement(new RyaURI("http://Eve"), new RyaURI("http://livesIn"), new RyaURI("http://Leeds")),
                new RyaStatement(new RyaURI("http://Eve"), new RyaURI("http://worksAt"), new RyaURI("http://Chipotle")),

                new RyaStatement(new RyaURI("http://Frank"), new RyaURI("http://talksTo"), new RyaURI("http://Alice")),
                new RyaStatement(new RyaURI("http://Frank"), new RyaURI("http://livesIn"), new RyaURI("http://London")),
                new RyaStatement(new RyaURI("http://Frank"), new RyaURI("http://worksAt"), new RyaURI("http://Chipotle")));

        // The expected results of the SPARQL query once the PCJ has been computed.
        final Set<BindingSet> expected = new HashSet<>();

        MapBindingSet bs = new MapBindingSet();
        bs.addBinding("customer", vf.createURI("http://Alice"));
        bs.addBinding("worker", vf.createURI("http://Bob"));
        bs.addBinding("city", vf.createURI("http://London"));
        expected.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("customer", vf.createURI("http://Alice"));
        bs.addBinding("worker", vf.createURI("http://Charlie"));
        bs.addBinding("city", vf.createURI("http://London"));
        expected.add(bs);

        bs = new MapBindingSet();
        bs.addBinding("customer", vf.createURI("http://Alice"));
        bs.addBinding("worker", vf.createURI("http://David"));
        bs.addBinding("city", vf.createURI("http://London"));
        expected.add(bs);

        // Create the PCJ table.
        final Connector accumuloConn = super.getAccumuloConnector();
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName());
        final String pcjId = pcjStorage.createPcj(sparql);

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Tell the Fluo app to maintain the PCJ.
            new CreateFluoPcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, getRyaInstanceName());

            // Stream the data into Fluo.
            new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

            // Fetch the exported results from Accumulo once the observers finish working.
            super.getMiniFluo().waitForObservers();

            // Fetch expected results from the PCJ table that is in Accumulo.
            final Set<BindingSet> results = Sets.newHashSet( pcjStorage.listResults(pcjId) );

            // Verify the end results of the query match the expected results.
            assertEquals(expected, results);
        }
    }
}