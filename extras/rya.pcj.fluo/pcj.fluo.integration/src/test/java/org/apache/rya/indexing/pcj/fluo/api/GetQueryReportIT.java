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

import static org.junit.Assert.assertEquals;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.pcj.fluo.api.GetQueryReport.QueryReport;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

/**
 * Integration tests the methods of {@link GetQueryReportl}.
 */
public class GetQueryReportIT extends RyaExportITBase {

    @Test
    public void getReport() throws Exception {
        final String sparql =
                "SELECT ?worker ?company ?city" +
                "{ " +
                  "FILTER(?worker = <http://Alice>) " +
                  "?worker <http://worksAt> ?company . " +
                  "?worker <http://livesIn> ?city ." +
                "}";

        // Triples that will be streamed into Fluo after the PCJ has been created.
        final Set<RyaStatement> streamedTriples = Sets.newHashSet(
                new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://worksAt"), new RyaURI("http://Taco Shop")),
                new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://worksAt"), new RyaURI("http://Burger Join")),
                new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://worksAt"), new RyaURI("http://Pastery Shop")),
                new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://worksAt"), new RyaURI("http://Burrito Place")),
                new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://livesIn"), new RyaURI("http://Lost County")),
                new RyaStatement(new RyaURI("http://Alice"), new RyaURI("http://livesIn"), new RyaURI("http://Big City")),
                new RyaStatement(new RyaURI("http://Bob"), new RyaURI("http://worksAt"), new RyaURI("http://Burrito Place")),
                new RyaStatement(new RyaURI("http://Bob"), new RyaURI("http://livesIn"), new RyaURI("http://Big City")),
                new RyaStatement(new RyaURI("http://Charlie"), new RyaURI("http://worksAt"), new RyaURI("http://Burrito Place")),
                new RyaStatement(new RyaURI("http://Charlie"), new RyaURI("http://livesIn"), new RyaURI("http://Big City")),
                new RyaStatement(new RyaURI("http://David"), new RyaURI("http://worksAt"), new RyaURI("http://Burrito Place")),
                new RyaStatement(new RyaURI("http://David"), new RyaURI("http://livesIn"), new RyaURI("http://Lost County")),
                new RyaStatement(new RyaURI("http://Eve"), new RyaURI("http://worksAt"), new RyaURI("http://Burrito Place")),
                new RyaStatement(new RyaURI("http://Eve"), new RyaURI("http://livesIn"), new RyaURI("http://Big City")),
                new RyaStatement(new RyaURI("http://Frank"), new RyaURI("http://worksAt"), new RyaURI("http://Burrito Place")),
                new RyaStatement(new RyaURI("http://Frank"), new RyaURI("http://livesIn"), new RyaURI("http://Lost County")));

        // Create the PCJ table.

        final Connector accumuloConn = super.getAccumuloConnector();
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, getRyaInstanceName());
        final String pcjId = pcjStorage.createPcj(sparql);

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Tell the Fluo app to maintain the PCJ.
            new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, getRyaInstanceName());

            // Stream the data into Fluo.
            new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

            // Wait for the results to finish processing.
            super.getMiniFluo().waitForObservers();

            // Fetch the report.
            final Map<String, PcjMetadata> metadata = new GetPcjMetadata().getMetadata(pcjStorage, fluoClient);
            final Set<String> queryIds = metadata.keySet();
            assertEquals(1, queryIds.size());
            final String queryId = queryIds.iterator().next();

            final QueryReport report = new GetQueryReport().getReport(fluoClient, queryId);

            // Build the expected counts map.
            final Map<String, BigInteger> expectedCounts = new HashMap<>();

            final FluoQuery fluoQuery = report.getFluoQuery();

            final String queryNodeId = fluoQuery.getQueryMetadata().get().getNodeId();
            expectedCounts.put(queryNodeId, BigInteger.valueOf(8));

            final String filterNodeId = fluoQuery.getFilterMetadata().iterator().next().getNodeId();
            expectedCounts.put(filterNodeId, BigInteger.valueOf(8));

            final String joinNodeId = fluoQuery.getJoinMetadata().iterator().next().getNodeId();
            expectedCounts.put(joinNodeId, BigInteger.valueOf(13));

            final Iterator<StatementPatternMetadata> patterns = fluoQuery.getStatementPatternMetadata().iterator();
            final StatementPatternMetadata sp1 = patterns.next();
            final StatementPatternMetadata sp2 = patterns.next();
            if(sp1.getStatementPattern().contains("http://worksAt")) {
                expectedCounts.put(sp1.getNodeId(), BigInteger.valueOf(9));
                expectedCounts.put(sp2.getNodeId(), BigInteger.valueOf(7));
            } else {
                expectedCounts.put(sp2.getNodeId(), BigInteger.valueOf(9));
                expectedCounts.put(sp1.getNodeId(), BigInteger.valueOf(7));
            }

            assertEquals(expectedCounts, report.getCounts());
        }
    }
}