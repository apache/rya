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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.GetQueryReport.QueryReport;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import mvm.rya.api.domain.RyaStatement;

/**
 * Integration tests the methods of {@link GetQueryReportl}.
 */
public class GetQueryReportIT extends ITBase {

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
                makeRyaStatement("http://Alice", "http://worksAt", "http://Taco Shop"),
                makeRyaStatement("http://Alice", "http://worksAt", "http://Burger Join"),
                makeRyaStatement("http://Alice", "http://worksAt", "http://Pastery Shop"),
                makeRyaStatement("http://Alice", "http://worksAt", "http://Burrito Place"),
                makeRyaStatement("http://Alice", "http://livesIn", "http://Lost County"),
                makeRyaStatement("http://Alice", "http://livesIn", "http://Big City"),
                makeRyaStatement("http://Bob", "http://worksAt", "http://Burrito Place"),
                makeRyaStatement("http://Bob", "http://livesIn", "http://Big City"),
                makeRyaStatement("http://Charlie", "http://worksAt", "http://Burrito Place"),
                makeRyaStatement("http://Charlie", "http://livesIn", "http://Big City"),
                makeRyaStatement("http://David", "http://worksAt", "http://Burrito Place"),
                makeRyaStatement("http://David", "http://livesIn", "http://Lost County"),
                makeRyaStatement("http://Eve", "http://worksAt", "http://Burrito Place"),
                makeRyaStatement("http://Eve", "http://livesIn", "http://Big City"),
                makeRyaStatement("http://Frank", "http://worksAt", "http://Burrito Place"),
                makeRyaStatement("http://Frank", "http://livesIn", "http://Lost County"));

        // Create the PCJ in Fluo.
        new CreatePcj().withRyaIntegration(fluoClient, RYA_TABLE_PREFIX, ryaRepo, accumuloConn, new HashSet<VariableOrder>(), sparql);

        // Stream the data into Fluo.
        new InsertTriples().insert(fluoClient, streamedTriples, Optional.<String>absent());

        // Wait for the results to finish processing.
        fluo.waitForObservers();

        // Fetch the report.
        final Map<String, PcjMetadata> metadata = new GetPcjMetadata().getMetadata(accumuloConn, fluoClient);
        final Set<String> queryIds = metadata.keySet();
        assertEquals(1, queryIds.size());
        final String queryId = queryIds.iterator().next();

        final QueryReport report = new GetQueryReport().getReport(fluoClient, queryId);

        // Build the expected counts map.
        final Map<String, BigInteger> expectedCounts = new HashMap<>();

        final FluoQuery fluoQuery = report.getFluoQuery();

        final String queryNodeId = fluoQuery.getQueryMetadata().getNodeId();
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