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

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.rya.indexing.pcj.fluo.api.CreateFluoPcj;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternIdCache;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoQueryUtils;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.junit.Test;

import com.google.common.collect.Sets;

public class StatementPatternIdCacheIT extends RyaExportITBase {

    /**
     * Ensure streamed matches are included in the result.
     */
    @Test
    public void statementPatternIdCacheTest() throws Exception {
        // A query that finds people who talk to Eve and work at Chipotle.
        final String sparql1 =
              "SELECT ?x WHERE { " +
                "?x <urn:pred1> <urn:obj1>. " +
                "?x <urn:pred2> <urn:obj2>." +
              "}";

        final String sparql2 =
                "SELECT ?x WHERE { " +
                  "?x <urn:pred3> <urn:obj3>. " +
                  "?x <urn:pred4> <urn:obj4>." +
                "}";

        try(FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {

            String pcjId = FluoQueryUtils.createNewPcjId();
            // Tell the Fluo app to maintain the PCJ.
            FluoQuery query1 = new CreateFluoPcj().createPcj(pcjId, sparql1, new HashSet<>(), fluoClient);
            Set<String> spIds1 = new HashSet<>();
            for(StatementPatternMetadata metadata: query1.getStatementPatternMetadata()) {
                spIds1.add(metadata.getNodeId());
            }

            StatementPatternIdCache cache = new StatementPatternIdCache();

            assertEquals(spIds1, cache.getStatementPatternIds(fluoClient.newTransaction()));

            FluoQuery query2 = new CreateFluoPcj().createPcj(pcjId, sparql2, new HashSet<>(), fluoClient);
            Set<String> spIds2 = new HashSet<>();
            for(StatementPatternMetadata metadata: query2.getStatementPatternMetadata()) {
                spIds2.add(metadata.getNodeId());
            }

            assertEquals(Sets.union(spIds1, spIds2), cache.getStatementPatternIds(fluoClient.newTransaction()));
        }
    }

}
