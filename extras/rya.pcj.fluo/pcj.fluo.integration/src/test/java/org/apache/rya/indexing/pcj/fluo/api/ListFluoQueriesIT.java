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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Transaction;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.CreatePCJ.QueryType;
import org.apache.rya.indexing.pcj.fluo.api.ListFluoQueries.FluoQueryStringBuilder;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.pcj.fluo.test.base.RyaExportITBase;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Sets;


public class ListFluoQueriesIT extends RyaExportITBase {

    @Test
    public void queryMetadataTest() throws Exception {
        final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

        String sparql1 = "select ?x ?y ?z where {?x <uri:p1> ?y; <uri:p2> 'literal1'. ?y <uri:p3> ?z }";
        String sparql2 = "select ?x ?y ?z where {{select ?x ?y ?z {?x <uri:p1> ?y; <uri:p2> ?z. ?y <uri:p3> ?z }}}";
        
        // Create the object that will be serialized.
        String queryId1 = NodeType.generateNewFluoIdForType(NodeType.QUERY);
        final QueryMetadata.Builder builder = QueryMetadata.builder(queryId1);
        builder.setQueryType(QueryType.PROJECTION);
        builder.setVarOrder(new VariableOrder("y;s;d"));
        builder.setSparql(sparql1);
        builder.setChildNodeId("childNodeId");
        builder.setExportStrategies(new HashSet<>(Arrays.asList(ExportStrategy.KAFKA)));
        final QueryMetadata meta1 = builder.build();

        String queryId2 = NodeType.generateNewFluoIdForType(NodeType.QUERY);
        final QueryMetadata.Builder builder2 = QueryMetadata.builder(queryId2);
        builder2.setQueryType(QueryType.PROJECTION);
        builder2.setVarOrder(new VariableOrder("y;s;d"));
        builder2.setSparql(sparql2);
        builder2.setChildNodeId("childNodeId");
        builder2.setExportStrategies(new HashSet<>(Arrays.asList(ExportStrategy.RYA)));
        final QueryMetadata meta2 = builder2.build();

        try (FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Write it to the Fluo table.
            try (Transaction tx = fluoClient.newTransaction()) {
                dao.write(tx, meta1);
                dao.write(tx, meta2);
                tx.commit();
            }
            ListFluoQueries listFluoQueries = new ListFluoQueries();
            List<String> queries = listFluoQueries.listFluoQueries(fluoClient);
            
            FluoQueryStringBuilder queryBuilder1 = new FluoQueryStringBuilder();
            String expected1 = queryBuilder1.setQueryId(queryId1).setQueryType(QueryType.PROJECTION).setQuery(sparql1)
                    .setExportStrategies(Sets.newHashSet(ExportStrategy.KAFKA)).build();

            FluoQueryStringBuilder queryBuilder2 = new FluoQueryStringBuilder();
            String expected2 = queryBuilder2.setQueryId(queryId2).setQueryType(QueryType.PROJECTION).setQuery(sparql2)
                    .setExportStrategies(Sets.newHashSet(ExportStrategy.RYA)).build();
            
            Set<String> expected = new HashSet<>();
            expected.add(expected1);
            expected.add(expected2);
            
            Assert.assertEquals(expected, Sets.newHashSet(queries));
        }
    }
}
