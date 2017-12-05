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
package org.apache.rya.indexing.pcj.fluo.app.query;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.apache.fluo.api.client.Transaction;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;
import org.mockito.Mockito;

public class FluoQueryMetadataCacheTest {

    @Test
    public void testCache() {
        FluoQueryMetadataDAO mockDAO = Mockito.mock(FluoQueryMetadataDAO.class);
        Transaction mockTx = Mockito.mock(Transaction.class);
        String nodeId = NodeType.generateNewFluoIdForType(NodeType.STATEMENT_PATTERN);
        StatementPatternMetadata metadata = StatementPatternMetadata.builder(nodeId).setParentNodeId("parent")
                .setStatementPattern("pattern").setVarOrder(new VariableOrder("xyz")).build();
        when(mockDAO.readStatementPatternMetadata(mockTx, nodeId)).thenReturn(metadata);

        FluoQueryMetadataCache cache = new FluoQueryMetadataCache(mockDAO, 20, 2);

        assertEquals(metadata, cache.readStatementPatternMetadata(mockTx, nodeId));

        cache.readStatementPatternMetadata(mockTx, nodeId);
        cache.readStatementPatternMetadata(mockTx, nodeId);
        cache.readStatementPatternMetadata(mockTx, nodeId);
        cache.readStatementPatternMetadata(mockTx, nodeId);

        Mockito.verify(mockDAO, Mockito.times(1)).readStatementPatternMetadata(mockTx, nodeId);
    }
}
