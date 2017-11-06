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

import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.STATEMENT_PATTERN_ID;
import static org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns.STATEMENT_PATTERN_IDS;
import static org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns.STATEMENT_PATTERN_IDS_HASH;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

public class StatementPatternIdCacheTest {

    @Test
    public void testCache() {
        Transaction mockTx = Mockito.mock(Transaction.class);
        String nodeId = NodeType.generateNewFluoIdForType(NodeType.STATEMENT_PATTERN);
        when(mockTx.get(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS)).thenReturn(Bytes.of(nodeId));
        when(mockTx.get(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS_HASH)).thenReturn(Bytes.of("123"));

        StatementPatternIdCache cache = new StatementPatternIdCache();
        Set<String> ids1 = cache.getStatementPatternIds(mockTx);
        Set<String> ids2 = cache.getStatementPatternIds(mockTx);

        Assert.assertEquals(ids1, ids2);
        Assert.assertEquals(Sets.newHashSet(nodeId), ids1);

        Mockito.verify(mockTx, Mockito.times(1)).get(Bytes.of(STATEMENT_PATTERN_ID), STATEMENT_PATTERN_IDS);
    }
}
