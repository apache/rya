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
package org.apache.rya.indexing.pcj.fluo.app.util;

import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.SP_PREFIX;

import org.apache.fluo.api.data.Bytes;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

public class BindingHashShardingFunctionTest {

    private static final ValueFactory vf = new ValueFactoryImpl();

    @Test
    public void shardAddAndRemoveTest() {
        String nodeId = NodeType.generateNewFluoIdForType(NodeType.STATEMENT_PATTERN);
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("entity", vf.createURI("urn:entity"));
        bs.addBinding("location", vf.createLiteral("location_1"));
        VisibilityBindingSet vBs = new VisibilityBindingSet(bs);
        VariableOrder varOrder = new VariableOrder("entity","location");
        Bytes row = RowKeyUtil.makeRowKey(nodeId, varOrder, vBs);
        Bytes shardedRow = BindingHashShardingFunction.addShard(nodeId, varOrder, vBs);
        Bytes shardlessRow = BindingHashShardingFunction.removeHash(Bytes.of(SP_PREFIX), shardedRow);
        Assert.assertEquals(row, shardlessRow);
    }

    @Test
    public void bindingSetRowTest() {
        String nodeId = NodeType.generateNewFluoIdForType(NodeType.STATEMENT_PATTERN);
        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("entity", vf.createURI("urn:entity"));
        bs.addBinding("location", vf.createLiteral("location_1"));
        VisibilityBindingSet vBs = new VisibilityBindingSet(bs);
        VariableOrder varOrder = new VariableOrder("entity","location");
        Bytes row = RowKeyUtil.makeRowKey(nodeId, varOrder, vBs);
        Bytes shardedRow = BindingHashShardingFunction.addShard(nodeId, varOrder, vBs);
        BindingSetRow expected = BindingSetRow.make(row);
        BindingSetRow actual = BindingSetRow.makeFromShardedRow(Bytes.of(SP_PREFIX), shardedRow);
        Assert.assertEquals(expected, actual);
    }
}
