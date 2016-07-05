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
package org.apache.rya.indexing.pcj.fluo.app;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.UUID;

import org.junit.Test;

import com.google.common.base.Optional;

/**
 * Tests the methods of {@link NodeType}.
 */
public class NodeTypeTest {

    @Test
    public void fromNodeId_StatementPattern() {
        final Optional<NodeType> type = NodeType.fromNodeId(IncrementalUpdateConstants.SP_PREFIX + "_" + UUID.randomUUID());
        assertEquals(NodeType.STATEMENT_PATTERN, type.get());
    }

    @Test
    public void fromNodeId_Join() {
        final Optional<NodeType> type = NodeType.fromNodeId(IncrementalUpdateConstants.JOIN_PREFIX + "_" + UUID.randomUUID());
        assertEquals(NodeType.JOIN, type.get());
    }

    @Test
    public void fromNodeId_Filter() {
        final Optional<NodeType> type = NodeType.fromNodeId(IncrementalUpdateConstants.FILTER_PREFIX + "_" + UUID.randomUUID());
        assertEquals(NodeType.FILTER, type.get());
    }

    @Test
    public void fromNodeId_Query() {
        final Optional<NodeType> type = NodeType.fromNodeId(IncrementalUpdateConstants.QUERY_PREFIX + "_" + UUID.randomUUID());
        assertEquals(NodeType.QUERY, type.get());
    }

    @Test
    public void fromNodeId_invalid() {
        final Optional<NodeType> type = NodeType.fromNodeId("Invalid ID String.");
        assertFalse( type.isPresent() );
    }
}