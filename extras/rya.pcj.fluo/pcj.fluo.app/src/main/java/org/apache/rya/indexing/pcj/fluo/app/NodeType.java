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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.FILTER_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.JOIN_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.QUERY_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.SP_PREFIX;

import com.google.common.base.Optional;

/**
 * Represents the different types of nodes that a Query may have.
 */
public enum NodeType {
    FILTER,
    JOIN,
    STATEMENT_PATTERN,
    QUERY;

    /**
     * Get the {@link NodeType} of a node based on its Node ID.
     *
     * @param nodeId - The Node ID of a node. (not null)
     * @return The {@link NodeType} of the node if it could be derived from the
     *   node's ID, otherwise absent.
     */
    public static Optional<NodeType> fromNodeId(final String nodeId) {
        checkNotNull(nodeId);

        NodeType type = null;

        if(nodeId.startsWith(SP_PREFIX)) {
            type = STATEMENT_PATTERN;
        } else if(nodeId.startsWith(FILTER_PREFIX)) {
            type = FILTER;
        } else if(nodeId.startsWith(JOIN_PREFIX)) {
            type = JOIN;
        } else if(nodeId.startsWith(QUERY_PREFIX)) {
            type = QUERY;
        }

        return Optional.fromNullable(type);
    }
}