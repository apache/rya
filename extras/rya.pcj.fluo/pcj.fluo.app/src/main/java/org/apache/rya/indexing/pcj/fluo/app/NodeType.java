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

import static java.util.Objects.requireNonNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.FILTER_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.JOIN_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.QUERY_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.SP_PREFIX;

import java.util.List;

import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns.QueryNodeMetadataColumns;
import org.openrdf.query.BindingSet;

import com.google.common.base.Optional;

import org.apache.fluo.api.data.Column;

/**
 * Represents the different types of nodes that a Query may have.
 */
public enum NodeType {
    FILTER (QueryNodeMetadataColumns.FILTER_COLUMNS, FluoQueryColumns.FILTER_BINDING_SET),
    JOIN(QueryNodeMetadataColumns.JOIN_COLUMNS, FluoQueryColumns.JOIN_BINDING_SET),
    STATEMENT_PATTERN(QueryNodeMetadataColumns.STATEMENTPATTERN_COLUMNS, FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET),
    QUERY(QueryNodeMetadataColumns.QUERY_COLUMNS, FluoQueryColumns.QUERY_BINDING_SET);

    //Metadata Columns associated with given NodeType
    private QueryNodeMetadataColumns metadataColumns;

    //Column where BindingSet results are stored for given NodeType
    private Column bindingSetColumn;

    /**
     * Constructs an instance of {@link NodeType}.
     *
     * @param metadataColumns - Metadata {@link Column}s associated with this {@link NodeType}. (not null)
     * @param bindingSetColumn - The {@link Column} used to store this {@link NodeType|'s {@link BindingSet}s. (not null)
     */
    private NodeType(QueryNodeMetadataColumns metadataColumns, Column bindingSetColumn) {
    	this.metadataColumns = requireNonNull(metadataColumns);
    	this.bindingSetColumn = requireNonNull(bindingSetColumn);
    }

    /**
     * @return Metadata {@link Column}s associated with this {@link NodeType}.
     */
    public List<Column> getMetaDataColumns() {
    	return metadataColumns.columns();
    }


    /**
     * @return The {@link Column} used to store this {@link NodeType|'s {@link BindingSet}s.
     */
    public Column getBsColumn() {
    	return bindingSetColumn;
    }

    /**
     * Get the {@link NodeType} of a node based on its Node ID.
     *
     * @param nodeId - The Node ID of a node. (not null)
     * @return The {@link NodeType} of the node if it could be derived from the
     *   node's ID, otherwise absent.
     */
    public static Optional<NodeType> fromNodeId(final String nodeId) {
        requireNonNull(nodeId);

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
