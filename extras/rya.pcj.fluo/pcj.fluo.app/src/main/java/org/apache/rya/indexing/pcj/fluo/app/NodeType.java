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
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.AGGREGATION_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.CONSTRUCT_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.FILTER_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.JOIN_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.QUERY_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.PROJECTION_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.SP_PREFIX;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.PERIODIC_QUERY_PREFIX;

import java.util.List;
import java.util.UUID;

import org.apache.fluo.api.data.Column;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns.QueryNodeMetadataColumns;

import com.google.common.base.Optional;

/**
 * Represents the different types of nodes that a Query may have.
 */
public enum NodeType {
    PERIODIC_QUERY(IncrementalUpdateConstants.PERIODIC_QUERY_PREFIX, QueryNodeMetadataColumns.PERIODIC_QUERY_COLUMNS, FluoQueryColumns.PERIODIC_QUERY_BINDING_SET),
    FILTER (IncrementalUpdateConstants.FILTER_PREFIX, QueryNodeMetadataColumns.FILTER_COLUMNS, FluoQueryColumns.FILTER_BINDING_SET),
    JOIN(IncrementalUpdateConstants.JOIN_PREFIX, QueryNodeMetadataColumns.JOIN_COLUMNS, FluoQueryColumns.JOIN_BINDING_SET),
    STATEMENT_PATTERN(IncrementalUpdateConstants.SP_PREFIX, QueryNodeMetadataColumns.STATEMENTPATTERN_COLUMNS, FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET),
    QUERY(IncrementalUpdateConstants.QUERY_PREFIX, QueryNodeMetadataColumns.QUERY_COLUMNS, FluoQueryColumns.QUERY_BINDING_SET),
    AGGREGATION(IncrementalUpdateConstants.AGGREGATION_PREFIX, QueryNodeMetadataColumns.AGGREGATION_COLUMNS, FluoQueryColumns.AGGREGATION_BINDING_SET),
    PROJECTION(IncrementalUpdateConstants.PROJECTION_PREFIX, QueryNodeMetadataColumns.PROJECTION_COLUMNS, FluoQueryColumns.PROJECTION_BINDING_SET),
    CONSTRUCT(IncrementalUpdateConstants.CONSTRUCT_PREFIX, QueryNodeMetadataColumns.CONSTRUCT_COLUMNS, FluoQueryColumns.CONSTRUCT_STATEMENTS);

    //Metadata Columns associated with given NodeType
    private QueryNodeMetadataColumns metadataColumns;

    //Column where results are stored for given NodeType
    private Column resultColumn;

    //Prefix for the given node type
    private String nodePrefix;
    /**
     * Constructs an instance of {@link NodeType}.
     *
     * @param metadataColumns - Metadata {@link Column}s associated with this {@link NodeType}. (not null)
     * @param resultColumn - The {@link Column} used to store this {@link NodeType}'s results. (not null)
     */
    private NodeType(String nodePrefix, QueryNodeMetadataColumns metadataColumns, Column resultColumn) {
    	this.metadataColumns = requireNonNull(metadataColumns);
    	this.resultColumn = requireNonNull(resultColumn);
    	this.nodePrefix = requireNonNull(nodePrefix);
    }
    
    /**
     * @return the prefix for the given node type
     */
    public String getNodeTypePrefix() {
        return nodePrefix;
    }

    /**
     * @return Metadata {@link Column}s associated with this {@link NodeType}.
     */
    public List<Column> getMetaDataColumns() {
    	return metadataColumns.columns();
    }


    /**
     * @return The {@link Column} used to store this {@link NodeType}'s query results.
     */
    public Column getResultColumn() {
    	return resultColumn;
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
        } else if(nodeId.startsWith(AGGREGATION_PREFIX)) {
            type = AGGREGATION;
        } else if(nodeId.startsWith(CONSTRUCT_PREFIX)) {
            type = CONSTRUCT;
        } else if(nodeId.startsWith(PROJECTION_PREFIX)) {
            type = PROJECTION;
        } else if(nodeId.startsWith(PERIODIC_QUERY_PREFIX)) {
            type = PERIODIC_QUERY;
        }

        return Optional.fromNullable(type);
    }
    
    /**
     * Creates an id for a given NodeType that is of the form {@link NodeType#getNodeTypePrefix()} + "_" + pcjId,
     * where the pcjId is an auto generated UUID with all dashes removed.
     * @param type {@link NodeType}
     * @return id for the given NodeType
     */
    public static String generateNewFluoIdForType(NodeType type) {
        String unique = UUID.randomUUID().toString().replaceAll("-", "");
        // Put them together to create the Node ID.
        return type.getNodeTypePrefix() + "_" + unique;
    }
    
    /**
     * Creates an id for a given NodeType that is of the form {@link NodeType#getNodeTypePrefix()} + "_" + pcjId
     * 
     * @param type {@link NodeType}
     * @return id for the given NodeType
     */
    public static String generateNewIdForType(NodeType type, String pcjId) {
        // Put them together to create the Node ID.
        return type.getNodeTypePrefix() + "_" + pcjId;
    }
    
    
    
}
