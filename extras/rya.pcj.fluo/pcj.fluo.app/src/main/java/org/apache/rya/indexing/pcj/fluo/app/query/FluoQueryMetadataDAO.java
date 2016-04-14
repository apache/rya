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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata.JoinType;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.collect.Sets;

import io.fluo.api.client.SnapshotBase;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.types.Encoder;
import io.fluo.api.types.StringEncoder;

/**
 * Reads and writes {@link FluoQuery} instances and their components to/from
 * a Fluo table.
 */
@ParametersAreNonnullByDefault
public class FluoQueryMetadataDAO {

    private static final Encoder encoder = new StringEncoder();

    /**
     * Write an instance of {@link QueryMetadata} to the Fluo table.
     *
     * @param tx - The transaction that will be used to commit the metadata. (not null)
     * @param metadata - The Query node metadata that will be written to the table. (not null)
     */
    public void write(final TransactionBase tx, final QueryMetadata metadata) {
        checkNotNull(tx);
        checkNotNull(metadata);

        final Bytes rowId = encoder.encode(metadata.getNodeId());
        tx.set(rowId, FluoQueryColumns.QUERY_NODE_ID, rowId);
        tx.set(rowId, FluoQueryColumns.QUERY_VARIABLE_ORDER, encoder.encode( metadata.getVariableOrder().toString() ));
        tx.set(rowId, FluoQueryColumns.QUERY_SPARQL, encoder.encode( metadata.getSparql() ));
        tx.set(rowId, FluoQueryColumns.QUERY_CHILD_NODE_ID, encoder.encode( metadata.getChildNodeId() ));
    }

    /**
     * Read an instance of {@link QueryMetadata} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata . (not null)
     * @param nodeId - The nodeId of the Query node that will be read. (not nul)
     * @return The {@link QueryMetadata} that was read from table.
     */
    public QueryMetadata readQueryMetadata(final SnapshotBase sx, final String nodeId) {
        return readQueryMetadataBuilder(sx, nodeId).build();
    }

    private QueryMetadata.Builder readQueryMetadataBuilder(final SnapshotBase sx, final String nodeId) {
        checkNotNull(sx);
        checkNotNull(nodeId);

        // Fetch the values from the Fluo table.
        final Bytes rowId = encoder.encode(nodeId);
        final Map<Column, Bytes> values = sx.get(rowId, Sets.newHashSet(
                FluoQueryColumns.QUERY_VARIABLE_ORDER,
                FluoQueryColumns.QUERY_SPARQL,
                FluoQueryColumns.QUERY_CHILD_NODE_ID));

        // Return an object holding them.
        final String varOrderString = encoder.decodeString( values.get(FluoQueryColumns.QUERY_VARIABLE_ORDER));
        final VariableOrder varOrder = new VariableOrder(varOrderString);

        final String sparql = encoder.decodeString( values.get(FluoQueryColumns.QUERY_SPARQL) );
        final String childNodeId = encoder.decodeString( values.get(FluoQueryColumns.QUERY_CHILD_NODE_ID) );

        return QueryMetadata.builder(nodeId)
                .setVariableOrder( varOrder )
                .setSparql( sparql )
                .setChildNodeId( childNodeId );
    }

    /**
     * Write an instance of {@link FilterMetadata} to the Fluo table.
     *
     * @param tx - The transaction that will be used to commit the metadata. (not null)
     * @param metadata - The Filter node metadata that will be written to the table. (not null)
     */
    public void write(final TransactionBase tx, final FilterMetadata metadata) {
        checkNotNull(tx);
        checkNotNull(metadata);

        final Bytes rowId = encoder.encode(metadata.getNodeId());
        tx.set(rowId, FluoQueryColumns.FILTER_NODE_ID, rowId);
        tx.set(rowId, FluoQueryColumns.FILTER_VARIABLE_ORDER, encoder.encode( metadata.getVariableOrder().toString() ));
        tx.set(rowId, FluoQueryColumns.FILTER_ORIGINAL_SPARQL, encoder.encode( metadata.getOriginalSparql() ));
        tx.set(rowId, FluoQueryColumns.FILTER_INDEX_WITHIN_SPARQL, encoder.encode( metadata.getFilterIndexWithinSparql() ));
        tx.set(rowId, FluoQueryColumns.FILTER_PARENT_NODE_ID, encoder.encode( metadata.getParentNodeId() ));
        tx.set(rowId, FluoQueryColumns.FILTER_CHILD_NODE_ID, encoder.encode( metadata.getChildNodeId() ));
    }

    /**
     * Read an instance of {@link FilterMetadata} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata. (not null)
     * @param nodeId - The nodeId of the Filter node that will be read. (not nul)
     * @return The {@link FilterMetadata} that was read from table.
     */
    public FilterMetadata readFilterMetadata(final SnapshotBase sx, final String nodeId) {
        return readFilterMetadataBuilder(sx, nodeId).build();
    }

    private FilterMetadata.Builder readFilterMetadataBuilder(final SnapshotBase sx, final String nodeId) {
        checkNotNull(sx);
        checkNotNull(nodeId);

        // Fetch the values from the Fluo table.
        final Bytes rowId = encoder.encode(nodeId);
        final Map<Column, Bytes> values = sx.get(rowId, Sets.newHashSet(
                FluoQueryColumns.FILTER_VARIABLE_ORDER,
                FluoQueryColumns.FILTER_ORIGINAL_SPARQL,
                FluoQueryColumns.FILTER_INDEX_WITHIN_SPARQL,
                FluoQueryColumns.FILTER_PARENT_NODE_ID,
                FluoQueryColumns.FILTER_CHILD_NODE_ID));

        // Return an object holding them.
        final String varOrderString = encoder.decodeString( values.get(FluoQueryColumns.FILTER_VARIABLE_ORDER));
        final VariableOrder varOrder = new VariableOrder(varOrderString);

        final String originalSparql = encoder.decodeString( values.get(FluoQueryColumns.FILTER_ORIGINAL_SPARQL) );
        final int filterIndexWithinSparql = encoder.decodeInteger( values.get(FluoQueryColumns.FILTER_INDEX_WITHIN_SPARQL) );
        final String parentNodeId = encoder.decodeString( values.get(FluoQueryColumns.FILTER_PARENT_NODE_ID) );
        final String childNodeId = encoder.decodeString( values.get(FluoQueryColumns.FILTER_CHILD_NODE_ID) );

        return FilterMetadata.builder(nodeId)
                .setVarOrder(varOrder)
                .setOriginalSparql(originalSparql)
                .setFilterIndexWithinSparql(filterIndexWithinSparql)
                .setParentNodeId(parentNodeId)
                .setChildNodeId(childNodeId);
    }

    /**
     * Write an instance of {@link JoinMetadata} to the Fluo table.
     *
     * @param tx - The transaction that will be used to commit the metadata. (not null)
     * @param metadata - The Join node metadata that will be written to the table. (not null)
     */
    public void write(final TransactionBase tx, final JoinMetadata metadata) {
        checkNotNull(tx);
        checkNotNull(metadata);

        final Bytes rowId = encoder.encode(metadata.getNodeId());
        tx.set(rowId, FluoQueryColumns.JOIN_NODE_ID, rowId);
        tx.set(rowId, FluoQueryColumns.JOIN_VARIABLE_ORDER, encoder.encode( metadata.getVariableOrder().toString() ));
        tx.set(rowId, FluoQueryColumns.JOIN_TYPE, encoder.encode(metadata.getJoinType().toString()) );
        tx.set(rowId, FluoQueryColumns.JOIN_PARENT_NODE_ID, encoder.encode( metadata.getParentNodeId() ));
        tx.set(rowId, FluoQueryColumns.JOIN_LEFT_CHILD_NODE_ID, encoder.encode( metadata.getLeftChildNodeId() ));
        tx.set(rowId, FluoQueryColumns.JOIN_RIGHT_CHILD_NODE_ID, encoder.encode( metadata.getRightChildNodeId() ));
    }

    /**
     * Read an instance of {@link JoinMetadata} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata. (not null)
     * @param nodeId - The nodeId of the Join node that will be read. (not nul)
     * @return The {@link JoinMetadata} that was read from table.
     */
    public JoinMetadata readJoinMetadata(final SnapshotBase sx, final String nodeId) {
        return readJoinMetadataBuilder(sx, nodeId).build();
    }

    private JoinMetadata.Builder readJoinMetadataBuilder(final SnapshotBase sx, final String nodeId) {
        checkNotNull(sx);
        checkNotNull(nodeId);

        // Fetch the values from the Fluo table.
        final Bytes rowId = encoder.encode(nodeId);
        final Map<Column, Bytes> values = sx.get(rowId, Sets.newHashSet(
                FluoQueryColumns.JOIN_VARIABLE_ORDER,
                FluoQueryColumns.JOIN_TYPE,
                FluoQueryColumns.JOIN_PARENT_NODE_ID,
                FluoQueryColumns.JOIN_LEFT_CHILD_NODE_ID,
                FluoQueryColumns.JOIN_RIGHT_CHILD_NODE_ID));

        // Return an object holding them.
        final String varOrderString = encoder.decodeString( values.get(FluoQueryColumns.JOIN_VARIABLE_ORDER));
        final VariableOrder varOrder = new VariableOrder(varOrderString);

        final String joinTypeString = encoder.decodeString( values.get(FluoQueryColumns.JOIN_TYPE) );
        final JoinType joinType = JoinType.valueOf(joinTypeString);

        final String parentNodeId = encoder.decodeString( values.get(FluoQueryColumns.JOIN_PARENT_NODE_ID) );
        final String leftChildNodeId = encoder.decodeString( values.get(FluoQueryColumns.JOIN_LEFT_CHILD_NODE_ID) );
        final String rightChildNodeId = encoder.decodeString( values.get(FluoQueryColumns.JOIN_RIGHT_CHILD_NODE_ID) );

        return JoinMetadata.builder(nodeId)
                .setVariableOrder(varOrder)
                .setJoinType(joinType)
                .setParentNodeId(parentNodeId)
                .setLeftChildNodeId(leftChildNodeId)
                .setRightChildNodeId(rightChildNodeId);
    }

    /**
     * Write an instance of {@link StatementPatternMetadata} to the Fluo table.
     *
     * @param tx - The transaction that will be used to commit the metadata. (not null)
     * @param metadata - The Statement Pattern node metadata that will be written to the table. (not null)
     */
    public void write(final TransactionBase tx, final StatementPatternMetadata metadata) {
        checkNotNull(tx);
        checkNotNull(metadata);

        final Bytes rowId = encoder.encode(metadata.getNodeId());
        tx.set(rowId, FluoQueryColumns.STATEMENT_PATTERN_NODE_ID, rowId);
        tx.set(rowId, FluoQueryColumns.STATEMENT_PATTERN_VARIABLE_ORDER, encoder.encode( metadata.getVariableOrder().toString() ));
        tx.set(rowId, FluoQueryColumns.STATEMENT_PATTERN_PATTERN, encoder.encode( metadata.getStatementPattern() ));
        tx.set(rowId, FluoQueryColumns.STATEMENT_PATTERN_PARENT_NODE_ID, encoder.encode( metadata.getParentNodeId() ));
    }

    /**
     * Read an instance of {@link StatementPatternMetadata} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata. (not null)
     * @param nodeId - The nodeId of the Statement Pattern node that will be read. (not nul)
     * @return The {@link StatementPatternMetadata} that was read from table.
     */
    public StatementPatternMetadata readStatementPatternMetadata(final SnapshotBase sx, final String nodeId) {
        return readStatementPatternMetadataBuilder(sx, nodeId).build();
    }

    private StatementPatternMetadata.Builder readStatementPatternMetadataBuilder(final SnapshotBase sx, final String nodeId) {
        checkNotNull(sx);
        checkNotNull(nodeId);

        // Fetch the values from the Fluo table.
        final Bytes rowId = encoder.encode(nodeId);
        final Map<Column, Bytes> values = sx.get(rowId, Sets.newHashSet(
                FluoQueryColumns.STATEMENT_PATTERN_VARIABLE_ORDER,
                FluoQueryColumns.STATEMENT_PATTERN_PATTERN,
                FluoQueryColumns.STATEMENT_PATTERN_PARENT_NODE_ID));

        // Return an object holding them.
        final String varOrderString = encoder.decodeString( values.get(FluoQueryColumns.STATEMENT_PATTERN_VARIABLE_ORDER));
        final VariableOrder varOrder = new VariableOrder(varOrderString);

        final String pattern = encoder.decodeString( values.get(FluoQueryColumns.STATEMENT_PATTERN_PATTERN) );
        final String parentNodeId = encoder.decodeString( values.get(FluoQueryColumns.STATEMENT_PATTERN_PARENT_NODE_ID) );

        return StatementPatternMetadata.builder(nodeId)
                .setVarOrder(varOrder)
                .setStatementPattern(pattern)
                .setParentNodeId(parentNodeId);
    }

    /**
     * Write an instance of {@link FluoQuery} to the Fluo table.
     *
     * @param tx - The transaction that will be used to commit the metadata. (not null)
     * @param query - The query metadata that will be written to the table. (not null)
     */
    public void write(final TransactionBase tx, final FluoQuery query) {
        checkNotNull(tx);
        checkNotNull(query);

        // Store the Query ID so that it may be looked up from the original SPARQL string.
        final String sparql = query.getQueryMetadata().getSparql();
        final String queryId = query.getQueryMetadata().getNodeId();
        tx.set(Bytes.of(sparql), FluoQueryColumns.QUERY_ID, Bytes.of(queryId));

        // Write the rest of the metadata objects.
        write(tx, query.getQueryMetadata());

        for(final FilterMetadata filter : query.getFilterMetadata()) {
            write(tx, filter);
        }

        for(final JoinMetadata join : query.getJoinMetadata()) {
            write(tx, join);
        }

        for(final StatementPatternMetadata statementPattern : query.getStatementPatternMetadata()) {
            write(tx, statementPattern);
        }
    }

    /**
     * Read an instance of {@link FluoQuery} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata from the Fluo table. (not null)
     * @param queryId - The ID of the query whose nodes will be read. (not null)
     * @return The {@link FluoQuery} that was read from table.
     */
    public FluoQuery readFluoQuery(final SnapshotBase sx, final String queryId) {
        checkNotNull(sx);
        checkNotNull(queryId);

        final FluoQuery.Builder fluoQueryBuilder = FluoQuery.builder();
        addChildMetadata(sx, fluoQueryBuilder, queryId);
        return fluoQueryBuilder.build();
    }

    private void addChildMetadata(final SnapshotBase sx, final FluoQuery.Builder builder, final String childNodeId) {
        checkNotNull(sx);
        checkNotNull(builder);
        checkNotNull(childNodeId);

        final NodeType childType = NodeType.fromNodeId(childNodeId).get();
        switch(childType) {
            case QUERY:
                // Add this node's metadata.
                final QueryMetadata.Builder queryBuilder = readQueryMetadataBuilder(sx, childNodeId);
                builder.setQueryMetadata(queryBuilder);

                // Add it's child's metadata.
                addChildMetadata(sx, builder, queryBuilder.build().getChildNodeId());
                break;

            case JOIN:
                // Add this node's metadata.
                final JoinMetadata.Builder joinBuilder = readJoinMetadataBuilder(sx, childNodeId);
                builder.addJoinMetadata(joinBuilder);

                // Add it's children's metadata.
                final JoinMetadata joinMetadata = joinBuilder.build();
                addChildMetadata(sx, builder, joinMetadata.getLeftChildNodeId());
                addChildMetadata(sx, builder, joinMetadata.getRightChildNodeId());
                break;

            case FILTER:
                // Add this node's metadata.
                final FilterMetadata.Builder filterBuilder = readFilterMetadataBuilder(sx, childNodeId);
                builder.addFilterMetadata(filterBuilder);

                // Add it's child's metadata.
                addChildMetadata(sx, builder, filterBuilder.build().getChildNodeId());
                break;

            case STATEMENT_PATTERN:
                // Add this node's metadata.
                final StatementPatternMetadata.Builder spBuilder = readStatementPatternMetadataBuilder(sx, childNodeId);
                builder.addStatementPatternBuilder(spBuilder);
                break;
        }
    }
}