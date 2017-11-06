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

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.CreatePCJ.QueryType;
import org.apache.rya.indexing.pcj.fluo.app.ConstructGraph;
import org.apache.rya.indexing.pcj.fluo.app.ConstructGraphSerializer;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.query.AggregationMetadata.AggregationElement;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata.JoinType;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Reads and writes {@link FluoQuery} instances and their components to/from
 * a Fluo table.
 * <p>
 * Note, this class should be implemented in a thread-safe manner due to current usage.
 */
@DefaultAnnotation(NonNull.class)
public class FluoQueryMetadataDAO {

    /**
     * Write an instance of {@link QueryMetadata} to the Fluo table.
     *
     * @param tx - The transaction that will be used to commit the metadata. (not null)
     * @param metadata - The Query node metadata that will be written to the table. (not null)
     */
    public void write(final TransactionBase tx, final QueryMetadata metadata) {
        requireNonNull(tx);
        requireNonNull(metadata);

        final Joiner joiner = Joiner.on(IncrementalUpdateConstants.VAR_DELIM);

        final String rowId = metadata.getNodeId();
        tx.set(rowId, FluoQueryColumns.QUERY_NODE_ID, rowId);
        tx.set(rowId, FluoQueryColumns.QUERY_VARIABLE_ORDER, metadata.getVariableOrder().toString());
        tx.set(rowId, FluoQueryColumns.QUERY_SPARQL, metadata.getSparql() );
        tx.set(rowId, FluoQueryColumns.QUERY_EXPORT_STRATEGIES, joiner.join(metadata.getExportStrategies()));
        tx.set(rowId, FluoQueryColumns.QUERY_TYPE, metadata.getQueryType().toString());
        tx.set(rowId, FluoQueryColumns.QUERY_CHILD_NODE_ID, metadata.getChildNodeId() );
    }

    /**
     * Read an instance of {@link QueryMetadata} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata . (not null)
     * @param nodeId - The nodeId of the Query node that will be read. (not nul)
     * @return The {@link QueryMetadata} that was read from the table.
     */
    public QueryMetadata readQueryMetadata(final SnapshotBase sx, final String nodeId) {
        return readQueryMetadataBuilder(sx, nodeId).build();
    }

    private QueryMetadata.Builder readQueryMetadataBuilder(final SnapshotBase sx, final String nodeId) {
        requireNonNull(sx);
        requireNonNull(nodeId);

        // Fetch the values from the Fluo table.
        final String rowId = nodeId;
        final Map<Column, String> values = sx.gets(rowId,
                FluoQueryColumns.QUERY_VARIABLE_ORDER,
                FluoQueryColumns.QUERY_SPARQL,
                FluoQueryColumns.QUERY_TYPE,
                FluoQueryColumns.QUERY_EXPORT_STRATEGIES,
                FluoQueryColumns.QUERY_CHILD_NODE_ID);

        // Return an object holding them.
        final String varOrderString = values.get(FluoQueryColumns.QUERY_VARIABLE_ORDER);
        final VariableOrder varOrder = new VariableOrder(varOrderString);

        final String sparql = values.get(FluoQueryColumns.QUERY_SPARQL);
        final String childNodeId = values.get(FluoQueryColumns.QUERY_CHILD_NODE_ID);
        final String queryType = values.get(FluoQueryColumns.QUERY_TYPE);
        final String[] exportStrategies = values.get(FluoQueryColumns.QUERY_EXPORT_STRATEGIES).split(IncrementalUpdateConstants.VAR_DELIM);

        final Set<ExportStrategy> strategies = new HashSet<>();
        for (final String strategy : exportStrategies) {
            if (!strategy.isEmpty()) {
                strategies.add(ExportStrategy.valueOf(strategy));
            }
        }

        return QueryMetadata.builder(nodeId)
                .setVarOrder( varOrder )
                .setSparql( sparql )
                .setExportStrategies(strategies)
                .setQueryType(QueryType.valueOf(queryType))
                .setChildNodeId( childNodeId );
    }


    /**
     * Write an instance of {@link ProjectionMetadata} to the Fluo table.
     *
     * @param tx - The transaction that will be used to commit the metadata. (not null)
     * @param metadata - The Query node metadata that will be written to the table. (not null)
     */
    public void write(final TransactionBase tx, final ProjectionMetadata metadata) {
        requireNonNull(tx);
        requireNonNull(metadata);

        final String rowId = metadata.getNodeId();
        tx.set(rowId, FluoQueryColumns.PROJECTION_NODE_ID, rowId);
        tx.set(rowId, FluoQueryColumns.PROJECTION_VARIABLE_ORDER, metadata.getVariableOrder().toString());
        tx.set(rowId, FluoQueryColumns.PROJECTION_PROJECTED_VARS, metadata.getProjectedVars().toString());
        tx.set(rowId, FluoQueryColumns.PROJECTION_PARENT_NODE_ID, metadata.getParentNodeId());
        tx.set(rowId, FluoQueryColumns.PROJECTION_CHILD_NODE_ID, metadata.getChildNodeId() );
    }

    /**
     * Read an instance of {@link ProjectionMetadata} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata . (not null)
     * @param nodeId - The nodeId of the Projection node that will be read. (not null)
     * @return The {@link ProjectionMetadata} that was read from the table.
     */
    public ProjectionMetadata readProjectionMetadata(final SnapshotBase sx, final String nodeId) {
        return readProjectionMetadataBuilder(sx, nodeId).build();
    }

    private ProjectionMetadata.Builder readProjectionMetadataBuilder(final SnapshotBase sx, final String nodeId) {
        requireNonNull(sx);
        requireNonNull(nodeId);

        // Fetch the values from the Fluo table.
        final String rowId = nodeId;
        final Map<Column, String> values = sx.gets(rowId,
                FluoQueryColumns.PROJECTION_VARIABLE_ORDER,
                FluoQueryColumns.PROJECTION_PROJECTED_VARS,
                FluoQueryColumns.PROJECTION_PARENT_NODE_ID,
                FluoQueryColumns.PROJECTION_CHILD_NODE_ID);

        // Return an object holding them.
        final String varOrderString = values.get(FluoQueryColumns.PROJECTION_VARIABLE_ORDER);
        final String projectedVarString = values.get(FluoQueryColumns.PROJECTION_PROJECTED_VARS);
        final VariableOrder varOrder = new VariableOrder(varOrderString);
        final VariableOrder projectedVars = new VariableOrder(projectedVarString);
        final String childNodeId = values.get(FluoQueryColumns.PROJECTION_CHILD_NODE_ID);
        final String parentNodeId = values.get(FluoQueryColumns.PROJECTION_PARENT_NODE_ID);


        return ProjectionMetadata.builder(nodeId)
                .setVarOrder( varOrder )
                .setProjectedVars(projectedVars)
                .setParentNodeId(parentNodeId)
                .setChildNodeId( childNodeId );
    }


    /**
     * Write an instance of {@link ConstructQueryMetadata} to the Fluo table.
     *
     * @param tx - The transaction that will be used to commit the metadata. (not null)
     * @param metadata - The Construct Query node metadata that will be written to the table. (not null)
     */
    public void write(final TransactionBase tx, final ConstructQueryMetadata metadata) {
        requireNonNull(tx);
        requireNonNull(metadata);

        final String rowId = metadata.getNodeId();
        tx.set(rowId, FluoQueryColumns.CONSTRUCT_NODE_ID, rowId);
        tx.set(rowId, FluoQueryColumns.CONSTRUCT_VARIABLE_ORDER, metadata.getVariableOrder().toString());
        tx.set(rowId, FluoQueryColumns.CONSTRUCT_CHILD_NODE_ID, metadata.getChildNodeId() );
        tx.set(rowId, FluoQueryColumns.CONSTRUCT_PARENT_NODE_ID, metadata.getParentNodeId() );
        tx.set(rowId, FluoQueryColumns.CONSTRUCT_GRAPH, ConstructGraphSerializer.toConstructString(metadata.getConstructGraph()));
    }

    /**
     * Read an instance of {@link ConstructQueryMetadata} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata . (not null)
     * @param nodeId - The nodeId of the Construct Query node that will be read. (not null)
     * @return The {@link ConstructQueryMetadata} that was read from table.
     */
    public ConstructQueryMetadata readConstructQueryMetadata(final SnapshotBase sx, final String nodeId) {
        return readConstructQueryMetadataBuilder(sx, nodeId).build();
    }

    private ConstructQueryMetadata.Builder readConstructQueryMetadataBuilder(final SnapshotBase sx, final String nodeId) {
        requireNonNull(sx);
        requireNonNull(nodeId);

        // Fetch the values from the Fluo table.
        final String rowId = nodeId;
        final Map<Column, String> values = sx.gets(rowId,
                FluoQueryColumns.CONSTRUCT_GRAPH,
                FluoQueryColumns.CONSTRUCT_CHILD_NODE_ID,
                FluoQueryColumns.CONSTRUCT_PARENT_NODE_ID,
                FluoQueryColumns.CONSTRUCT_VARIABLE_ORDER);

        final String graphString = values.get(FluoQueryColumns.CONSTRUCT_GRAPH);
        final ConstructGraph graph = ConstructGraphSerializer.toConstructGraph(graphString);
        final String childNodeId = values.get(FluoQueryColumns.CONSTRUCT_CHILD_NODE_ID);
        final String parentNodeId = values.get(FluoQueryColumns.CONSTRUCT_PARENT_NODE_ID);
        final String varOrderString = values.get(FluoQueryColumns.CONSTRUCT_VARIABLE_ORDER);


        return ConstructQueryMetadata.builder()
                .setNodeId(nodeId)
                .setParentNodeId(parentNodeId)
                .setConstructGraph(graph)
                .setVarOrder(new VariableOrder(varOrderString))
                .setChildNodeId(childNodeId);
    }


    /**
     * Write an instance of {@link FilterMetadata} to the Fluo table.
     *
     * @param tx - The transaction that will be used to commit the metadata. (not null)
     * @param metadata - The Filter node metadata that will be written to the table. (not null)
     */
    public void write(final TransactionBase tx, final FilterMetadata metadata) {
        requireNonNull(tx);
        requireNonNull(metadata);

        final String rowId = metadata.getNodeId();
        tx.set(rowId, FluoQueryColumns.FILTER_NODE_ID, rowId);
        tx.set(rowId, FluoQueryColumns.FILTER_VARIABLE_ORDER, metadata.getVariableOrder().toString());
        tx.set(rowId, FluoQueryColumns.FILTER_SPARQL, metadata.getFilterSparql() );
        tx.set(rowId, FluoQueryColumns.FILTER_PARENT_NODE_ID, metadata.getParentNodeId() );
        tx.set(rowId, FluoQueryColumns.FILTER_CHILD_NODE_ID, metadata.getChildNodeId() );
    }

    /**
     * Read an instance of {@link FilterMetadata} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata. (not null)
     * @param nodeId - The nodeId of the Filter node that will be read. (not nul)
     * @return The {@link FilterMetadata} that was read from the table.
     */
    public FilterMetadata readFilterMetadata(final SnapshotBase sx, final String nodeId) {
        return readFilterMetadataBuilder(sx, nodeId).build();
    }

    private FilterMetadata.Builder readFilterMetadataBuilder(final SnapshotBase sx, final String nodeId) {
        requireNonNull(sx);
        requireNonNull(nodeId);

        // Fetch the values from the Fluo table.
        final String rowId = nodeId;
        final Map<Column, String> values = sx.gets(rowId,
                FluoQueryColumns.FILTER_VARIABLE_ORDER,
                FluoQueryColumns.FILTER_SPARQL,
                FluoQueryColumns.FILTER_PARENT_NODE_ID,
                FluoQueryColumns.FILTER_CHILD_NODE_ID);

        // Return an object holding them.
        final String varOrderString = values.get(FluoQueryColumns.FILTER_VARIABLE_ORDER);
        final VariableOrder varOrder = new VariableOrder(varOrderString);

        final String originalSparql = values.get(FluoQueryColumns.FILTER_SPARQL);
        final String parentNodeId = values.get(FluoQueryColumns.FILTER_PARENT_NODE_ID);
        final String childNodeId = values.get(FluoQueryColumns.FILTER_CHILD_NODE_ID);

        return FilterMetadata.builder(nodeId).setVarOrder(varOrder).setFilterSparql(originalSparql)
                .setParentNodeId(parentNodeId).setChildNodeId(childNodeId);
    }

    /**
     * Write an instance of {@link PeriodicQueryMetadata} to the Fluo table.
     *
     * @param tx
     *            - The transaction that will be used to commit the metadata.
     *            (not null)
     * @param metadata
     *            - The PeriodicBin node metadata that will be written to the
     *            table. (not null)
     */
    public void write(final TransactionBase tx, final PeriodicQueryMetadata metadata) {
        requireNonNull(tx);
        requireNonNull(metadata);

        final String rowId = metadata.getNodeId();
        tx.set(rowId, FluoQueryColumns.PERIODIC_QUERY_NODE_ID, rowId);
        tx.set(rowId, FluoQueryColumns.PERIODIC_QUERY_VARIABLE_ORDER, metadata.getVariableOrder().toString());
        tx.set(rowId, FluoQueryColumns.PERIODIC_QUERY_PARENT_NODE_ID, metadata.getParentNodeId());
        tx.set(rowId, FluoQueryColumns.PERIODIC_QUERY_CHILD_NODE_ID, metadata.getChildNodeId());
        tx.set(rowId, FluoQueryColumns.PERIODIC_QUERY_PERIOD, Long.toString(metadata.getPeriod()));
        tx.set(rowId, FluoQueryColumns.PERIODIC_QUERY_WINDOWSIZE, Long.toString(metadata.getWindowSize()));
        tx.set(rowId, FluoQueryColumns.PERIODIC_QUERY_TIMEUNIT, metadata.getUnit().name());
        tx.set(rowId, FluoQueryColumns.PERIODIC_QUERY_TEMPORAL_VARIABLE, metadata.getTemporalVariable());
    }

    /**
     * Read an instance of {@link PeriodicQueryMetadata} from the Fluo table.
     *
     * @param sx
     *            - The snapshot that will be used to read the metadata. (not
     *            null)
     * @param nodeId
     *            - The nodeId of the PeriodicBin node that will be read. (not
     *            null)
     * @return The {@link PeriodicQueryMetadata} that was read from table.
     */
    public PeriodicQueryMetadata readPeriodicQueryMetadata(final SnapshotBase sx, final String nodeId) {
        return readPeriodicQueryMetadataBuilder(sx, nodeId).build();
    }

    private PeriodicQueryMetadata.Builder readPeriodicQueryMetadataBuilder(final SnapshotBase sx, final String nodeId) {
        requireNonNull(sx);
        requireNonNull(nodeId);

        // Fetch the values from the Fluo table.
        final String rowId = nodeId;
        final Map<Column, String> values = sx.gets(rowId, FluoQueryColumns.PERIODIC_QUERY_VARIABLE_ORDER,
                FluoQueryColumns.PERIODIC_QUERY_PARENT_NODE_ID, FluoQueryColumns.PERIODIC_QUERY_CHILD_NODE_ID,
                FluoQueryColumns.PERIODIC_QUERY_PERIOD, FluoQueryColumns.PERIODIC_QUERY_WINDOWSIZE,
                FluoQueryColumns.PERIODIC_QUERY_TIMEUNIT, FluoQueryColumns.PERIODIC_QUERY_TEMPORAL_VARIABLE);

        // Return an object holding them.
        final String varOrderString = values.get(FluoQueryColumns.PERIODIC_QUERY_VARIABLE_ORDER);
        final VariableOrder varOrder = new VariableOrder(varOrderString);
        final String parentNodeId = values.get(FluoQueryColumns.PERIODIC_QUERY_PARENT_NODE_ID);
        final String childNodeId = values.get(FluoQueryColumns.PERIODIC_QUERY_CHILD_NODE_ID);
        final String temporalVariable = values.get(FluoQueryColumns.PERIODIC_QUERY_TEMPORAL_VARIABLE);
        final String period = values.get(FluoQueryColumns.PERIODIC_QUERY_PERIOD);
        final String window = values.get(FluoQueryColumns.PERIODIC_QUERY_WINDOWSIZE);
        final String timeUnit = values.get(FluoQueryColumns.PERIODIC_QUERY_TIMEUNIT);

        return PeriodicQueryMetadata.builder()
                .setNodeId(nodeId)
                .setVarOrder(varOrder)
                .setParentNodeId(parentNodeId)
                .setChildNodeId(childNodeId)
                .setWindowSize(Long.parseLong(window))
                .setPeriod(Long.parseLong(period))
                .setTemporalVariable(temporalVariable)
                .setUnit(TimeUnit.valueOf(timeUnit));

    }



    /**
     * Write an instance of {@link JoinMetadata} to the Fluo table.
     *
     * @param tx - The transaction that will be used to commit the metadata. (not null)
     * @param metadata - The Join node metadata that will be written to the table. (not null)
     */
    public void write(final TransactionBase tx, final JoinMetadata metadata) {
        requireNonNull(tx);
        requireNonNull(metadata);

        final String rowId = metadata.getNodeId();
        tx.set(rowId, FluoQueryColumns.JOIN_NODE_ID, rowId);
        tx.set(rowId, FluoQueryColumns.JOIN_VARIABLE_ORDER, metadata.getVariableOrder().toString());
        tx.set(rowId, FluoQueryColumns.JOIN_TYPE, metadata.getJoinType().toString() );
        tx.set(rowId, FluoQueryColumns.JOIN_PARENT_NODE_ID, metadata.getParentNodeId() );
        tx.set(rowId, FluoQueryColumns.JOIN_LEFT_CHILD_NODE_ID, metadata.getLeftChildNodeId() );
        tx.set(rowId, FluoQueryColumns.JOIN_BATCH_SIZE, Integer.toString(metadata.getJoinBatchSize()));
        tx.set(rowId, FluoQueryColumns.JOIN_RIGHT_CHILD_NODE_ID, metadata.getRightChildNodeId() );
    }

    /**
     * Read an instance of {@link JoinMetadata} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata. (not null)
     * @param nodeId - The nodeId of the Join node that will be read. (not nul)
     * @return The {@link JoinMetadata} that was read from the table.
     */
    public JoinMetadata readJoinMetadata(final SnapshotBase sx, final String nodeId) {
        return readJoinMetadataBuilder(sx, nodeId).build();
    }

    private JoinMetadata.Builder readJoinMetadataBuilder(final SnapshotBase sx, final String nodeId) {
        requireNonNull(sx);
        requireNonNull(nodeId);

        // Fetch the values from the Fluo table.
        final String rowId = nodeId;
        final Map<Column, String> values = sx.gets(rowId,
                FluoQueryColumns.JOIN_VARIABLE_ORDER,
                FluoQueryColumns.JOIN_TYPE,
                FluoQueryColumns.JOIN_PARENT_NODE_ID,
                FluoQueryColumns.JOIN_LEFT_CHILD_NODE_ID,
                FluoQueryColumns.JOIN_BATCH_SIZE,
                FluoQueryColumns.JOIN_RIGHT_CHILD_NODE_ID);

        // Return an object holding them.
        final String varOrderString = values.get(FluoQueryColumns.JOIN_VARIABLE_ORDER);
        final VariableOrder varOrder = new VariableOrder(varOrderString);

        final String joinTypeString = values.get(FluoQueryColumns.JOIN_TYPE);
        final JoinType joinType = JoinType.valueOf(joinTypeString);

        final String parentNodeId = values.get(FluoQueryColumns.JOIN_PARENT_NODE_ID);
        final String leftChildNodeId = values.get(FluoQueryColumns.JOIN_LEFT_CHILD_NODE_ID);
        final int joinBatchSize = Integer.parseInt(values.get(FluoQueryColumns.JOIN_BATCH_SIZE));
        final String rightChildNodeId = values.get(FluoQueryColumns.JOIN_RIGHT_CHILD_NODE_ID);

        return JoinMetadata.builder(nodeId)
                .setVarOrder(varOrder)
                .setJoinType(joinType)
                .setParentNodeId(parentNodeId)
                .setJoinBatchSize(joinBatchSize)
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
        requireNonNull(tx);
        requireNonNull(metadata);

        final String rowId = metadata.getNodeId();
        tx.set(rowId, FluoQueryColumns.STATEMENT_PATTERN_NODE_ID, rowId);
        tx.set(rowId, FluoQueryColumns.STATEMENT_PATTERN_VARIABLE_ORDER, metadata.getVariableOrder().toString());
        tx.set(rowId, FluoQueryColumns.STATEMENT_PATTERN_PATTERN, metadata.getStatementPattern() );
        tx.set(rowId, FluoQueryColumns.STATEMENT_PATTERN_PARENT_NODE_ID, metadata.getParentNodeId());
    }

    /**
     * Read an instance of {@link StatementPatternMetadata} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata. (not null)
     * @param nodeId - The nodeId of the Statement Pattern node that will be read. (not nul)
     * @return The {@link StatementPatternMetadata} that was read from the table.
     */
    public StatementPatternMetadata readStatementPatternMetadata(final SnapshotBase sx, final String nodeId) {
        return readStatementPatternMetadataBuilder(sx, nodeId).build();
    }

    private StatementPatternMetadata.Builder readStatementPatternMetadataBuilder(final SnapshotBase sx, final String nodeId) {
        requireNonNull(sx);
        requireNonNull(nodeId);

        // Fetch the values from the Fluo table.
        final String rowId = nodeId;
        final Map<Column, String> values = sx.gets(rowId,
                FluoQueryColumns.STATEMENT_PATTERN_VARIABLE_ORDER,
                FluoQueryColumns.STATEMENT_PATTERN_PATTERN,
                FluoQueryColumns.STATEMENT_PATTERN_PARENT_NODE_ID);

        // Return an object holding them.
        final String varOrderString = values.get(FluoQueryColumns.STATEMENT_PATTERN_VARIABLE_ORDER);
        final VariableOrder varOrder = new VariableOrder(varOrderString);

        final String pattern = values.get(FluoQueryColumns.STATEMENT_PATTERN_PATTERN);
        final String parentNodeId = values.get(FluoQueryColumns.STATEMENT_PATTERN_PARENT_NODE_ID);

        return StatementPatternMetadata.builder(nodeId).setVarOrder(varOrder).setStatementPattern(pattern).setParentNodeId(parentNodeId);
    }


    /**
     * Write an instance of {@link AggregationMetadata} to the Fluo table.
     *
     * @param tx - The transaction that will be used to commit the metadata. (not null)
     * @param metadata - The Aggregation node metadata that will be written to the table. (not null)
     */
    public void write(final TransactionBase tx, final AggregationMetadata metadata) {
        requireNonNull(tx);
        requireNonNull(metadata);

        final String rowId = metadata.getNodeId();
        tx.set(rowId, FluoQueryColumns.AGGREGATION_NODE_ID, rowId);
        tx.set(rowId, FluoQueryColumns.AGGREGATION_VARIABLE_ORDER, metadata.getVariableOrder().toString());
        tx.set(rowId, FluoQueryColumns.AGGREGATION_PARENT_NODE_ID, metadata.getParentNodeId());
        tx.set(rowId, FluoQueryColumns.AGGREGATION_CHILD_NODE_ID, metadata.getChildNodeId());

        // Store the Group By variable order.
        final VariableOrder groupByVars = metadata.getGroupByVariableOrder();
        final String groupByString = Joiner.on(";").join(groupByVars.getVariableOrders());
        tx.set(rowId, FluoQueryColumns.AGGREGATION_GROUP_BY_BINDING_NAMES, groupByString);

        // Serialize the collection of AggregationElements.
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try(final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject( metadata.getAggregations() );
        } catch (final IOException e) {
            throw new RuntimeException("Problem encountered while writing AggregationMetadata to the Fluo table. Unable " +
                    "to serialize the AggregationElements to a byte[].", e);
        }
        tx.set(Bytes.of(rowId.getBytes(Charsets.UTF_8)), FluoQueryColumns.AGGREGATION_AGGREGATIONS, Bytes.of(baos.toByteArray()));
    }

    /**
     * Read an instance of {@link AggregationMetadata} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata. (not null)
     * @param nodeId - The nodeId of the Aggregation node that will be read. (not null)
     * @return The {@link AggregationMetadata} that was read from the table.
     */
    public AggregationMetadata readAggregationMetadata(final SnapshotBase sx, final String nodeId) {
        return readAggregationMetadataBuilder(sx, nodeId).build();
    }

    private AggregationMetadata.Builder readAggregationMetadataBuilder(final SnapshotBase sx, final String nodeId) {
        requireNonNull(sx);
        requireNonNull(nodeId);

        // Fetch the values from the Fluo table.
        final String rowId = nodeId;
        final Map<Column, String> values = sx.gets(rowId,
                FluoQueryColumns.AGGREGATION_VARIABLE_ORDER,
                FluoQueryColumns.AGGREGATION_PARENT_NODE_ID,
                FluoQueryColumns.AGGREGATION_CHILD_NODE_ID,
                FluoQueryColumns.AGGREGATION_GROUP_BY_BINDING_NAMES);


        // Return an object holding them.
        final String varOrderString = values.get(FluoQueryColumns.AGGREGATION_VARIABLE_ORDER);
        final VariableOrder varOrder = new VariableOrder(varOrderString);

        final String parentNodeId = values.get(FluoQueryColumns.AGGREGATION_PARENT_NODE_ID);
        final String childNodeId = values.get(FluoQueryColumns.AGGREGATION_CHILD_NODE_ID);

        // Read the Group By variable order if one was present.
        final String groupByString = values.get(FluoQueryColumns.AGGREGATION_GROUP_BY_BINDING_NAMES);
        final VariableOrder groupByVars = groupByString.isEmpty() ? new VariableOrder() : new VariableOrder( groupByString.split(";") );

        // Deserialize the collection of AggregationElements.
        final Bytes aggBytes = sx.get(Bytes.of(nodeId.getBytes(Charsets.UTF_8)), FluoQueryColumns.AGGREGATION_AGGREGATIONS);
        final Collection<AggregationElement> aggregations;
        try(final ObjectInputStream ois = new ObjectInputStream(aggBytes.toInputStream())) {
             aggregations = (Collection<AggregationElement>)ois.readObject();
        } catch (final IOException | ClassNotFoundException e) {
            throw new RuntimeException("Problem encountered while reading AggregationMetadata from the Fluo table. Unable " +
                    "to deserialize the AggregationElements from a byte[].", e);
        }

        final AggregationMetadata.Builder builder = AggregationMetadata.builder(nodeId)
                .setVarOrder(varOrder)
                .setParentNodeId(parentNodeId)
                .setChildNodeId(childNodeId)
                .setGroupByVariableOrder(groupByVars);

        for(final AggregationElement aggregation : aggregations) {
            builder.addAggregation(aggregation);
        }

        return builder;
    }

    /**
     * Write an instance of {@link FluoQuery} to the Fluo table.
     *
     * @param tx - The transaction that will be used to commit the metadata. (not null)
     * @param query - The query metadata that will be written to the table. (not null)
     */
    public void write(final TransactionBase tx, final FluoQuery query) {
        requireNonNull(tx);
        requireNonNull(query);

        // The results of the query are eventually exported to an instance
        // of Rya, so store the Rya ID for the PCJ.
        write(tx, query.getQueryMetadata());

        // Write the rest of the metadata objects.

        if (query.getQueryType() == QueryType.CONSTRUCT) {
            final ConstructQueryMetadata constructMetadata = query.getConstructQueryMetadata().get();
            write(tx, constructMetadata);
        }

        for(final ProjectionMetadata projection : query.getProjectionMetadata()) {
            write(tx, projection);
        }

        final Optional<PeriodicQueryMetadata> periodicMetadata = query.getPeriodicQueryMetadata();
        if(periodicMetadata.isPresent()) {
            write(tx, periodicMetadata.get());
        }

        for(final FilterMetadata filter : query.getFilterMetadata()) {
            write(tx, filter);
        }

        for(final JoinMetadata join : query.getJoinMetadata()) {
            write(tx, join);
        }

        Set<String> ids = new HashSet<>();
        for(final StatementPatternMetadata statementPattern : query.getStatementPatternMetadata()) {
            write(tx, statementPattern);
            ids.add(statementPattern.getNodeId());
        }
        StatementPatternIdManager.addStatementPatternIds(tx, Sets.newHashSet(ids));

        for(final AggregationMetadata aggregation : query.getAggregationMetadata()) {
            write(tx, aggregation);
        }
    }


    /**
     * Read an instance of {@link FluoQuery} from the Fluo table.
     *
     * @param sx - The snapshot that will be used to read the metadata from the Fluo table. (not null)
     * @param queryId - The ID of the query whose nodes will be read. (not null)
     * @return The {@link FluoQuery} that was read from table.
     * @throws UnsupportedQueryException
     */
    public FluoQuery readFluoQuery(final SnapshotBase sx, final String queryId) throws UnsupportedQueryException {
        requireNonNull(sx);
        requireNonNull(queryId);

        final FluoQuery.Builder fluoQueryBuilder = FluoQuery.builder();
        addChildMetadata(sx, fluoQueryBuilder, queryId);
        return fluoQueryBuilder.build();
    }

    private void addChildMetadata(final SnapshotBase sx, final FluoQuery.Builder builder, final String childNodeId) {
        requireNonNull(sx);
        requireNonNull(builder);
        requireNonNull(childNodeId);

        final NodeType childType = NodeType.fromNodeId(childNodeId).get();
        switch (childType) {
        case QUERY:
            // Add this node's metadata.
            final QueryMetadata.Builder queryBuilder = readQueryMetadataBuilder(sx, childNodeId);
            builder.setQueryMetadata(queryBuilder);

            // Add it's child's metadata.
            addChildMetadata(sx, builder, queryBuilder.build().getChildNodeId());
            break;

        case PROJECTION:
            //Add this node's metadata
            final ProjectionMetadata.Builder projectionBuilder = readProjectionMetadataBuilder(sx, childNodeId);
            builder.addProjectionBuilder(projectionBuilder);

            //Add it's child's metadata
            addChildMetadata(sx, builder, projectionBuilder.build().getChildNodeId());
            break;

        case CONSTRUCT:
            final ConstructQueryMetadata.Builder constructBuilder = readConstructQueryMetadataBuilder(sx, childNodeId);
            builder.setConstructQueryMetadata(constructBuilder);

            // Add it's child's metadata.
            addChildMetadata(sx, builder, constructBuilder.build().getChildNodeId());
            break;

        case PERIODIC_QUERY:
            // Add this node's metadata.
            final PeriodicQueryMetadata.Builder periodicQueryBuilder = readPeriodicQueryMetadataBuilder(sx, childNodeId);
            builder.addPeriodicQueryMetadata(periodicQueryBuilder);

            // Add it's child's metadata.
            addChildMetadata(sx, builder, periodicQueryBuilder.build().getChildNodeId());
            break;

        case AGGREGATION:
            // Add this node's metadata.
            final AggregationMetadata.Builder aggregationBuilder = readAggregationMetadataBuilder(sx, childNodeId);
            builder.addAggregateMetadata(aggregationBuilder);

            // Add it's child's metadata.
            addChildMetadata(sx, builder, aggregationBuilder.build().getChildNodeId());
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
        default:
            break;

        }
    }
}