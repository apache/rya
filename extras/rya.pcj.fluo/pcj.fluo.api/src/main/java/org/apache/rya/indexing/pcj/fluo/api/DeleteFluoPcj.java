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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformationDAO;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchDeleteInformation;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternIdManager;
import org.apache.rya.indexing.pcj.fluo.app.query.UnsupportedQueryException;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoQueryUtils;
import org.eclipse.rdf4j.query.BindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Deletes a Pre-computed Join (PCJ) from Fluo.
 * <p>
 * This is a two phase process.
 * <ol>
 * <li>Delete metadata about each node of the query using a single Fluo
 * transaction. This prevents new {@link BindingSet}s from being created when
 * new triples are inserted.</li>
 * <li>Delete BindingSets associated with each node of the query. This is done
 * in a batch fashion to guard against large delete transactions that don't fit
 * into memory.</li>
 * </ol>
 */
@DefaultAnnotation(NonNull.class)
public class DeleteFluoPcj {

    private final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();
    private final int batchSize;

    /**
     * Constructs an instance of {@link DeleteFluoPcj}.
     *
     * @param batchSize - The number of entries that will be deleted at a time. (> 0)
     */
    public DeleteFluoPcj(final int batchSize) {
        checkArgument(batchSize > 0);
        this.batchSize = batchSize;
    }

    /**
     * Deletes all metadata and {@link BindingSet}s associated with a Rya
     * Precomputed Join Index from the Fluo application that is incrementally
     * updating it.
     *
     * @param client - Connects to the Fluo application that is updating the PCJ
     *            Index. (not null)
     * @param pcjId - The PCJ ID for the query that will removed from the Fluo
     *            application. (not null)
     * @throws UnsupportedQueryException - thrown when Fluo app is unable to read FluoQuery associated
     * with given pcjId.
     */
    public void deletePcj(final FluoClient client, final String pcjId) throws UnsupportedQueryException {
        requireNonNull(client);
        requireNonNull(pcjId);

        final Transaction tx = client.newTransaction();

        // Delete the query's metadata. This halts input.
        final List<String> nodeIds = getNodeIds(tx, pcjId);
        deleteMetadata(tx, nodeIds, pcjId);

        // Delete the binding sets associated with the query's nodes.
        for (final String nodeId : nodeIds) {
            deleteData(client, nodeId);
        }
    }

    /**
     * This method retrieves all of the nodeIds that are part of the query with
     * specified pcjId.
     *
     * @param tx - Transaction of a given Fluo table. (not null)
     * @param pcjId - Id of query. (not null)
     * @return list of Node IDs associated with the query {@code pcjId}.
     * @throws UnsupportedQueryException - thrown when Fluo app is unable to read FluoQuery associated
     * with given pcjId.
     */
    private List<String> getNodeIds(Transaction tx, String pcjId) throws UnsupportedQueryException {
        requireNonNull(tx);
        requireNonNull(pcjId);

        String queryId = NodeType.generateNewIdForType(NodeType.QUERY, pcjId);
        FluoQuery fluoQuery = dao.readFluoQuery(tx, queryId);
        return FluoQueryUtils.collectNodeIds(fluoQuery);
    }


    /**
     * Deletes metadata for all nodeIds associated with a given queryId in a
     * single transaction. Prevents additional BindingSets from being created as
     * new triples are added.
     *
     * @param tx - Transaction of a given Fluo table. (not null)
     * @param nodeIds - Nodes whose metatdata will be deleted. (not null)
     * @param pcjId - The PCJ ID of the query whose will be deleted. (not null)
     */
    private void deleteMetadata(final Transaction tx, final List<String> nodeIds, final String pcjId) {
        requireNonNull(tx);
        requireNonNull(nodeIds);
        requireNonNull(pcjId);

        try (final Transaction typeTx = tx) {
            Set<String> spNodeIds = new HashSet<>();
            //remove metadata associated with each nodeId and store statement pattern nodeIds
            for (final String nodeId : nodeIds) {
                final NodeType type = NodeType.fromNodeId(nodeId).get();
                if(type == NodeType.STATEMENT_PATTERN) {
                    spNodeIds.add(nodeId);
                }
                deleteMetadataColumns(typeTx, nodeId, type.getMetaDataColumns());
            }
            //Use stored statement pattern nodeIds to update list of stored statement pattern nodeIds
            //in Fluo table
            StatementPatternIdManager.removeStatementPatternIds(typeTx, spNodeIds);
            typeTx.commit();
        }
    }

    /**
     * Deletes all metadata for a Query Node.
     *
     * @param tx - Transaction the deletes will be performed with. (not null)
     * @param nodeId - The Node ID of the query node to delete. (not null)
     * @param columns - The columns that will be deleted. (not null)
     */
    private void deleteMetadataColumns(final Transaction tx, final String nodeId, final List<Column> columns) {
        requireNonNull(tx);
        requireNonNull(columns);
        requireNonNull(nodeId);

        final Bytes row = Bytes.of(nodeId);
        for (final Column column : columns) {
            tx.delete(row, column);
        }
    }

    /**
     * Deletes all results (BindingSets or Statements) associated with the specified nodeId.
     *
     * @param nodeId - nodeId whose {@link BindingSet}s will be deleted. (not null)
     * @param client - Used to delete the data. (not null)
     */
    private void deleteData(final FluoClient client, final String nodeId) {
        requireNonNull(client);
        requireNonNull(nodeId);

        final NodeType type = NodeType.fromNodeId(nodeId).get();
        Transaction tx = client.newTransaction();
        Bytes prefixBytes = Bytes.of(type.getNodeTypePrefix());
        SpanBatchDeleteInformation batch = SpanBatchDeleteInformation.builder().setColumn(type.getResultColumn())
                .setSpan(Span.prefix(prefixBytes)).setBatchSize(batchSize).setNodeId(Optional.of(nodeId)).build();
        BatchInformationDAO.addBatch(tx, nodeId, batch);
        tx.commit();
    }

}
