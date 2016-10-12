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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.query.FilterMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.QueryMetadata;
import org.openrdf.query.BindingSet;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;

/**
 * Deletes a Pre-computed Join (PCJ) from Fluo.
 * <p>
 * This is a two phase process.
 * <ol>
 *   <li>Delete metadata about each node of the query using a single Fluo
 *       transaction. This prevents new {@link BindingSet}s from being created when
 *       new triples are inserted.</li>
 *   <li>Delete BindingSets associated with each node of the query. This is done
 *       in a batch fashion to guard against large delete transactions that don't fit
 *       into memory.</li>
 * </ol>
 */
@ParametersAreNonnullByDefault
public class DeletePcj {

    private final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();
    private final int batchSize;

    /**
     * Constructs an instance of {@link DeletePcj}.
     *
     * @param batchSize - The number of entries that will be deleted at a time. (> 0)
     */
    public DeletePcj(final int batchSize) {
        checkArgument(batchSize > 0);
        this.batchSize = batchSize;
    }

    /**
     * Deletes all metadata and {@link BindingSet}s associated with a Rya
     * Precomputed Join Index from the Fluo application that is incrementally
     * updating it.
     *
     * @param client - Connects to the Fluo application that is updating the PCJ Index. (not null)
     * @param pcjId - The PCJ ID for the query that will removed from the Fluo application. (not null)
     */
    public void deletePcj(final FluoClient client, final String pcjId) {
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
     */
    private List<String> getNodeIds(Transaction tx, String pcjId) {
        requireNonNull(tx);
        requireNonNull(pcjId);

        // Get the ID that tracks the query within the Fluo application.
        final String queryId = getQueryIdFromPcjId(tx, pcjId);

        // Get the query's children nodes.
        final List<String> nodeIds = new ArrayList<>();
        nodeIds.add(queryId);
        getChildNodeIds(tx, queryId, nodeIds);
        return nodeIds;
    }

    /**
     * Recursively navigate query tree to extract all of the nodeIds.
     *
     * @param tx - Transaction of a given Fluo table. (not null)
     * @param nodeId - Current node in query tree. (not null)
     * @param nodeIds - The Node IDs extracted from query tree. (not null)
     */
    private void getChildNodeIds(final Transaction tx, final String nodeId, final List<String> nodeIds) {
        requireNonNull(tx);
        requireNonNull(nodeId);
        requireNonNull(nodeIds);

        final NodeType type = NodeType.fromNodeId(nodeId).get();
        switch (type) {
            case QUERY:
                final QueryMetadata queryMeta = dao.readQueryMetadata(tx, nodeId);
                final String queryChild = queryMeta.getChildNodeId();
                nodeIds.add(queryChild);
                getChildNodeIds(tx, queryChild, nodeIds);
                break;
            case JOIN:
                final JoinMetadata joinMeta = dao.readJoinMetadata(tx, nodeId);
                final String lchild = joinMeta.getLeftChildNodeId();
                final String rchild = joinMeta.getRightChildNodeId();
                nodeIds.add(lchild);
                nodeIds.add(rchild);
                getChildNodeIds(tx, lchild, nodeIds);
                getChildNodeIds(tx, rchild, nodeIds);
                break;
            case FILTER:
                final FilterMetadata filterMeta = dao.readFilterMetadata(tx, nodeId);
                final String filterChild = filterMeta.getChildNodeId();
                nodeIds.add(filterChild);
                getChildNodeIds(tx, filterChild, nodeIds);
                break;
            case STATEMENT_PATTERN:
                break;
        }
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
            deletePcjIdAndSparqlMetadata(typeTx, pcjId);

            for (final String nodeId : nodeIds) {
                final NodeType type = NodeType.fromNodeId(nodeId).get();
                deleteMetadataColumns(typeTx, nodeId, type.getMetaDataColumns());
            }
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
     * Deletes high level query meta for converting from queryId to pcjId and vice
     * versa, as well as converting from sparql to queryId.
     *
     * @param tx - Transaction the deletes will be performed with. (not null)
     * @param pcjId - The PCJ whose metadata will be deleted. (not null)
     */
    private void deletePcjIdAndSparqlMetadata(final Transaction tx, final String pcjId) {
        requireNonNull(tx);
        requireNonNull(pcjId);

        final String queryId = getQueryIdFromPcjId(tx, pcjId);
        final String sparql = getSparqlFromQueryId(tx, queryId);
        tx.delete(queryId, FluoQueryColumns.RYA_PCJ_ID);
        tx.delete(sparql, FluoQueryColumns.QUERY_ID);
        tx.delete(pcjId, FluoQueryColumns.PCJ_ID_QUERY_ID);
    }


    /**
     * Deletes all BindingSets associated with the specified nodeId.
     *
     * @param nodeId - nodeId whose {@link BindingSet}s will be deleted. (not null)
     * @param client - Used to delete the data. (not null)
     */
    private void deleteData(final FluoClient client, final String nodeId) {
        requireNonNull(client);
        requireNonNull(nodeId);

        final NodeType type = NodeType.fromNodeId(nodeId).get();
        Transaction tx = client.newTransaction();
        while(deleteDataBatch(tx, getIterator(tx, nodeId, type.getBsColumn()), type.getBsColumn())) {
            tx = client.newTransaction();
        }
    }

    private CellScanner getIterator(final Transaction tx, final String nodeId, final Column column) {
        requireNonNull(tx);
        requireNonNull(nodeId);
        requireNonNull(column);

        return tx.scanner().fetch(column).over(Span.prefix(nodeId)).build();
    }

    private boolean deleteDataBatch(final Transaction tx, final CellScanner scanner, final Column column) {
        requireNonNull(tx);
        requireNonNull(scanner);
        requireNonNull(column);

        try(Transaction ntx = tx) {
          int count = 0;
          Iterator<RowColumnValue> iter = scanner.iterator();
          while (iter.hasNext() && count < batchSize) {
            final Bytes row = iter.next().getRow();
            count++;
            tx.delete(row, column);
          }

          final boolean hasNext = iter.hasNext();
          tx.commit();
          return hasNext;
        }
    }

    private String getQueryIdFromPcjId(final Transaction tx, final String pcjId) {
        requireNonNull(tx);
        requireNonNull(pcjId);

        final Bytes queryIdBytes = tx.get(Bytes.of(pcjId), FluoQueryColumns.PCJ_ID_QUERY_ID);
        return queryIdBytes.toString();
    }

    private String getSparqlFromQueryId(final Transaction tx, final String queryId) {
        requireNonNull(tx);
        requireNonNull(queryId);

        final QueryMetadata metadata = dao.readQueryMetadata(tx, queryId);
        return metadata.getSparql();
    }
}
