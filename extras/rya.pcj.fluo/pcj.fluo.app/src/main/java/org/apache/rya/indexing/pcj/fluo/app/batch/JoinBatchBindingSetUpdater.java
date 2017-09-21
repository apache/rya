package org.apache.rya.indexing.pcj.fluo.app.batch;

import static com.google.common.base.Optional.fromNullable;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.IterativeJoin;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.LeftOuterJoin;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.NaturalJoin;
import org.apache.rya.indexing.pcj.fluo.app.JoinResultUpdater.Side;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation.Task;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.util.AggregationStateManager;
import org.apache.rya.indexing.pcj.fluo.app.util.RowKeyUtil;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetSerDe;

import com.google.common.base.Preconditions;

/**
 * Performs updates to BindingSets in the JoinBindingSet column in batch fashion.
 */
public class JoinBatchBindingSetUpdater extends AbstractBatchBindingSetUpdater {

    private static final Logger log = Logger.getLogger(JoinBatchBindingSetUpdater.class);
    private static final VisibilityBindingSetSerDe BS_SERDE = new VisibilityBindingSetSerDe();
    private static final FluoQueryMetadataDAO dao = new FluoQueryMetadataDAO();

    /**
     * Processes {@link JoinBatchInformation}. Updates the BindingSets associated with the specified nodeId. The
     * BindingSets are processed in batch fashion, where the number of results is indicated by
     * {@link JoinBatchInformation#getBatchSize()}. BindingSets are either Added, Deleted, or Updated according to
     * {@link JoinBatchInformation#getTask()}. In the event that the number of entries that need to be updated exceeds
     * the batch size, the row of the first unprocessed BindingSets is used to create a new JoinBatch job to process the
     * remaining BindingSets.
     * 
     * @throws Exception
     */
    @Override
    public void processBatch(TransactionBase tx, Bytes row, BatchInformation batch) throws Exception {
        super.processBatch(tx, row, batch);
        String nodeId = BatchRowKeyUtil.getNodeId(row);
        Preconditions.checkArgument(batch instanceof JoinBatchInformation);
        JoinBatchInformation joinBatch = (JoinBatchInformation) batch;
        Task task = joinBatch.getTask();
        VisibilityBindingSet bs = joinBatch.getBs();

        // create aggregation state manager is aggregation state metadata exists
        Optional<AggregationStateManager> stateManager = Optional.empty();
        if (joinBatch.getAggregationStateMeta().isPresent()) {
            stateManager = Optional.of(new AggregationStateManager(tx, joinBatch.getAggregationStateMeta().get()));
        }
        //if no state is present or childBindingSet satisfies aggregation state, then proceed with join
        if (!stateManager.isPresent() || stateManager.get().checkAggregationState(bs)) {
            // Figure out which join algorithm we are going to use.
            final IterativeJoin joinAlgorithm;
            switch (joinBatch.getJoinType()) {
            case NATURAL_JOIN:
                joinAlgorithm = new NaturalJoin();
                break;
            case LEFT_OUTER_JOIN:
                joinAlgorithm = new LeftOuterJoin();
                break;
            default:
                throw new RuntimeException("Unsupported JoinType: " + joinBatch.getJoinType());
            }

            Set<VisibilityBindingSet> bsSet = new HashSet<>();
            Optional<RowColumn> rowCol = fillSiblingBatch(tx, joinBatch, bsSet);

            // Iterates over the resulting BindingSets from the join.
            final Iterator<VisibilityBindingSet> newJoinResults;
            if (joinBatch.getSide() == Side.LEFT) {
                newJoinResults = joinAlgorithm.newLeftResult(bs, bsSet.iterator(), fromNullable(stateManager.orElse(null)));
            } else {
                newJoinResults = joinAlgorithm.newRightResult(bsSet.iterator(), bs, fromNullable(stateManager.orElse(null)));
            }

            // Insert the new join binding sets to the Fluo table.
            final JoinMetadata joinMetadata = dao.readJoinMetadata(tx, nodeId);
            final VariableOrder joinVarOrder = joinMetadata.getVariableOrder();
            while (newJoinResults.hasNext()) {
                final VisibilityBindingSet newJoinResult = newJoinResults.next();
                // create BindingSet value
                Bytes bsBytes = BS_SERDE.serialize(newJoinResult);
                // make rowId
                Bytes rowKey = RowKeyUtil.makeRowKey(nodeId, joinVarOrder, newJoinResult);
                final Column col = FluoQueryColumns.JOIN_BINDING_SET;
                processTask(tx, task, rowKey, col, bsBytes);
            }

            // if batch limit met, there are additional entries to process
            // update the span and register updated batch job
            if (rowCol.isPresent()) {
                Span newSpan = getNewSpan(rowCol.get(), joinBatch.getSpan());
                joinBatch.setSpan(newSpan);
                BatchInformationDAO.addBatch(tx, nodeId, joinBatch);
            }
        }
    }

    private void processTask(TransactionBase tx, Task task, Bytes row, Column column, Bytes value) {
        switch (task) {
        case Add:
            tx.set(row, column, value);
            break;
        case Delete:
            tx.delete(row, column);
            break;
        case Update:
            log.trace("The Task Update is not supported for JoinBatchBindingSetUpdater.  Batch will not be processed.");
            break;
        default:
            log.trace("Invalid Task type.  Aborting batch operation.");
            break;
        }
    }

    /**
     * Fetches batch to be processed by scanning over the Span specified by the {@link JoinBatchInformation}. The number
     * of results is less than or equal to the batch size specified by the JoinBatchInformation.
     * 
     * @param tx - Fluo transaction in which batch operation is performed
     * @param batch - batch order to be processed
     * @param bsSet- set that batch results are added to
     * @return Set - containing results of sibling scan.
     * @throws Exception
     */
    private Optional<RowColumn> fillSiblingBatch(TransactionBase tx, JoinBatchInformation batch, Set<VisibilityBindingSet> bsSet)
            throws Exception {

        Span span = batch.getSpan();
        Column column = batch.getColumn();
        int batchSize = batch.getBatchSize();

        RowScanner rs = tx.scanner().over(span).fetch(column).byRow().build();
        Iterator<ColumnScanner> colScannerIter = rs.iterator();

        boolean batchLimitMet = false;
        Bytes row = span.getStart().getRow();
        while (colScannerIter.hasNext() && !batchLimitMet) {
            ColumnScanner colScanner = colScannerIter.next();
            row = colScanner.getRow();
            Iterator<ColumnValue> iter = colScanner.iterator();
            while (iter.hasNext()) {
                if (bsSet.size() >= batchSize) {
                    batchLimitMet = true;
                    break;
                }
                bsSet.add(BS_SERDE.deserialize(iter.next().getValue()));
            }
        }

        if (batchLimitMet) {
            return Optional.of(new RowColumn(row, column));
        } else {
            return Optional.empty();
        }
    }
}
