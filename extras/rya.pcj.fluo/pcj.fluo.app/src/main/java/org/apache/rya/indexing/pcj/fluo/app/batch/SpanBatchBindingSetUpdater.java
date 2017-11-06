package org.apache.rya.indexing.pcj.fluo.app.batch;

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
import java.util.Iterator;
import java.util.Optional;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.BindingSetRow;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation.Task;

import com.google.common.base.Preconditions;

/**
 * This class processes {@link SpanBatchDeleteInformation} objects by deleting the entries in the Fluo Column
 * corresponding to the {@link Span} of the BatchInformation object. This class will delete entries until the batch size
 * is met, and then create a new SpanBatchDeleteInformation object with an updated Span whose starting point is the
 * stopping point of this batch. If the batch limit is not met, then a new batch is not created and the task is
 * complete.
 *
 */
public class SpanBatchBindingSetUpdater extends AbstractBatchBindingSetUpdater {

    private static final Logger log = Logger.getLogger(SpanBatchBindingSetUpdater.class);

    /**
     * Process SpanBatchDeleteInformation objects by deleting all entries indicated by Span until batch limit is met.
     *
     * @param tx - Fluo Transaction
     * @param row - Byte row identifying BatchInformation
     * @param batch - SpanBatchDeleteInformation object to be processed
     */
    @Override
    public void processBatch(TransactionBase tx, Bytes row, BatchInformation batch) throws Exception {
        super.processBatch(tx, row, batch);
        Preconditions.checkArgument(batch instanceof SpanBatchDeleteInformation);
        SpanBatchDeleteInformation spanBatch = (SpanBatchDeleteInformation) batch;
        Optional<String> nodeId = spanBatch.getNodeId();
        Task task = spanBatch.getTask();
        int batchSize = spanBatch.getBatchSize();
        Span span = spanBatch.getSpan();
        Column column = batch.getColumn();
        Optional<RowColumn> rowCol = Optional.empty();

        switch (task) {
        case Add:
            log.trace("The Task Add is not supported for SpanBatchBindingSetUpdater.  Batch " + batch + " will not be processed.");
            break;
        case Delete:
            rowCol = deleteBatch(tx, nodeId, span, column, batchSize);
            break;
        case Update:
            log.trace("The Task Update is not supported for SpanBatchBindingSetUpdater.  Batch " + batch + " will not be processed.");
            break;
        default:
            log.trace("Invalid Task type.  Aborting batch operation.");
            break;
        }

        if (rowCol.isPresent()) {
            Span newSpan = getNewSpan(rowCol.get(), spanBatch.getSpan());
            log.trace("Batch size met.  There are remaining results that need to be deleted.  Creating a new batch of size: "
                    + spanBatch.getBatchSize() + " with Span: " + newSpan + " and Column: " + column);
            spanBatch.setSpan(newSpan);
            BatchInformationDAO.addBatch(tx, BatchRowKeyUtil.getNodeId(row), spanBatch);
        }
    }

    private Optional<RowColumn> deleteBatch(TransactionBase tx, Optional<String> nodeId, Span span, Column column, int batchSize) {

        log.trace("Deleting batch of size: " + batchSize + " using Span: " + span + " and Column: " + column);
        RowScanner rs = tx.scanner().over(span).fetch(column).byRow().build();
        try {
            Iterator<ColumnScanner> colScannerIter = rs.iterator();

            int count = 0;
            boolean batchLimitMet = false;
            Bytes row = span.getStart().getRow();

            //get prefix if nodeId is specified
            Optional<Bytes> prefixBytes = Optional.empty();
            if (nodeId.isPresent()) {
                NodeType type = NodeType.fromNodeId(nodeId.get()).get();
                prefixBytes = Optional.ofNullable(Bytes.of(type.getNodeTypePrefix()));
            }

            while (colScannerIter.hasNext() && !batchLimitMet) {
                ColumnScanner colScanner = colScannerIter.next();
                row = colScanner.getRow();

                //extract the nodeId from the returned row if a nodeId was passed
                //into the SpanBatchInformation.  This is to ensure that the returned
                //row nodeId is equal to the nodeId passed in to the span batch information
                Optional<String> rowNodeId = Optional.empty();
                if (prefixBytes.isPresent()) {
                    rowNodeId = Optional.of(BindingSetRow.makeFromShardedRow(prefixBytes.get(), row).getNodeId());
                }

                //if nodeId is present, then results returned by span are filtered
                //on the nodeId.  This occurs when the hash is not included in the span
                if (!rowNodeId.isPresent() || rowNodeId.equals(nodeId)) {
                    Iterator<ColumnValue> iter = colScanner.iterator();
                    while (iter.hasNext()) {
                        if (count >= batchSize) {
                            batchLimitMet = true;
                            break;
                        }
                        ColumnValue colVal = iter.next();
                        tx.delete(row, colVal.getColumn());
                        count++;
                    }
                }
            }

            if (batchLimitMet) {
                return Optional.of(new RowColumn(row));
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

}
