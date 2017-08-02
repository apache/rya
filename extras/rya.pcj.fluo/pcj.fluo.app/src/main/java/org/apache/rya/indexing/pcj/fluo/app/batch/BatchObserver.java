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
import java.util.Optional;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.rya.indexing.pcj.fluo.app.NodeType;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

/**
 * BatchObserver processes tasks that need to be broken into batches. Entries
 * stored stored in this {@link FluoQueryColumns#BATCH_COLUMN} are of the form
 * Row: nodeId, Value: BatchInformation. The nodeId indicates the node that the
 * batch operation will be performed on. All batch operations are performed on
 * the bindingSet column for the {@link NodeType} corresponding to the given
 * nodeId. For example, if the nodeId indicated that the NodeType was
 * StatementPattern, then the batch operation would be performed on
 * {@link FluoQueryColumns#STATEMENT_PATTERN_BINDING_SET}. This Observer applies
 * batch updates to overcome the restriction that all Fluo transactions are processed
 * in memory. This allows the Rya Fluo application to process large tasks that cannot
 * fit into memory in a piece-wise, batch fashion.
 */
public class BatchObserver extends AbstractObserver {

    /**
     * Processes the BatchInformation objects when they're written to the Batch column
     * @param tx - Fluo transaction 
     * @param row - row that contains {@link BatchInformation}
     * @param col - column that contains BatchInformation
     */
    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
        Optional<BatchInformation> batchInfo = BatchInformationDAO.getBatchInformation(tx, row);
        if(batchInfo.isPresent()) {
            batchInfo.get().getBatchUpdater().processBatch(tx, row, batchInfo.get());
        }
    }

    @Override
    public ObservedColumn getObservedColumn() {
        return new ObservedColumn(FluoQueryColumns.BATCH_COLUMN, NotificationType.STRONG);
    }

}
