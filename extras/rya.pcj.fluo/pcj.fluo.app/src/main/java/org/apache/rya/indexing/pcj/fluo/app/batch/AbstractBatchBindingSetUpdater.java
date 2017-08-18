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
package org.apache.rya.indexing.pcj.fluo.app.batch;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

/**
 * This class provides common functionality for implementations of {@link BatchBindingSetUpdater}.
 *
 */
public abstract class AbstractBatchBindingSetUpdater implements BatchBindingSetUpdater {

    /**
     * Updates the Span to create a new {@link BatchInformation} object to be fed to the
     * {@link BatchObserver}.  This message is called in the event that the BatchBindingSetUpdater
     * reaches the batch size before processing all entries relevant to its Span.
     * @param newStart - new start to the Span
     * @param oldSpan - old Span to be updated with newStart
     * @return - updated Span used with an updated BatchInformation object to complete the batch task
     */
    public static Span getNewSpan(RowColumn newStart, Span oldSpan) {
        return new Span(newStart, oldSpan.isStartInclusive(), oldSpan.getEnd(), oldSpan.isEndInclusive());
    }
    
    /**
     * Cleans up old batch job.  This method is meant to be called by any overriding method
     * to clean up old batch tasks.
     */
    @Override
    public void processBatch(TransactionBase tx, Bytes row, BatchInformation batch) throws Exception {
        tx.delete(row, FluoQueryColumns.BATCH_COLUMN);
    }


}
