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
import org.apache.rya.indexing.pcj.fluo.app.batch.serializer.BatchInformationSerializer;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;

/**
 * Class used for reading and writing {@link BatchInformation}.
 *
 */
public class BatchInformationDAO {
    
    /**
     * Adds BatchInformation to the {@link FluoQueryColumns#BATCH_COLUMN}.
     * @param tx - Fluo Transaction
     * @param nodeId - query node that batch task will be performed on
     * @param batch - BatchInformation to be processed
     */
    public static void addBatch(TransactionBase tx, String nodeId, BatchInformation batch) {
        Bytes row = BatchRowKeyUtil.getRow(nodeId);
        tx.set(row, FluoQueryColumns.BATCH_COLUMN, Bytes.of(BatchInformationSerializer.toBytes(batch)));
    }
    
    /**
     * Retrieve BatchInformation
     * @param tx - Fluo transaction
     * @param row - row that contains batch information - this is the query id that batch task will be performed on
     * @return Optional contained the BatchInformation if it is there
     */
    public static Optional<BatchInformation> getBatchInformation(TransactionBase tx, Bytes row) {
        Bytes val = tx.get(row, FluoQueryColumns.BATCH_COLUMN);
        if(val != null) {
            return BatchInformationSerializer.fromBytes(val.toArray());
        } else {
            return Optional.empty();
        }
    }
    
}
