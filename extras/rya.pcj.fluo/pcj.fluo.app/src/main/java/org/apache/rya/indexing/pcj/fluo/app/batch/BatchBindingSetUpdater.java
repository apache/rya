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
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;

/**
 * Interface for applying batch updates to the Fluo table based on the provided {@link BatchInformation}.
 * This updater is used by the {@link BatchObserver} to apply batch updates to overcome the restriction
 * that all transactions are processed in memory.  This allows Observers process potentially large 
 * tasks that cannot fit into memory in a piece-wise, batch fashion.
 */
public interface BatchBindingSetUpdater {

    /**
     * Processes the {@link BatchInformation} object.  The BatchInformation will
     * typically include a Task (either Add, Update, or Delete), along with information
     * about the starting point to begin processing data.
     * @param tx - Fluo Transaction
     * @param row - contains the ID of the Fluo node to be processed
     * @param batch - contains info about which cells for the Fluo query result node
     * should be processed
     * @throws Exception
     */
    public void processBatch(TransactionBase tx, Bytes row, BatchInformation batch) throws Exception;
    
}
