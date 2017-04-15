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
import org.apache.fluo.api.data.Column;

import com.google.common.base.Preconditions;

/**
 * This class contains all of the common info contained in other implementations
 * of BatchInformation.
 *
 */
public abstract class BasicBatchInformation implements BatchInformation {
    
    private int batchSize;
    private Task task;
    private Column column;
    
    /**
     * Create BasicBatchInformation object
     * @param batchSize - size of batch to be processed
     * @param task - task to be processed
     * @param column - Column in which data is proessed
     */
    public BasicBatchInformation(int batchSize, Task task, Column column ) {
        this.task = Preconditions.checkNotNull(task);
        this.column = Preconditions.checkNotNull(column);
        Preconditions.checkArgument(batchSize > 0);
        this.batchSize = batchSize;
    }
    
    /**
     * Creates a BasicBatchInformation 
     * @param task
     */
    public BasicBatchInformation(Task task, Column column) {
        Preconditions.checkNotNull(task);
        Preconditions.checkNotNull(column);
        this.task = task;
        this.column = column;
        this.batchSize = DEFAULT_BATCH_SIZE;
    }

    /**
     * @return - size of batch that tasks are performed in
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * @return - type of Task performed (Add, Delete, Update)
     */
    public Task getTask() {
        return task;
    }
    
    /**
     * @return - Column in which Task will be performed
     */
    public Column getColumn() {
        return column;
    }
    
}
