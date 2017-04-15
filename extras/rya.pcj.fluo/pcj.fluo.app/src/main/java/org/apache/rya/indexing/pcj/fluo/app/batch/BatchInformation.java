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

/**
 * Interface for submitting batch Fluo tasks to be processed by the
 * {@link BatchObserver}. The BatchObserver applies batch updates to overcome
 * the restriction that all Fluo transactions are processed in memory. This
 * allows the Rya Fluo application to process large tasks that cannot fit into
 * memory in a piece-wise, batch fashion.
 */
public interface BatchInformation {

    public static enum Task {Add, Delete, Update}
    public static int DEFAULT_BATCH_SIZE = 5000;
    
    /**
     * @return batchsize of task 
     */
    public int getBatchSize();
    
    /**
     *
     * @return Task to be performed
     */
    public Task getTask();
    
    /**
     * 
     * @return Column that task will be performed on
     */
    public Column getColumn();
    
    /**
     * 
     * @return BatchBindingSetUpdater used to process this Batch Task
     */
    public BatchBindingSetUpdater getBatchUpdater();
    
}
