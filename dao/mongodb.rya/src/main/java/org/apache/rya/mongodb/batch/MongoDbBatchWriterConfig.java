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
package org.apache.rya.mongodb.batch;

import com.google.common.base.Preconditions;

/**
 * Configuration for the MongoDB Batch Writer.
 */
public class MongoDbBatchWriterConfig {
    /**
     * The default number of statements to batch write at a time.
     */
    public static final int DEFAULT_BATCH_SIZE = 50000;
    private Integer batchSize = null;

    /**
     * The default time to wait in milliseconds to flush all statements out that
     * are queued for insertion if the queue has not filled up to its capacity
     * of {@link #DEFAULT_BATCH_SIZE} or the user configured buffer size.
     */
    public static final long DEFAULT_BATCH_FLUSH_TIME_MS = 100L;
    private Long batchFlushTimeMs = null;

    /**
     * Creates a new instance of {@link MongoDbBatchWriterConfig}.
     */
    public MongoDbBatchWriterConfig() {
    }

    /**
     * Gets the configured number of statements to batch write at a time.
     * @return the configured value or the default value.
     */
    public int getBatchSize() {
        return batchSize != null ? batchSize : DEFAULT_BATCH_SIZE;
    }

    /**
     * Sets the number of statements to batch write at a time.
     * @param batchSize the number of statements in each batch.
     * @return the {@link MongoDbBatchWriterConfig}.
     */
    public MongoDbBatchWriterConfig setBatchSize(final int batchSize) {
        Preconditions.checkArgument(batchSize > 0, "Batch size must be positive.");
        this.batchSize = batchSize;
        return this;
    }

    /**
     * Gets the configured time to wait in milliseconds to flush all statements
     * out that are queued for insertion if the queue has not filled up to its
     * capacity.
     * @return the configured value or the default value.
     */
    public long getBatchFlushTimeMs() {
        return batchFlushTimeMs != null ? batchFlushTimeMs : DEFAULT_BATCH_FLUSH_TIME_MS;
    }

    /**
     * Sets the time to wait in milliseconds to flush all statements out that
     * are queued for insertion if the queue has not filled up to its capacity.
     * @param batchFlushTimeMs the time to wait before flushing all queued
     * statements that have not been written.
     * @return the {@link MongoDbBatchWriterConfig}.
     */
    public MongoDbBatchWriterConfig setBatchFlushTimeMs(final long batchFlushTimeMs) {
        Preconditions.checkArgument(batchFlushTimeMs >= 0, "Batch flush time must be non-negative.");
        this.batchFlushTimeMs = batchFlushTimeMs;
        return this;
    }
}