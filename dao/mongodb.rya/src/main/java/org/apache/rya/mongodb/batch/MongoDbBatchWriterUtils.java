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

import org.apache.hadoop.conf.Configuration;

/**
 * Constant and utility methods related to batch writing statements in a MongoDB
 * Rya repository.
 */
public final class MongoDbBatchWriterUtils {
    /**
     * The default number of statements to batch write at a time. The user can
     * specify a buffer size in the config with the
     * {@link #BATCH_WRITE_SIZE_TAG} option.
     */
    public static final int DEFAULT_BATCH_WRITE_SIZE = 50000;

    /**
     * The default time to wait in milliseconds to flush all statements out that
     * are queued for insertion if the queue has not filled up to its capacity
     * of {@link #DEFAULT_STATEMENT_INSERTION_BUFFER_SIZE} or the user
     * configured buffer size. The user can specify a flush time in the config
     * with the {@link #BATCH_FLUSH_TIME_MS_TAG} option.
     */
    public static final long DEFAULT_BATCH_FLUSH_TIME_MS = 5000L;

    /**
     * Config tag used to specify the number of statements to batch write at a
     * time.
     */
    public static final String BATCH_WRITE_SIZE_TAG = "rya.mongodb.dao.batchwritesize";

    /**
     * Config tag used to specify the time to wait in milliseconds to flush all
     * statements out that are queued for insertion if the queue has not filled
     * up to its capacity.
     */
    public static final String BATCH_FLUSH_TIME_MS_TAG = "rya.mongodb.dao.batchflushtime";

    /**
     * Private constructor to prevent instantiation.
     */
    private MongoDbBatchWriterUtils() {
    }

    /**
     * The number of statements to batch write at a time.
     * @param conf the {@link Configuration} to check.
     * @return the configured value or the default value.
     */
    public static int getConfigBatchWriteSize(final Configuration conf) {
        return conf.getInt(BATCH_WRITE_SIZE_TAG, DEFAULT_BATCH_WRITE_SIZE);
    }

    /**
     * The time to wait in milliseconds to flush all statements out that are
     * queued for insertion if the queue has not filled up to its capacity.
     * @param conf the {@link Configuration} to check.
     * @return the configured value or the default value.
     */
    public static long getConfigBatchFlushTimeMs(final Configuration conf) {
        return conf.getLong(BATCH_FLUSH_TIME_MS_TAG, DEFAULT_BATCH_FLUSH_TIME_MS);
    }
}