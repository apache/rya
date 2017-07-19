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

/**
 * An exception to be used when there is a problem running the MongoDB Batch
 * Writer.
 */
public class MongoDbBatchWriterException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of {@link MongoDbBatchWriterException}.
     */
    public MongoDbBatchWriterException() {
        super();
    }

    /**
     * Creates a new instance of {@link MongoDbBatchWriterException}.
     * @param message the detail message.
     */
    public MongoDbBatchWriterException(final String message) {
        super(message);
    }

    /**
     * Creates a new instance of {@link MongoDbBatchWriterException}.
     * @param message the detail message.
     * @param throwable the {@link Throwable} source.
     */
    public MongoDbBatchWriterException(final String message, final Throwable source) {
        super(message, source);
    }

    /**
     * Creates a new instance of {@link MongoDbBatchWriterException}.
     * @param source the {@link Throwable} source.
     */
    public MongoDbBatchWriterException(final Throwable source) {
        super(source);
    }
}