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
package org.apache.rya.streams.api.queries;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import info.aduna.iteration.CloseableIteration;

/**
 * An ordered log of all of the changes that have been applied to the SPARQL Queries that are managed by Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public interface QueryChangeLog extends AutoCloseable {

    /**
     * Write a new {@link QueryChange} to the end of the change log.
     *
     * @param newChange - The change that will be written. (not null)
     * @throws QueryChangeLogException The change could not be written to the log.
     */
    public void write(QueryChange newChange) throws QueryChangeLogException;

    /**
     * Read all of the entries that are in this log starting from the first one.
     *
     * @return All of the entries that are in the log.
     * @throws QueryChangeLogException The entries could not be fetched.
     */
    public CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> readFromStart() throws QueryChangeLogException;

    /**
     * Read all of the entries that are inclusively at and after a specific position within the change log.
     *
     * @param position - The position that the iteration will begin at, inclusively.
     * @return The entries that are at and after the specified position.
     * @throws QueryChangeLogException The entries could not be fetched.
     */
    public CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> readFromPosition(long position) throws QueryChangeLogException;

    /**
     * One of the {@link QueryChangeLog} functions failed.
     */
    public static class QueryChangeLogException extends Exception {
        private static final long serialVersionUID = 1L;

        public QueryChangeLogException(final String message) {
            super(message);
        }

        public QueryChangeLogException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}