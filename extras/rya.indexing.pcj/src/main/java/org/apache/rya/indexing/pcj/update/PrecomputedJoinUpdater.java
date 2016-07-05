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
package org.apache.rya.indexing.pcj.update;

import java.util.Collection;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.pcj.storage.PcjException;

import mvm.rya.api.domain.RyaStatement;

/**
 * Updates the state of all PCJ indices whenever {@link RyaStatement}s are
 * added to or removed from the system.
 */
@ParametersAreNonnullByDefault
public interface PrecomputedJoinUpdater {

    /**
     * The PCJ indices will be updated to include new statements within
     * their results.
     *
     * @param statements - The statements that will be used to updated the index. (not null)
     * @throws PcjUpdateException The statements could not be added to the index.
     */
    public void addStatements(final Collection<RyaStatement> statements) throws PcjUpdateException;

    /**
     * The PCJ indices will be updated to remove any results that are
     * derived from the provided statements.
     * </p>
     * A result will only be deleted from the index if all statements
     * it is derived from are removed. For example, suppose the following
     * instructions execute:
     * <pre>
     *   Insert Statement A
     *   Insert Statement B
     *   Insert Statement C
     *   A and B Join to create Result A
     *   B and C Join to create Result A again
     *   Delete Statement A
     * </pre>
     * Result A will remain in the index because B and C have not been
     * delete. However, If either B or C are deleted, then the result will
     * also be deleted because it can no longer be derived from the remaining
     * information.
     *
     * @param statements - The statements that will be used to updated the index. (not null)
     * @throws PcjUpdateException The statements could not be removed from the index.
     */
    public void deleteStatements(Collection<RyaStatement> statements) throws PcjUpdateException;

    /**
     * If the updater does any batching, then this will force it to flush immediately.
     *
     * @throws PcjUpdateException The updater could not be flushed.
     */
    public void flush() throws PcjUpdateException;

    /**
     * Cleans up any resources required to perform the updates (sockets, streams, etc).
     *
     * @throws PcjUpdateException The updater could not be closed.
     */
    public void close() throws PcjUpdateException;

    /**
     * An operation of {@link PrecomputedJoinUpdater} failed.
     */
    public static class PcjUpdateException extends PcjException {
        private static final long serialVersionUID = 1L;

        /**
         * Constructs a new exception with the specified detail message. The cause
         * is not initialized, and may subsequently be initialized by a call to
         * {@link Throwable#initCause(java.lang.Throwable)}.
         *
         * @param message - The detail message. The detail message is saved for
         *   later retrieval by the {@link Throwable#getMessage()} method.
         */
        public PcjUpdateException(final String message) {
            super(message);
        }

        /**
         * Constructs a new exception with the specified detail message and cause.
         * </p>
         * Note that the detail message associated with cause is not automatically
         * incorporated in this exception's detail message.
         *
         * @param message - The detail message (which is saved for later retrieval
         *   by the {@link Throwable#getMessage()} method).
         * @param cause - The cause (which is saved for later retrieval by the
         *   {@link Throwable#getCause()} method). (A null value is permitted, and
         *   indicates that the cause is nonexistent or unknown.)
         */
        public PcjUpdateException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}