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
package org.apache.rya.indexing.pcj.storage;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;

/**
 * Functions that create and maintain the PCJ tables that are used by Rya.
 */
@ParametersAreNonnullByDefault
public interface PrecomputedJoinStorage {

    /**
     * Get a list of all Precomputed Join indices that are being maintained.
     *
     * @return The lkist of managed Precomputed Join indices.
     */
    public List<String> listPcjs() throws PCJStorageException;

    /**
     * Create a new Precomputed Join index.
     *
     * @param sparql - A SPAQL query that defines how the index will be updated. (not null)
     * @param varOrders - The variable orders the results within the table will be written to. (not null)
     * @return A unique identifier for the index.
     */
    public String createPcj(final String sparql, final Set<VariableOrder> varOrders) throws PCJStorageException;

    /**
     * Get metadata about the Precomputed Join index.
     *
     * @param pcjId - Identifies the index the metadata will be read from. (not null)
     * @return The metadata stored within the index.
     */
    public PcjMetadata getPcjMetadata(final String pcjId) throws PCJStorageException;

    /**
     * Adds new join results to a Precomputed Join index.
     *
     * @param pcjId - Identifies the index the results will be added to. (not null)
     * @param results - The results that will be added to the index. (not null)
     * @throws PCJStorageException Indicates the results could not be added to the index.
     */
    public void addResults(final String pcjId, final Collection<VisibilityBindingSet> results) throws PCJStorageException;

    /**
     * Clears all values from a Precomputed Join index. The index will remain,
     * but all of its values will be removed.
     *
     * @param pcjId - Identifies the index to purge. (not null)
     * @throws PCJStorageException Indicates the values from the index could not be purged.
     */
    public void purge(final String pcjId) throws PCJStorageException;

    /**
     * Completely removes a Precomputed Join index from the system.
     *
     * @param pcjId - Identifies the index to drop. (not null)
     * @throws PCJStorageException Indicates the index could not be dropped.
     */
    public void dropPcj(final String pcjId) throws PCJStorageException;


    /**
     * Releases and resources that are being used by the storage.
     *
     * @throws PCJStorageException Indicates the resources could not be released.
     */
    public void close() throws PCJStorageException;

    /**
     * An operation of {@link PrecomputedJoinStorage} failed.
     */
    public static class PCJStorageException extends PcjException {
        private static final long serialVersionUID = 1L;

        /**
         * Constructs a new exception with the specified detail message. The cause
         * is not initialized, and may subsequently be initialized by a call to
         * {@link Throwable#initCause(java.lang.Throwable)}.
         *
         * @param message - The detail message. The detail message is saved for
         *   later retrieval by the {@link Throwable#getMessage()} method.
         */
        public PCJStorageException(final String message) {
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
        public PCJStorageException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}