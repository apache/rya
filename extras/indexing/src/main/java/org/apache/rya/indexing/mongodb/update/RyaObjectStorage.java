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
package org.apache.rya.indexing.mongodb.update;

import java.util.Optional;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.mongodb.IndexingException;

/**
 * Stores and provides access to objects of type T.
 * The RyaURI subject is the primary storage key used.
 * @param <T> - The type of object to store/access.
 */
public interface RyaObjectStorage<T> {

    /**
     * Creates a new {@link RyaObjectStorage#T} within the storage. The new object's subject must be unique.
     *
     * @param obj - The {@link RyaObjectStorage#T} to create. (not null)
     * @throws ObjectAlreadyExistsException An Object could not be created because one already exists for the Subject.
     * @throws ObjectStorageException A problem occurred while creating the Object.
     */
    public void create(T doc) throws ObjectAlreadyExistsException, ObjectStorageException;

    /**
     * Get an Object from the storage by its subject.
     *
     * @param subject - Identifies which Object to get. (not null)
     * @return The Object if one exists for the subject.
     * @throws ObjectStorageException A problem occurred while fetching the Object from the storage.
     */
    public Optional<T> get(RyaURI subject) throws ObjectStorageException;

    /**
     * Update the state of an {@link RyaObjectStorage#T}.
     *
     * @param old - The Object the changes were applied to. (not null)
     * @param updated - The updated Object to store. (not null)
     * @throws StaleUpdateException The {@code old} Object does not match any that are stored.
     * @throws ObjectStorageException A problem occurred while updating the Object within the storage.
     */
    public void update(T old, T updated) throws StaleUpdateException, ObjectStorageException;

    /**
     * Deletes an {@link RyaObjectStorage#T} from the storage.
     *
     * @param subject -Identifies which {@link RyaObjectStorage#T} to delete. (not null)
     * @return {@code true} if something was deleted; otherwise {@code false}.
     * @throws ObjectStorageException A problem occurred while deleting from the storage.
     */
    public boolean delete(RyaURI subject) throws ObjectStorageException;

    /**
     * Indicates a problem while interacting with an {@link RyaObjectStorage}.
     */
    public static class ObjectStorageException extends IndexingException {
        private static final long serialVersionUID = 1L;

        /**
         * Constructs a new exception with the specified detail message.  The
         * cause is not initialized, and may subsequently be initialized by
         * a call to {@link #initCause}.
         *
         * @param   message   the detail message. The detail message is saved for
         *          later retrieval by the {@link #getMessage()} method.
         */
        public ObjectStorageException(final String message) {
            super(message);
        }

        /**
         * Constructs a new exception with the specified detail message and
         * cause.  <p>Note that the detail message associated with
         * {@code cause} is <i>not</i> automatically incorporated in
         * this exception's detail message.
         *
         * @param  message the detail message (which is saved for later retrieval
         *         by the {@link #getMessage()} method).
         * @param  cause the cause (which is saved for later retrieval by the
         *         {@link #getCause()} method).  (A <tt>null</tt> value is
         *         permitted, and indicates that the cause is nonexistent or
         *         unknown.)
         */
        public ObjectStorageException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * An {@link RyaObjectStorage#T} could not be created because one already exists for the Subject.
     */
    public static class ObjectAlreadyExistsException extends ObjectStorageException {
        private static final long serialVersionUID = 1L;

        public ObjectAlreadyExistsException(final String message) {
            super(message);
        }

        public ObjectAlreadyExistsException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * An object could not be updated because the old state does not
     * match the current state.
     */
    public static class StaleUpdateException extends ObjectStorageException {
        private static final long serialVersionUID = 1L;

        public StaleUpdateException(final String message) {
            super(message);
        }

        public StaleUpdateException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
