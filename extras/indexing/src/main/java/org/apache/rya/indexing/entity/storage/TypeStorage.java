/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.entity.storage;

import java.util.Optional;

import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.indexing.entity.EntityIndexException;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.storage.mongo.ConvertingCursor;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Stores and provides access to {@link Type}s.
 */
@DefaultAnnotation(NonNull.class)
public interface TypeStorage {

    /**
     * Creates a new {@link Type} within the storage. The new Type's ID must be unique.
     *
     * @param type - The {@link Type} to create. (not null)
     * @throws TypeStorageException A problem occurred while creating the Type.
     */
    public void create(Type type) throws TypeStorageException;

    /**
     * Get a {@link Type} from the storage by its ID.
     *
     * @param typeId - The {@link Type}'s ID. (not null)
     * @return The {@link Type} if one exists for the ID.
     * @throws TypeStorageException A problem occurred while fetching from the storage.
     */
    public Optional<Type> get(RyaIRI typeId) throws TypeStorageException;

    /**
     * Get all {@link Type}s that include a specific {@link Property} name.
     *
     * @param propertyName - The name to search for. (not null)
     * @return All {@link Type}s that include {@code propertyName}.
     * @throws TypeStorageException A problem occurred while searching for the Types
     *   that have the Property name.
     */
    public ConvertingCursor<Type> search(RyaIRI propertyName) throws TypeStorageException;

    /**
     * Deletes a {@link Type} from the storage.
     *
     * @param typeId - The ID of the {@link Type} to delete. (not null)
     * @return {@code true} if something was deleted; otherwise {@code false}.
     * @throws TypeStorageException A problem occurred while deleting from the storage.
     */
    public boolean delete(RyaIRI typeId) throws TypeStorageException;

    /**
     * A problem occurred while interacting with a {@link TypeStorage}.
     */
    public static class TypeStorageException extends EntityIndexException {
        private static final long serialVersionUID = 1L;

        /**
         * Constructs a new exception with the specified detail message.  The
         * cause is not initialized, and may subsequently be initialized by
         * a call to {@link #initCause}.
         *
         * @param   message   the detail message. The detail message is saved for
         *          later retrieval by the {@link #getMessage()} method.
         */
        public TypeStorageException(final String message) {
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
        public TypeStorageException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}