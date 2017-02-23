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
import java.util.Set;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.model.TypedEntity;
import org.apache.rya.indexing.entity.storage.mongo.ConvertingCursor;
import org.apache.rya.indexing.mongodb.update.RyaObjectStorage;
import org.calrissian.mango.collect.CloseableIterator;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Stores and provides access to {@link Entity}s.
 */
@DefaultAnnotation(NonNull.class)
public interface EntityStorage extends RyaObjectStorage<Entity> {
    /**
     * Search the stored {@link Entity}s that have a specific {@link Type} as
     * well as the provided {@link Property} values.
     *
     * @param subject - The {@link RyaURI} subject of the Entity. (Optional)
     * @param type - The {@link Type} of the Entities. (not null)
     * @param properties - The {@link Property} values that must be set on the Entity. (not null)
     * @return A {@link CloseableIterator} over the {@link TypedEntity}s that match the search parameters.
     * @throws EntityStorageException A problem occurred while searching the storage.
     */
    public ConvertingCursor<TypedEntity> search(final Optional<RyaURI> subject, Type type, Set<Property> properties) throws EntityStorageException;

    /**
     * Indicates a problem while interacting with an {@link EntityStorage}.
     */
    public static class EntityStorageException extends ObjectStorageException {
        private static final long serialVersionUID = 1L;

        /**
         * Constructs a new exception with the specified detail message.  The
         * cause is not initialized, and may subsequently be initialized by
         * a call to {@link #initCause}.
         *
         * @param   message   the detail message. The detail message is saved for
         *          later retrieval by the {@link #getMessage()} method.
         */
        public EntityStorageException(final String message) {
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
        public EntityStorageException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * An {@link Entity} could not be created because one already exists for the Subject.
     */
    public static class EntityAlreadyExistsException extends EntityStorageException {
        private static final long serialVersionUID = 1L;

        public EntityAlreadyExistsException(final String message) {
            super(message);
        }

        public EntityAlreadyExistsException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * An {@link TypedEntity} could not be updated because the old state does not
     * match the current state.
     */
    public static class StaleUpdateException extends EntityStorageException {
        private static final long serialVersionUID = 1L;

        public StaleUpdateException(final String message) {
            super(message);
        }

        public StaleUpdateException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}