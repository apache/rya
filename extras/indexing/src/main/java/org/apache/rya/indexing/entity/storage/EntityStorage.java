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

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.entity.EntityIndexException;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.model.TypedEntity;

import mvm.rya.api.domain.RyaURI;

/**
 * Stores and provides access to {@link Entity}s.
 */
@ParametersAreNonnullByDefault
public interface EntityStorage {

    /**
     * Creates a new {@link Entity} within the storage. The new Entity's subject must be unique.
     *
     * @param entity - The {@link Entity} to create. (not null)
     * @throws EntityAlreadyExistsException An {@link Entity} could not be created because one already exists for the Subject.
     * @throws EntityStorageException A problem occurred while creating the Entity.
     */
    public void create(Entity entity) throws EntityAlreadyExistsException, EntityStorageException;

    /**
     * Get an {@link Entity} from the storage by its subject.
     *
     * @param subject - Identifies which {@link Entity} to get. (not null)
     * @return The {@link Entity} if one exists for the subject.
     * @throws EntityStorageException A problem occurred while fetching the Entity from the storage.
     */
    public Optional<Entity> get(RyaURI subject) throws EntityStorageException;

    /**
     * Update the state of an {@link Entity}.
     *
     * @param old - The Entity the changes were applied to. (not null)
     * @param updated - The updated Entity to store. (not null)
     * @throws StaleUpdateException The {@code old} Entity does not match any Entities that are stored.
     * @throws EntityStorageException A problem occurred while updating the Entity within the storage.
     */
    public void update(Entity old, Entity updated) throws StaleUpdateException, EntityStorageException;

    /**
     * Search the stored {@link Entity}s that have a specific {@link Type} as
     * well as the provided {@link Property} values.
     *
     * @param type - The {@link Type} of the Entities. (not null)
     * @param properties - The {@link Property} values that must be set on the Entity. (not null)
     * @return A {@link CloseableIterator} over the {@link TypedEntity}s that match the search parameters.
     * @throws EntityStorageException A problem occurred while searching the storage.
     */
    public CloseableIterator<TypedEntity> search(Type type, Set<Property> properties) throws EntityStorageException;

    /**
     * Deletes an {@link Entity} from the storage.
     *
     * @param subject -Identifies which {@link Entity} to delete. (not null)
     * @return {@code true} if something was deleted; otherwise {@code false}.
     * @throws EntityStorageException A problem occurred while deleting from the storage.
     */
    public boolean delete(RyaURI subject) throws EntityStorageException;

    /**
     * Indicates a problem while interacting with an {@link EntityStorage}.
     */
    public static class EntityStorageException extends EntityIndexException {
        private static final long serialVersionUID = 1L;

        /**
         * Constructs a new exception with the specified detail message.  The
         * cause is not initialized, and may subsequently be initialized by
         * a call to {@link #initCause}.
         *
         * @param   message   the detail message. The detail message is saved for
         *          later retrieval by the {@link #getMessage()} method.
         */
        public EntityStorageException(String message) {
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
        public EntityStorageException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * An {@link Entity} could not be created because one already exists for the Subject.
     */
    public static class EntityAlreadyExistsException extends EntityStorageException {
        private static final long serialVersionUID = 1L;

        public EntityAlreadyExistsException(String message) {
            super(message);
        }

        public EntityAlreadyExistsException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * An {@link TypedEntity} could not be updated because the old state does not
     * match the current state.
     */
    public static class StaleUpdateException extends EntityStorageException {
        private static final long serialVersionUID = 1L;

        public StaleUpdateException(String message) {
            super(message);
        }

        public StaleUpdateException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}