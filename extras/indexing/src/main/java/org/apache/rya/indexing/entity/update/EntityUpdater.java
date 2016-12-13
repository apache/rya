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
package org.apache.rya.indexing.entity.update;

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import java.util.function.Function;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.EntityStorage.EntityAlreadyExistsException;
import org.apache.rya.indexing.entity.storage.EntityStorage.EntityStorageException;
import org.apache.rya.indexing.entity.storage.EntityStorage.StaleUpdateException;

import mvm.rya.api.domain.RyaURI;

/**
 * Performs update operations over an {@link EntityStorage}.
 */
@ParametersAreNonnullByDefault
public class EntityUpdater {

    private final EntityStorage storage;

    /**
     * Constructs an instance of {@link EntityUpdater}.
     *
     * @param storage - The storage this updater operates over. (not null)
     */
    public EntityUpdater(EntityStorage storage) {
        this.storage = requireNonNull(storage);
    }

    /**
     * Tries to updates the state of an {@link Entity} until the update succeeds
     * or a non-recoverable exception is thrown.
     *
     * @param subject - The Subject of the {@link Entity} that will be updated. (not null)
     * @param mutator - Performs the mutation on the old state of the Entity and returns
     *   the new state of the Entity. (not null)
     * @throws EntityStorageException A non-recoverable error has caused the update to fail.
     */
    public void update(RyaURI subject, EntityMutator mutator) throws EntityStorageException {
        requireNonNull(subject);
        requireNonNull(mutator);

        // Fetch the current state of the Entity.
        boolean completed = false;
        while(!completed) {
            try {
                final Optional<Entity> old = storage.get(subject);
                final Optional<Entity> updated = mutator.apply(old);

                final boolean doWork = updated.isPresent();
                if(doWork) {
                    if(!old.isPresent()) {
                        storage.create(updated.get());
                    } else {
                        storage.update(old.get(), updated.get());
                    }
                }
                completed = true;
            } catch(final EntityAlreadyExistsException | StaleUpdateException e) {
                // These are recoverable exceptions. Try again.
            } catch(final RuntimeException e) {
                throw new EntityStorageException("Failed to update Entity with Subject '" + subject.getData() + "'.", e);
            }
        }
    }

    /**
     * Implementations of this interface are used to update the state of an
     * {@link Entity} in unison with a {@link EntityUpdater}.
     * </p>
     * This table describes what the updater will do depending on if an Entity
     * exists and if an updated Entity is returned.
     * </p>
     * <table border="1px">
     *     <tr><th>Entity Provided</th><th>Update Returned</th><th>Effect</th></tr>
     *     <tr>
     *         <td>true</td>
     *         <td>true</td>
     *         <td>The old Entity will be updated using the returned state.</td>
     *     </tr>
     *     <tr>
     *         <td>true</td>
     *         <td>false</td>
     *         <td>No work is performed.</td>
     *     </tr>
     *     <tr>
     *         <td>false</td>
     *         <td>true</td>
     *         <td>A new Entity will be created using the returned state.</td>
     *     </tr>
     *     <tr>
     *         <td>false</td>
     *         <td>false</td>
     *         <td>No work is performed.</td>
     *     </tr>
     * </table>
     */
    public interface EntityMutator extends Function<Optional<Entity>, Optional<Entity>> { }
}