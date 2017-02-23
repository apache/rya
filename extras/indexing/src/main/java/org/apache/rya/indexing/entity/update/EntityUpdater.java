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

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.EntityStorage.EntityStorageException;
import org.apache.rya.indexing.mongodb.update.DocumentUpdater;
import org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Performs update operations over an {@link EntityStorage}.
 */
@DefaultAnnotation(NonNull.class)
public class EntityUpdater implements DocumentUpdater<RyaURI, Entity>{

    private final EntityStorage storage;

    /**
     * Constructs an instance of {@link EntityUpdater}.
     *
     * @param storage - The storage this updater operates over. (not null)
     */
    public EntityUpdater(final EntityStorage storage) {
        this.storage = requireNonNull(storage);
    }

    @Override
    public void create(final Entity newObj) throws EntityStorageException {
        try {
            storage.create(newObj);
        } catch (final ObjectStorageException e) {
            throw new EntityStorageException(e.getMessage(), e);
        }
    }

    @Override
    public void update(final Entity old, final Entity updated) throws EntityStorageException {
        try {
            storage.update(old, updated);
        } catch (final ObjectStorageException e) {
            throw new EntityStorageException(e.getMessage(), e);
        }
    }

    @Override
    public Optional<Entity> getOld(final RyaURI key) throws EntityStorageException {
        try {
            return storage.get(key);
        } catch (final ObjectStorageException e) {
            throw new EntityStorageException(e.getMessage(), e);
        }
    }
}