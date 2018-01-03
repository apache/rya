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

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.index.RyaSecondaryIndexer;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.EntityStorage.EntityStorageException;
import org.apache.rya.indexing.entity.storage.TypeStorage;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Updates the {@link Entity}s that are in a {@link EntityStorage} when new
 * {@link RyaStatement}s are added/removed from the Rya instance.
 */
public interface EntityIndexer extends RyaSecondaryIndexer {

    /**
     * Creates the {@link EntityStorage} that will be used by the indexer.
     *
     * @return The {@link EntityStorage} that will be used by this indexer.
     * @throws EntityStorageException
     */
    public @Nullable EntityStorage getEntityStorage() throws EntityStorageException;

    /**
     * Creates the {@link TypeStorage} that will be used by the indexer.
     *
     * @return The {@link TypeStorage} that will be used by this indexer.
     */
    public @Nullable TypeStorage getTypeStorage();
}