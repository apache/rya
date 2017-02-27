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
package org.apache.rya.indexing.smarturi;

import java.util.Map;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.model.TypedEntity;
import org.apache.rya.indexing.entity.storage.mongo.ConvertingCursor;
import org.calrissian.mango.collect.CloseableIterator;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

/**
 * Interface for interacting with a Smart URI's datastore.
 */
public interface SmartUriStorage {
    /**
     * Stores the map into the datastore.
     * @param subject the {@link RyaURI} subject of the Entity. Identifies the
     * thing that is being represented as an Entity.
     * @param map the {@link Map} of {@link URI}s to {@link Value}s.
     * @throws SmartUriException
     */
    public void storeEntity(final RyaURI subject, final Map<URI, Value> map) throws SmartUriException;

    /**
     * Stores the entity into the datastore.
     * @param entity the {@link Entity}.
     * @throws SmartUriException
     */
    public void storeEntity(final Entity entity) throws SmartUriException;

    /**
     * Updates the entity.
     * @param oldEntity the old {@link Entity} to update
     * @param updatedEntity the new {@link Entity} to replace the old one with.
     * @throws SmartUriException
     */
    public void updateEntity(final Entity oldEntity, final Entity updatedEntity) throws SmartUriException;

    /**
     * Queries for the entity based on the subject
     * @param subject the {@link RyaURI} subject of the Entity. Identifies the
     * thing that is being represented as an Entity.
     * @return the {@link Entity} matching the subject.
     * @throws SmartUriException
     */
    public Entity queryEntity(final RyaURI subject) throws SmartUriException;

    /**
     * Queries the datastore for the map.
     * @param type the type associated with the entity values.
     * @param map the {@link Map} of {@link URI}s to {@link Value}s.
     * @return a {@link CloseableIterator} over the {@link TypedEntity}s that
     * match the search parameters.
     * @throws SmartUriException
     */
   public ConvertingCursor<TypedEntity> queryEntity(final Type type, final Map<URI, Value> map) throws SmartUriException;
}