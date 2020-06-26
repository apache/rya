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
package org.apache.rya.indexing.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.domain.RyaResource;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.model.TypedEntity;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.EntityStorage.EntityStorageException;
import org.apache.rya.indexing.entity.storage.mongo.ConvertingCursor;
import org.apache.rya.indexing.entity.storage.mongo.MongoEntityStorage;
import org.apache.rya.indexing.mongodb.update.RyaObjectStorage.ObjectStorageException;
import org.apache.rya.indexing.smarturi.SmartUriAdapter;
import org.apache.rya.indexing.smarturi.SmartUriException;
import org.apache.rya.indexing.smarturi.SmartUriStorage;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * MongoDB implementation of the Smart URI.
 */
public class MongoDbSmartUri implements SmartUriStorage {
    private boolean isInit = false;
    private final StatefulMongoDBRdfConfiguration conf;
    private MongoClient mongoClient = null;
    private EntityStorage entityStorage;

    /**
     * Creates a new instance of {@link MongoDbSmartUri}.
     * @param conf the {@link StatefulMongoDBRdfConfiguration}. (not {@code null})
     */
    public MongoDbSmartUri(final StatefulMongoDBRdfConfiguration conf) {
        this.conf = checkNotNull(conf);
    }

    @Override
    public void storeEntity(final RyaResource subject, final Map<IRI, Value> map) throws SmartUriException {
        checkInit();

        final IRI uri = SmartUriAdapter.serializeUri(subject, map);
        final Entity entity = SmartUriAdapter.deserializeUriEntity(uri);

        // Create it.
        try {
            entityStorage.create(entity);
        } catch (final ObjectStorageException e) {
            throw new SmartUriException("Failed to create entity storage", e);
        }
    }

    @Override
    public void storeEntity(final Entity entity) throws SmartUriException {
        checkInit();

        // Create it.
        try {
            entityStorage.create(entity);
        } catch (final ObjectStorageException e) {
            throw new SmartUriException("Failed to create entity storage", e);
        }
    }

    @Override
    public void updateEntity(final Entity oldEntity, final Entity updatedEntity) throws SmartUriException {
        checkInit();

        // Update it.
        try {
            entityStorage.update(oldEntity, updatedEntity);
        } catch (final ObjectStorageException e) {
            throw new SmartUriException("Failed to update entity", e);
        }
    }

    @Override
    public Entity queryEntity(final RyaResource subject) throws SmartUriException {
        checkInit();

        // Query it.
        try {
            final Optional<Entity> resultEntity = entityStorage.get(subject);
            return resultEntity.get();
        } catch (final ObjectStorageException e) {
            throw new SmartUriException("Failed to query entity storage", e);
        }
    }

    @Override
    public ConvertingCursor<TypedEntity> queryEntity(final Type type, final Map<IRI, Value> map) throws SmartUriException {
        checkInit();

        // Query it.
        try {
            final Set<Property> properties = SmartUriAdapter.mapToProperties(map);
            final ConvertingCursor<TypedEntity> cursor = entityStorage.search(Optional.empty(), type, properties);
            return cursor;
        } catch (final EntityStorageException e) {
            throw new SmartUriException("Failed to query entity storage", e);
        }
    }

    private void checkInit() throws SmartUriException {
        if (!isInit) {
            try {
                setupClient(conf);
            } catch (final UnknownHostException | MongoException | EntityStorageException e) {
                throw new SmartUriException("Failed to setup MongoDB client", e);
            }
        }
    }

    /**
     * Setup the MongoDB client.
     * @param conf the {@link Configuration}.
     * @throws UnknownHostException
     * @throws MongoException
     * @throws EntityStorageException
     */
    private void setupClient(final StatefulMongoDBRdfConfiguration conf) throws UnknownHostException, MongoException, EntityStorageException {
        mongoClient = conf.getMongoClient();
        entityStorage = new MongoEntityStorage(mongoClient, conf.getRyaInstanceName());
        isInit = true;
    }

    /**
     * @return the {@link EntityStorage}.
     */
    public EntityStorage getEntityStorage() {
        return entityStorage;
    }
}