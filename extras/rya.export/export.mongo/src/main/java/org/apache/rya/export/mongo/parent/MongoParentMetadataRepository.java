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
package org.apache.rya.export.mongo.parent;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.rya.export.api.metadata.MergeParentMetadata;
import org.apache.rya.export.api.metadata.ParentMetadataDoesNotExistException;
import org.apache.rya.export.api.metadata.ParentMetadataExistsException;
import org.apache.rya.export.api.metadata.ParentMetadataRepository;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * Repository for storing the {@link MergeParentMetadata}.
 */
public class MongoParentMetadataRepository implements ParentMetadataRepository {
    private static final String COLLECTION_NAME = "parent_metadata";
    private final ParentMetadataRepositoryAdapter adapter;
    private final DBCollection collection;

    /**
     * Creates a new {@link MongoParentMetadataRepository}
     * @param client - The client connection to mongo.
     * @param dbName - The database to connect to, usually the RyaInstanceName
     */
    public MongoParentMetadataRepository(final MongoClient client, final String dbName) {
        checkNotNull(client);
        checkNotNull(dbName);
        collection = client.getDB(dbName).getCollection(COLLECTION_NAME);
        adapter = new ParentMetadataRepositoryAdapter();
    }

    @Override
    public MergeParentMetadata get() throws ParentMetadataDoesNotExistException {
        final DBObject mongoMetadata = collection.findOne();
        if(mongoMetadata == null) {
            throw new ParentMetadataDoesNotExistException("The parent metadata has not been set.");
        }
        return adapter.deserialize(mongoMetadata);
    }

    @Override
    public void set(final MergeParentMetadata metadata) throws ParentMetadataExistsException {
        if(collection.getCount() > 0) {
            throw new ParentMetadataExistsException("The parent metadata has already been set.");
        }
        final DBObject dbo = adapter.serialize(metadata);
        collection.insert(dbo);
    }
}