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
package org.apache.rya.mongodb.batch.collection;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;

/**
 * Provides access to the {@link MongoCollection} type.
 */
public class MongoCollectionType implements CollectionType<Document> {
    private final MongoCollection<Document> collection;

    /**
     * Creates a new instance of {@link MongoCollectionType}.
     * @param collection the {@link MongoCollection}. (not {@code null})
     */
    public MongoCollectionType(final MongoCollection<Document> collection) {
        this.collection = checkNotNull(collection);
    }

    @Override
    public void insertOne(final Document item) {
        collection.insertOne(item);
    }

    @Override
    public void insertMany(final List<Document> items) {
        collection.insertMany(items, new InsertManyOptions().ordered(false));
    }
}