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

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.InsertOptions;
import com.mongodb.WriteConcern;

/**
 * Provides access to the {@link DBCollection} type.
 * @Deprecated use {@link MongoCollectionType}
 */
@Deprecated
public class DbCollectionType implements CollectionType<DBObject> {
    private final DBCollection collection;

    /**
     * Creates a new instance of {@link DbCollectionType}.
     * @param collection the {@link DBCollection}. (not {@code null})
     */
    public DbCollectionType(final DBCollection collection) {
        this.collection = checkNotNull(collection);
    }

    @Override
    public void insertOne(final DBObject item) {
        collection.insert(item, WriteConcern.ACKNOWLEDGED);
    }

    @Override
    public void insertMany(final List<DBObject> items) {
        collection.insert(items, new InsertOptions().continueOnError(true));
    }
}