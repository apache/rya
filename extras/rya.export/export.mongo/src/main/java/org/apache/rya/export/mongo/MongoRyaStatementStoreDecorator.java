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
package org.apache.rya.export.mongo;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.api.store.RyaStatementStoreDecorator;

import com.mongodb.MongoClient;

/**
 * Ensures the decorator that the decorated store is mongodb backed.
 */
public abstract class MongoRyaStatementStoreDecorator extends RyaStatementStoreDecorator {
    final MongoRyaStatementStore store;

    /**
     * Creates a new {@link MongoRyaStatementStoreDecorator} around the provided {@link RyaStatementStore}.
     * @param store - The {@link RyaStatementStore} to decorate.
     */
    public MongoRyaStatementStoreDecorator(final MongoRyaStatementStore store) {
        super(store);
        this.store = checkNotNull(store);
    }

    protected MongoClient getClient() {
        return store.getClient();
    }
}