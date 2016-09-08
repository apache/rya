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
import static mvm.rya.mongodb.dao.SimpleMongoDBStorageStrategy.TIMESTAMP;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.rya.export.api.store.AddStatementException;
import org.apache.rya.export.api.store.ContainsStatementException;
import org.apache.rya.export.api.store.RemoveStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.api.store.UpdateStatementException;

import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.mongodb.MongoDBRyaDAO;
import mvm.rya.mongodb.dao.SimpleMongoDBStorageStrategy;

/**
 * Mongo implementation of {@link RyaStatementStore}.  Allows for exporting and
 * importing rya statements from MongoDB.
 */
public class MongoRyaStatementStore implements RyaStatementStore{
    public static final String TRIPLES_COLLECTION = "rya__triples";
    private final SimpleMongoDBStorageStrategy adapter;
    private final DB db;

    private final String ryaInstanceName;
    private final MongoClient client;
    private final MongoDBRyaDAO dao;

    /**
     * Creates a new {@link MongoRyaStatementStore}.
     * @param client - The client to connect to Mongo. (not null)
     * @param ryaInstance - The rya instance to connect to. (not null)
     * @param dao - The {@link MongoDBRyaDAO} to use to access statements. (not null)
     */
    public MongoRyaStatementStore(final MongoClient client, final String ryaInstance, final MongoDBRyaDAO dao) {
        this.client = checkNotNull(client);
        ryaInstanceName = checkNotNull(ryaInstance);
        this.dao = checkNotNull(dao);
        db = this.client.getDB(ryaInstanceName);
        adapter = new SimpleMongoDBStorageStrategy();
    }

    @Override
    public Iterator<RyaStatement> fetchStatements() {
        final Cursor cur = db.getCollection(TRIPLES_COLLECTION).find().sort(new BasicDBObject(TIMESTAMP, 1));
        final List<RyaStatement> statements = new ArrayList<>();
        while(cur.hasNext()) {
            final RyaStatement statement = adapter.deserializeDBObject(cur.next());
            statements.add(statement);
        }
        return statements.iterator();
    }

    @Override
    public void addStatement(final RyaStatement statement) throws AddStatementException {
        try {
            dao.add(statement);
        } catch (final RyaDAOException e) {
            throw new AddStatementException("Unable to add statement: '" + statement.toString() + "'", e);
        }
    }

    @Override
    public void removeStatement(final RyaStatement statement) throws RemoveStatementException {
        try {
            //mongo dao does not need a config to remove.
            dao.delete(statement, null);
        } catch (final RyaDAOException e) {
            throw new RemoveStatementException("Unable to remove statement: '" + statement.toString() + "'", e);
        }
    }

    @Override
    public boolean containsStatement(final RyaStatement statement) throws ContainsStatementException {
        final DBObject dbo = adapter.serialize(statement);
        return db.getCollection(TRIPLES_COLLECTION).find(dbo).count() > 0;
    }

    protected MongoClient getClient() {
        return client;
    }

    @Override
    public void updateStatement(final RyaStatement original, final RyaStatement update) throws UpdateStatementException {
        //Since mongo does not support visibility, this does nothing for mongo.
        //Do not want a throw a not-implemented exception since that could potentially
        //break stuff.
    }
}