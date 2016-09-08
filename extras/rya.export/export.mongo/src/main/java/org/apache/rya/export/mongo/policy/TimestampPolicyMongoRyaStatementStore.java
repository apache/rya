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

package org.apache.rya.export.mongo.policy;

import static org.apache.rya.export.mongo.MongoRyaStatementStore.TRIPLES_COLLECTION;
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.TIMESTAMP;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.export.api.conf.policy.TimestampPolicyStatementStore;
import org.apache.rya.export.api.store.FetchStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.mongo.MongoRyaStatementStore;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBObject;

/**
 * A {@link RyaStatementStore} decorated to connect to a Mongo database and
 * filter statements based on a timestamp.
 */
public class TimestampPolicyMongoRyaStatementStore extends TimestampPolicyStatementStore {
    private final SimpleMongoDBStorageStrategy adapter;
    private final DB db;

    /**
     * Creates a new {@link TimestampPolicyMongoRyaStatementStore}
     * @param store - The {@link MongoRyaStatementStore} to connect to
     * @param timestamp - The Date to filter statements on.
     * @param ryaInstanceName - The rya instance to merge statements to/from.
     */
    public TimestampPolicyMongoRyaStatementStore(final MongoRyaStatementStore store, final Date timestamp, final String ryaInstanceName) {
        super(store, timestamp);
        adapter = new SimpleMongoDBStorageStrategy();
        db = store.getClient().getDB(ryaInstanceName);
    }

    @Override
    public Iterator<RyaStatement> fetchStatements() throws FetchStatementException {
        final DBObject timeObj = new BasicDBObjectBuilder()
            .add(SimpleMongoDBStorageStrategy.TIMESTAMP,
                new BasicDBObjectBuilder()
                    .add("$gte", timestamp.getTime()).get())
            .get();
        final Cursor cur = db.getCollection(TRIPLES_COLLECTION).find(timeObj).sort(new BasicDBObject(TIMESTAMP, 1));
        final List<RyaStatement> statements = new ArrayList<>();
        while(cur.hasNext()) {
            final RyaStatement statement = adapter.deserializeDBObject(cur.next());
            statements.add(statement);
        }
        return statements.iterator();
    }
}
