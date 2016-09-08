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
package org.apache.rya.export.mongo.time;

import static com.google.common.base.Preconditions.checkNotNull;
import static mvm.rya.mongodb.dao.SimpleMongoDBStorageStrategy.TIMESTAMP;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.rya.export.mongo.MongoRyaStatementStore;
import org.apache.rya.export.mongo.MongoRyaStatementStoreDecorator;

import com.mongodb.BasicDBObject;
import com.mongodb.Cursor;
import com.mongodb.DB;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.mongodb.dao.SimpleMongoDBStorageStrategy;

/**
 * A {@link MongoRyaStatementStore} that, when fetching statements, only
 * fetches statements after a certain time.
 */
public class TimeMongoRyaStatementStore extends MongoRyaStatementStoreDecorator {
    private final Date time;
    private final DB db;

    private final SimpleMongoDBStorageStrategy adapter;

    /**
     * Creates a new {@link TimeMongoRyaStatementStore}.
     * @param store - The {@link MongoRyaStatementStore} to decorate.
     * @param time - The time used when fetching statements.
     * @param ryaInstanceName - The rya instance used.
     */
    public TimeMongoRyaStatementStore(final MongoRyaStatementStore store, final Date time, final String ryaInstanceName) {
        super(store);
        this.time = checkNotNull(time);
        db = getClient().getDB(ryaInstanceName);
        adapter = new SimpleMongoDBStorageStrategy();
    }

    /**
     * @return
     * @see org.apache.rya.export.mongo.MongoRyaStatementStore#fetchStatements()
     */
    @Override
    public Iterator<RyaStatement> fetchStatements() {
        //RyaStatement timestamps are stored as longs, not dates.
        final BasicDBObject dbo = new BasicDBObject(TIMESTAMP, new BasicDBObject("$gte", time.getTime()));
        final Cursor cur = db.getCollection(MongoRyaStatementStore.TRIPLES_COLLECTION).find(dbo).sort(new BasicDBObject(TIMESTAMP, 1));
        final List<RyaStatement> statements = new ArrayList<>();
        while(cur.hasNext()) {
            final RyaStatement statement = adapter.deserializeDBObject(cur.next());
            statements.add(statement);
        }
        return statements.iterator();
    }

    @Override
    public boolean equals(final Object obj) {
        if(obj instanceof TimeMongoRyaStatementStore) {
            final TimeMongoRyaStatementStore other = (TimeMongoRyaStatementStore) obj;
            final EqualsBuilder builder = new EqualsBuilder()
                .appendSuper(super.equals(obj))
                .append(time, other.time);
            return builder.isEquals();
        }
        return false;
    }


    @Override
    public int hashCode() {
        final HashCodeBuilder builder = new HashCodeBuilder()
            .appendSuper(super.hashCode())
            .append(time);
        return builder.toHashCode();
    }
}