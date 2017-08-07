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
package org.apache.rya.mongodb.iter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.dao.MongoDBStorageStrategy;
import org.apache.rya.mongodb.document.operators.aggregation.AggregationUtil;
import org.bson.Document;

import com.mongodb.DBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.util.JSON;

import info.aduna.iteration.CloseableIteration;

public class RyaStatementCursorIterator implements CloseableIteration<RyaStatement, RyaDAOException> {
    private static final Logger log = Logger.getLogger(RyaStatementCursorIterator.class);

    private final MongoCollection coll;
    private final Iterator<DBObject> queryIterator;
    private Iterator<Document> resultsIterator;
    private final MongoDBStorageStrategy<RyaStatement> strategy;
    private Long maxResults;
    private final Authorizations auths;

    public RyaStatementCursorIterator(final MongoCollection<Document> collection, final Set<DBObject> queries,
            final MongoDBStorageStrategy<RyaStatement> strategy, final Authorizations auths) {
        coll = collection;
        queryIterator = queries.iterator();
        this.strategy = strategy;
        this.auths = auths;
    }

    @Override
    public boolean hasNext() {
        if (!currentCursorIsValid()) {
            findNextValidCursor();
        }
        return currentCursorIsValid();
    }

    @Override
    public RyaStatement next() {
        if (!currentCursorIsValid()) {
            findNextValidCursor();
        }
        if (currentCursorIsValid()) {
            // convert to Rya Statement
            final Document queryResult = resultsIterator.next();
            final DBObject dbo = (DBObject) JSON.parse(queryResult.toJson());
            final RyaStatement statement = strategy.deserializeDBObject(dbo);
            return statement;
        }
        return null;
    }

    private void findNextValidCursor() {
        while (queryIterator.hasNext()){
            final DBObject currentQuery = queryIterator.next();

            // Executing redact aggregation to only return documents the user
            // has access to.
            final List<Document> pipeline = new ArrayList<>();
            pipeline.add(new Document("$match", currentQuery));
            pipeline.addAll(AggregationUtil.createRedactPipeline(auths));
            log.debug(pipeline);
            final AggregateIterable<Document> output = coll.aggregate(pipeline);
            output.batchSize(1000);

            resultsIterator = output.iterator();
            if (resultsIterator.hasNext()) {
                break;
            }
        }
    }

    private boolean currentCursorIsValid() {
        return (resultsIterator != null) && resultsIterator.hasNext();
    }


    public void setMaxResults(final Long maxResults) {
        this.maxResults = maxResults;
    }

    @Override
    public void close() throws RyaDAOException {
        // TODO don't know what to do here
    }

    @Override
    public void remove() throws RyaDAOException {
        next();
    }
}
