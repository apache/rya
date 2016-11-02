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

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import info.aduna.iteration.CloseableIteration;

public class RyaStatementCursorIterator implements CloseableIteration<RyaStatement, RyaDAOException> {
    private static final Logger log = Logger.getLogger(RyaStatementCursorIterator.class);

    private final DBCollection coll;
    private final Iterator<DBObject> queryIterator;
    private Iterator<DBObject> resultsIterator;
    private final MongoDBStorageStrategy<RyaStatement> strategy;
    private Long maxResults;
    private final Authorizations auths;

    public RyaStatementCursorIterator(final DBCollection coll, final Set<DBObject> queries, final MongoDBStorageStrategy<RyaStatement> strategy, final Authorizations auths) {
        this.coll = coll;
        this.queryIterator = queries.iterator();
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
            final DBObject queryResult = resultsIterator.next();
            final RyaStatement statement = strategy.deserializeDBObject(queryResult);
            return statement;
        }
        return null;
    }

    private void findNextValidCursor() {
        while (queryIterator.hasNext()){
            final DBObject currentQuery = queryIterator.next();

            // Executing redact aggregation to only return documents the user
            // has access to.
            final List<DBObject> pipeline = new ArrayList<>();
            pipeline.add(new BasicDBObject("$match", currentQuery));
            pipeline.addAll(AggregationUtil.createRedactPipeline(auths));
            log.debug(pipeline);
            final AggregationOutput output = coll.aggregate(pipeline);
            resultsIterator = output.results().iterator();
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
