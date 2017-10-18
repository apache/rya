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

import java.util.*;
import java.util.Map.Entry;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.mongodb.DBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.util.JSON;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.dao.MongoDBStorageStrategy;
import org.apache.rya.mongodb.document.operators.aggregation.AggregationUtil;
import org.bson.Document;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.query.BindingSet;

public class RyaStatementBindingSetCursorIterator implements CloseableIteration<Entry<RyaStatement, BindingSet>, RyaDAOException> {
    private static final Logger log = Logger.getLogger(RyaStatementBindingSetCursorIterator.class);
    
    private static final int QUERY_BATCH_SIZE = 50;

    private final MongoCollection<Document> coll;
    private final Multimap<RyaStatement, BindingSet> rangeMap;
    private final Multimap<RyaStatement, BindingSet> executedRangeMap = HashMultimap.create();
    private final Iterator<RyaStatement> queryIterator;
    private Iterator<Document> batchQueryResultsIterator;
    private RyaStatement currentResultStatement;
    private Iterator<BindingSet> currentBindingSetIterator;
    private final MongoDBStorageStrategy<RyaStatement> strategy;
    private final Authorizations auths;

    public RyaStatementBindingSetCursorIterator(final MongoCollection<Document> coll,
            final Multimap<RyaStatement, BindingSet> rangeMap, final MongoDBStorageStrategy<RyaStatement> strategy,
            final Authorizations auths) {
        this.coll = coll;
        this.rangeMap = rangeMap;
        queryIterator = rangeMap.keySet().iterator();
        this.strategy = strategy;
        this.auths = auths;
    }

    @Override
    public boolean hasNext() {
        if (!currentBindingSetIteratorIsValid()) {
            findNextResult();
        }
        return currentBindingSetIteratorIsValid();
    }

    @Override
    public Entry<RyaStatement, BindingSet> next() {
        if (!currentBindingSetIteratorIsValid()) {
            findNextResult();
        }
        if (currentBindingSetIteratorIsValid()) {
            final BindingSet currentBindingSet = currentBindingSetIterator.next();
            return new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>(currentResultStatement, currentBindingSet);
        }
        return null;
    }

    private boolean currentBindingSetIteratorIsValid() {
        return (currentBindingSetIterator != null) && currentBindingSetIterator.hasNext();
    }

    private void findNextResult() {
        if (!currentBatchQueryResultCursorIsValid()) {
            submitBatchQuery();
        }
        
        if (currentBatchQueryResultCursorIsValid()) {
            // convert to Rya Statement
            final Document queryResult = batchQueryResultsIterator.next();
            final DBObject dbo = (DBObject) JSON.parse(queryResult.toJson());
            currentResultStatement = strategy.deserializeDBObject(dbo);
            
            // Find all of the queries in the executed RangeMap that this result matches
            // and collect all of those binding sets
            Set<BindingSet> bsList = new HashSet<>();
            for (RyaStatement executedQuery : executedRangeMap.keys()) {
                if (isResultForQuery(executedQuery, currentResultStatement)) {
                    bsList.addAll(executedRangeMap.get(executedQuery));
                }
            }
            currentBindingSetIterator = bsList.iterator();
        }
        
        // Handle case of invalid currentResultStatement or no binding sets returned
        if ((currentBindingSetIterator == null || !currentBindingSetIterator.hasNext()) && (currentBatchQueryResultCursorIsValid() || queryIterator.hasNext())) {
            findNextResult();
        }
    }
    
    private static boolean isResultForQuery(RyaStatement query, RyaStatement result) {
        return isResult(query.getSubject(), result.getSubject()) &&
                isResult(query.getPredicate(), result.getPredicate()) &&
                isResult(query.getObject(), result.getObject()) &&
                isResult(query.getContext(), result.getContext());
    }
    
    private static boolean isResult(RyaType query, RyaType result) {
        return (query == null) || query.equals(result);
    }

    private void submitBatchQuery() {
        int count = 0;
        executedRangeMap.clear();
        final List<Document> pipeline = new ArrayList<>();
        final List<DBObject> match = new ArrayList<>();

        while (queryIterator.hasNext() && count < QUERY_BATCH_SIZE){
            count++;
            RyaStatement query = queryIterator.next();
            executedRangeMap.putAll(query, rangeMap.get(query));
            final DBObject currentQuery = strategy.getQuery(query);
            match.add(currentQuery);
        }

        if (match.size() > 1) {
            pipeline.add(new Document("$match", new Document("$or", match)));
        } else if (match.size() == 1) {
            pipeline.add(new Document("$match", match.get(0)));
        } else {
            batchQueryResultsIterator = Iterators.emptyIterator();
            return;
        }
        
        // Executing redact aggregation to only return documents the user has access to.
        pipeline.addAll(AggregationUtil.createRedactPipeline(auths));
        log.info(pipeline);

        final AggregateIterable<Document> aggIter = coll.aggregate(pipeline);
        aggIter.batchSize(1000);
        batchQueryResultsIterator = aggIter.iterator();
    }

    private boolean currentBatchQueryResultCursorIsValid() {
        return (batchQueryResultsIterator != null) && batchQueryResultsIterator.hasNext();
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
