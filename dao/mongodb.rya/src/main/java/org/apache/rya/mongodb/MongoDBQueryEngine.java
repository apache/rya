/**
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
package org.apache.rya.mongodb;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.query.BatchRyaQuery;
import org.apache.rya.api.persist.query.RyaQuery;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.mongodb.dao.MongoDBStorageStrategy;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.apache.rya.mongodb.iter.RyaStatementBindingSetCursorIterator;
import org.apache.rya.mongodb.iter.RyaStatementCursorIterator;
import org.bson.Document;
import org.calrissian.mango.collect.CloseableIterable;
import org.calrissian.mango.collect.CloseableIterables;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import info.aduna.iteration.CloseableIteration;

/**
 * Date: 7/17/12
 * Time: 9:28 AM
 */
public class MongoDBQueryEngine implements RyaQueryEngine<StatefulMongoDBRdfConfiguration> {

    private StatefulMongoDBRdfConfiguration configuration;
    private final MongoDBStorageStrategy<RyaStatement> strategy = new SimpleMongoDBStorageStrategy();

    @Override
    public void setConf(final StatefulMongoDBRdfConfiguration conf) {
        configuration = conf;
    }

    @Override
    public StatefulMongoDBRdfConfiguration getConf() {
        return configuration;
    }

    @Override
    public CloseableIteration<RyaStatement, RyaDAOException> query(
            final RyaStatement stmt, final StatefulMongoDBRdfConfiguration conf)
            throws RyaDAOException {
        Preconditions.checkNotNull(stmt);
        Preconditions.checkNotNull(conf);

        final Entry<RyaStatement, BindingSet> entry = new AbstractMap.SimpleEntry<>(stmt, new MapBindingSet());
        final Collection<Entry<RyaStatement, BindingSet>> collection = Collections.singleton(entry);

        return new RyaStatementCursorIterator(queryWithBindingSet(collection, conf));
    }

    @Override
    public CloseableIteration<? extends Entry<RyaStatement, BindingSet>, RyaDAOException> queryWithBindingSet(
            final Collection<Entry<RyaStatement, BindingSet>> stmts,
            final StatefulMongoDBRdfConfiguration conf) throws RyaDAOException {
        Preconditions.checkNotNull(stmts);
        Preconditions.checkNotNull(conf);

        final Multimap<RyaStatement, BindingSet> rangeMap = HashMultimap.create();

        //TODO: cannot span multiple tables here
        try {
            for (final Map.Entry<RyaStatement, BindingSet> stmtbs : stmts) {
                final RyaStatement stmt = stmtbs.getKey();
                final BindingSet bs = stmtbs.getValue();
                rangeMap.put(stmt, bs);
            }

            // TODO not sure what to do about regex ranges?
            final RyaStatementBindingSetCursorIterator iterator = new RyaStatementBindingSetCursorIterator(
                    getCollection(conf), rangeMap, strategy, conf.getAuthorizations());

            return iterator;
        } catch (final Exception e) {
            throw new RyaDAOException(e);
        }

    }
    @Override
    public CloseableIteration<RyaStatement, RyaDAOException> batchQuery(
            final Collection<RyaStatement> stmts, final StatefulMongoDBRdfConfiguration conf)
            throws RyaDAOException {
        final Map<RyaStatement, BindingSet> queries = new HashMap<>();

        for (final RyaStatement stmt : stmts) {
            queries.put(stmt, new MapBindingSet());
        }

        return new RyaStatementCursorIterator(queryWithBindingSet(queries.entrySet(), conf));
    }

    @Override
    public CloseableIterable<RyaStatement> query(final RyaQuery ryaQuery)
            throws RyaDAOException {
        Preconditions.checkNotNull(ryaQuery);

        return query(new BatchRyaQuery(Collections.singleton(ryaQuery.getQuery())));
    }

    @Override
    public CloseableIterable<RyaStatement> query(final BatchRyaQuery batchRyaQuery)
            throws RyaDAOException {
        Preconditions.checkNotNull(batchRyaQuery);

        final Map<RyaStatement, BindingSet> queries = new HashMap<>();

        for (final RyaStatement stmt : batchRyaQuery.getQueries()) {
            queries.put(stmt, new MapBindingSet());
        }

        final Iterator<RyaStatement> iterator = new RyaStatementCursorIterator(queryWithBindingSet(queries.entrySet(), getConf()));
        return CloseableIterables.wrap((Iterable<RyaStatement>) () -> iterator);
    }

    private MongoCollection<Document> getCollection(final StatefulMongoDBRdfConfiguration conf) {
        final MongoDatabase db = conf.getMongoClient().getDatabase(conf.getMongoDBName());
        return db.getCollection(conf.getTriplesCollectionName());
    }

    @Override
    public void close() throws IOException {
//        if (mongoClient != null){ mongoClient.close(); }
    }
}