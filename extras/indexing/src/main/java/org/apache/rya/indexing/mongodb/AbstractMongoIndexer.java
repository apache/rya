package org.apache.rya.indexing.mongodb;

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

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.mongodb.MongoConnectorFactory;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.MongoSecondaryIndex;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

import info.aduna.iteration.CloseableIteration;

/**
 * Secondary Indexer using MondoDB
 * @param <T> - The {@link AbstractMongoIndexingStorageStrategy} this indexer uses.
 */
public abstract class AbstractMongoIndexer<T extends IndexingMongoDBStorageStrategy> implements MongoSecondaryIndex {
    private static final Logger LOG = Logger.getLogger(AbstractMongoIndexer.class);

    private boolean isInit = false;
    protected Configuration conf;
    protected MongoDBRyaDAO dao;
    protected MongoClient mongoClient;
    protected String dbName;
    protected DB db;
    protected DBCollection collection;
    protected Set<URI> predicates;

    protected T storageStrategy;

    protected void initCore() {
        dbName = conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME);
        db = this.mongoClient.getDB(dbName);
        collection = db.getCollection(conf.get(MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX, "rya") + getCollectionName());
    }

    @Override
    public void setClient(final MongoClient client){
    	this.mongoClient = client;
    }

    @VisibleForTesting
    public void initIndexer(final Configuration conf, final MongoClient client) {
        setClient(client);
        final ServerAddress address = client.getAddress();
        conf.set(MongoDBRdfConfiguration.MONGO_INSTANCE, address.getHost());
        conf.set(MongoDBRdfConfiguration.MONGO_INSTANCE_PORT, Integer.toString(address.getPort()));
        setConf(conf);
        if (!isInit) {
            init();
            isInit = true;
        }
    }

    @Override
    public void setConf(final Configuration conf) {
        this.conf = conf;
        if (!isInit){
        	setClient(MongoConnectorFactory.getMongoClient(conf));
        	init();
        }
    }

    @Override
    public void close() throws IOException {
        mongoClient.close();
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public String getTableName() {
        return dbName;
    }

    @Override
    public Set<URI> getIndexablePredicates() {
        return predicates;
    }

    @Override
    public void deleteStatement(final RyaStatement stmt) throws IOException {
       final DBObject obj = storageStrategy.getQuery(stmt);
       collection.remove(obj);
    }

    @Override
    public void storeStatements(final Collection<RyaStatement> ryaStatements)
            throws IOException {
        for (final RyaStatement ryaStatement : ryaStatements){
            storeStatement(ryaStatement);
        }
    }

    @Override
    public void storeStatement(final RyaStatement ryaStatement) throws IOException {
        try {
            final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
            final boolean isValidPredicate = predicates.isEmpty() || predicates.contains(statement.getPredicate());
            if (isValidPredicate && (statement.getObject() instanceof Literal)) {
                final DBObject obj = storageStrategy.serialize(ryaStatement);
                if (obj != null) {
                    collection.insert(obj, WriteConcern.ACKNOWLEDGED);
                }
            }
        } catch (final IllegalArgumentException e) {
            LOG.error("Unable to parse the statement: " + ryaStatement.toString());
        }
    }

    @Override
    public void dropGraph(final RyaURI... graphs) {
        throw new UnsupportedOperationException();
    }

    protected CloseableIteration<Statement, QueryEvaluationException> withConstraints(final StatementConstraints constraints, final DBObject preConstraints) {
        final DBObject dbo = QueryBuilder.start().and(preConstraints).and(storageStrategy.getQuery(constraints)).get();
        return closableIterationFromCursor(dbo);
    }

    private CloseableIteration<Statement, QueryEvaluationException> closableIterationFromCursor(final DBObject dbo) {
        final DBCursor cursor = collection.find(dbo);
        return new CloseableIteration<Statement, QueryEvaluationException>() {
            @Override
            public boolean hasNext() {
                return cursor.hasNext();
            }

            @Override
            public Statement next() throws QueryEvaluationException {
                final DBObject dbo = cursor.next();
                return RyaToRdfConversions.convertStatement(storageStrategy.deserializeDBObject(dbo));
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove not implemented");
            }

            @Override
            public void close() throws QueryEvaluationException {
                cursor.close();
            }
        };
    }

    /**
     * @return The name of the {@link DBCollection} to use with the storage strategy.
     */
    public abstract String getCollectionName();
}
