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
package org.apache.rya.indexing.mongodb;

import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.MongoSecondaryIndex;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.apache.rya.mongodb.batch.MongoDbBatchWriter;
import org.apache.rya.mongodb.batch.MongoDbBatchWriterConfig;
import org.apache.rya.mongodb.batch.MongoDbBatchWriterException;
import org.apache.rya.mongodb.batch.MongoDbBatchWriterUtils;
import org.apache.rya.mongodb.batch.collection.DbCollectionType;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.QueryEvaluationException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;

/**
 * Secondary Indexer using MondoDB
 * @param <T> - The {@link AbstractMongoIndexingStorageStrategy} this indexer uses.
 */
public abstract class AbstractMongoIndexer<T extends IndexingMongoDBStorageStrategy> implements MongoSecondaryIndex {
    private static final Logger LOG = Logger.getLogger(AbstractMongoIndexer.class);

    private boolean isInit = false;
    private boolean flushEachUpdate = true;
    protected StatefulMongoDBRdfConfiguration conf;
    protected MongoDBRyaDAO dao;
    protected MongoClient mongoClient;
    protected String dbName;
    protected DB db;
    protected DBCollection collection;
    protected Set<IRI> predicates;

    protected T storageStrategy;

    private MongoDbBatchWriter<DBObject> mongoDbBatchWriter;

    protected void initCore() {
        dbName = conf.getMongoDBName();
        this.mongoClient = conf.getMongoClient();
        db = this.mongoClient.getDB(dbName);
        final String collectionName = conf.get(MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX, "rya") + getCollectionName();
        collection = db.getCollection(collectionName);

        flushEachUpdate = ((MongoDBRdfConfiguration)conf).flushEachUpdate();

        final MongoDbBatchWriterConfig mongoDbBatchWriterConfig = MongoDbBatchWriterUtils.getMongoDbBatchWriterConfig(conf);
        mongoDbBatchWriter = new MongoDbBatchWriter<>(new DbCollectionType(collection), mongoDbBatchWriterConfig);
        try {
            mongoDbBatchWriter.start();
        } catch (final MongoDbBatchWriterException e) {
            LOG.error("Error start MongoDB batch writer", e);
        }
    }

    @Override
    public void setConf(final Configuration conf) {
        checkState(conf instanceof StatefulMongoDBRdfConfiguration,
                "The provided Configuration must be a StatefulMongoDBRdfConfiguration, but it was " + conf.getClass().getName());
        this.conf = (StatefulMongoDBRdfConfiguration) conf;
    }

    @Override
    public void close() throws IOException {
        flush();
        try {
            mongoDbBatchWriter.shutdown();
        } catch (final MongoDbBatchWriterException e) {
            throw new IOException("Error shutting down MongoDB batch writer", e);
        }
    }

    @Override
    public void flush() throws IOException {
        try {
            mongoDbBatchWriter.flush();
        } catch (final MongoDbBatchWriterException e) {
            throw new IOException("Error flushing batch writer", e);
        }
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
    public Set<IRI> getIndexablePredicates() {
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
            storeStatement(ryaStatement, false);
        }
        if (flushEachUpdate) {
            flush();
        }
    }

    @Override
    public void storeStatement(final RyaStatement ryaStatement) throws IOException {
        storeStatement(ryaStatement, flushEachUpdate);
    }

    private void storeStatement(final RyaStatement ryaStatement, final boolean flush) throws IOException {
        final DBObject obj = prepareStatementForStorage(ryaStatement);
        try {
            mongoDbBatchWriter.addObjectToQueue(obj);
            if (flush) {
                flush();
            }
        } catch (final MongoDbBatchWriterException e) {
            throw new IOException("Error storing statement", e);
        }
    }

    private DBObject prepareStatementForStorage(final RyaStatement ryaStatement) {
        try {
            final Statement statement = RyaToRdfConversions.convertStatement(ryaStatement);
            final boolean isValidPredicate = predicates.isEmpty() || predicates.contains(statement.getPredicate());
            if (isValidPredicate && (statement.getObject() instanceof Literal)) {
                final DBObject obj = storageStrategy.serialize(ryaStatement);
                return obj;
            }
        } catch (final IllegalArgumentException e) {
            LOG.error("Unable to parse the statement: " + ryaStatement.toString(), e);
        }

        return null;
    }

    @Override
    public void dropGraph(final RyaIRI... graphs) {
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
