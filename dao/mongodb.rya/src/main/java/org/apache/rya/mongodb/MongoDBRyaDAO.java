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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.persist.RyaDAO;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.persist.RyaNamespaceManager;
import org.apache.rya.api.persist.index.RyaSecondaryIndexer;
import org.apache.rya.api.persist.query.RyaQueryEngine;
import org.apache.rya.mongodb.batch.MongoDbBatchWriter;
import org.apache.rya.mongodb.batch.MongoDbBatchWriterConfig;
import org.apache.rya.mongodb.batch.MongoDbBatchWriterException;
import org.apache.rya.mongodb.batch.MongoDbBatchWriterUtils;
import org.apache.rya.mongodb.batch.collection.DbCollectionType;
import org.apache.rya.mongodb.dao.MongoDBNamespaceManager;
import org.apache.rya.mongodb.dao.MongoDBStorageStrategy;
import org.apache.rya.mongodb.dao.SimpleMongoDBNamespaceManager;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.apache.rya.mongodb.document.util.DocumentVisibilityUtil;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoClient;

/**
 * Default DAO for mongo backed RYA allowing for CRUD operations.
 */
public final class MongoDBRyaDAO implements RyaDAO<StatefulMongoDBRdfConfiguration>{
    private static final Logger log = Logger.getLogger(MongoDBRyaDAO.class);

    private final AtomicBoolean isInitialized = new AtomicBoolean();
    private final AtomicBoolean flushEachUpdate = new AtomicBoolean(true);
    private StatefulMongoDBRdfConfiguration conf;
    private MongoClient mongoClient;
    private DB db;
    private DBCollection coll;
    private MongoDBQueryEngine queryEngine;
    private MongoDBStorageStrategy<RyaStatement> storageStrategy;
    private MongoDBNamespaceManager nameSpaceManager;

    private List<MongoSecondaryIndex> secondaryIndexers;
    private Authorizations auths;

    private MongoDbBatchWriter<DBObject> mongoDbBatchWriter;

    @Override
    public synchronized void setConf(final StatefulMongoDBRdfConfiguration conf) {
        this.conf = requireNonNull(conf);
        mongoClient = this.conf.getMongoClient();
        auths = conf.getAuthorizations();
        flushEachUpdate.set(conf.flushEachUpdate());
    }
  
    
    public void setDB(final DB db) {
        this.db = db;
    }

    public void setDBCollection(final DBCollection coll) {
        this.coll = coll;
    }

    @Override
    public synchronized StatefulMongoDBRdfConfiguration getConf() {
        return conf;
    }

    @Override
    public void init() throws RyaDAOException {
        if (isInitialized.get()) {
            return;
        }
        secondaryIndexers = conf.getAdditionalIndexers();
        for(final MongoSecondaryIndex index: secondaryIndexers) {
            index.setConf(conf);
        }

        db = mongoClient.getDB(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
        coll = db.getCollection(conf.getTriplesCollectionName());
        nameSpaceManager = new SimpleMongoDBNamespaceManager(db.getCollection(conf.getNameSpacesCollectionName()));
        queryEngine = new MongoDBQueryEngine();
        queryEngine.setConf(conf);
        storageStrategy = new SimpleMongoDBStorageStrategy();
        storageStrategy.createIndices(coll);
        for(final MongoSecondaryIndex index: secondaryIndexers) {
            index.init();
        }

        final MongoDbBatchWriterConfig mongoDbBatchWriterConfig = MongoDbBatchWriterUtils.getMongoDbBatchWriterConfig(conf);
        mongoDbBatchWriter = new MongoDbBatchWriter<>(new DbCollectionType(coll), mongoDbBatchWriterConfig);
        try {
            mongoDbBatchWriter.start();
        } catch (final MongoDbBatchWriterException e) {
            throw new RyaDAOException("Error starting MongoDB batch writer", e);
        }
        isInitialized.set(true);
    }

    @Override
    public boolean isInitialized() throws RyaDAOException {
        return isInitialized.get();
    }

    @Override
    public void destroy() throws RyaDAOException {
        if (!isInitialized.get()) {
            return;
        }
        isInitialized.set(false);
        flush();
        try {
            mongoDbBatchWriter.shutdown();
        } catch (final MongoDbBatchWriterException e) {
            throw new RyaDAOException("Error shutting down MongoDB batch writer", e);
        }
        for(final MongoSecondaryIndex indexer : secondaryIndexers) {
            try {
                indexer.close();
            } catch (final IOException e) {
                log.error("Error closing indexer: " + indexer.getClass().getSimpleName(), e);
            }
        }

        IOUtils.closeQuietly(queryEngine);
    }

    @Override
    public void add(final RyaStatement statement) throws RyaDAOException {
        // add it to the collection
        try {
            final boolean canAdd = DocumentVisibilityUtil.doesUserHaveDocumentAccess(auths, statement.getColumnVisibility());
            if (canAdd) {
                final DBObject obj = storageStrategy.serialize(statement);
                try {
                    mongoDbBatchWriter.addObjectToQueue(obj);
                    if (flushEachUpdate.get()) {
                        flush();
                    }
                } catch (final MongoDbBatchWriterException e) {
                    throw new RyaDAOException("Error adding statement", e);
                }
                for(final RyaSecondaryIndexer index: secondaryIndexers) {
                    index.storeStatement(statement);
                }
            } else {
                throw new RyaDAOException("User does not have the required authorizations to add statement");
            }
        } catch (final IOException e) {
            log.error("Unable to add: " + statement.toString());
            throw new RyaDAOException(e);
        }
        catch (final DuplicateKeyException e){
            log.error("Attempting to load duplicate triple: " + statement.toString());
        }
    }

    @Override
    public void add(final Iterator<RyaStatement> statementIter) throws RyaDAOException {
        final List<DBObject> dbInserts = new ArrayList<>();
        while (statementIter.hasNext()){
            final RyaStatement ryaStatement = statementIter.next();
            final boolean canAdd = DocumentVisibilityUtil.doesUserHaveDocumentAccess(auths, ryaStatement.getColumnVisibility());
            if (canAdd) {
                final DBObject insert = storageStrategy.serialize(ryaStatement);
                dbInserts.add(insert);

                try {
                    for (final RyaSecondaryIndexer index : secondaryIndexers) {
                        index.storeStatement(ryaStatement);
                    }
                } catch (final IOException e) {
                    log.error("Failed to add: " + ryaStatement.toString() + " to the indexer");
                }
            } else {
                throw new RyaDAOException("User does not have the required authorizations to add statement");
            }
        }
        try {
            mongoDbBatchWriter.addObjectsToQueue(dbInserts);
            if (flushEachUpdate.get()) {
                flush();
            }
        } catch (final MongoDbBatchWriterException e) {
            throw new RyaDAOException("Error adding statements", e);
        }
    }

    @Override
    public void delete(final RyaStatement statement, final StatefulMongoDBRdfConfiguration conf)
            throws RyaDAOException {
        final boolean canDelete = DocumentVisibilityUtil.doesUserHaveDocumentAccess(auths, statement.getColumnVisibility());
        if (canDelete) {
            final DBObject obj = storageStrategy.getQuery(statement);
            coll.remove(obj);
            for (final RyaSecondaryIndexer index : secondaryIndexers) {
                try {
                    index.deleteStatement(statement);
                } catch (final IOException e) {
                    log.error("Unable to remove statement: " + statement.toString() + " from secondary indexer: " + index.getTableName(), e);
                }
            }
        } else {
            throw new RyaDAOException("User does not have the required authorizations to delete statement");
        }
    }

    @Override
    public void dropGraph(final StatefulMongoDBRdfConfiguration conf, final RyaIRI... graphs)
            throws RyaDAOException {

    }

    @Override
    public void delete(final Iterator<RyaStatement> statements,
            final StatefulMongoDBRdfConfiguration conf) throws RyaDAOException {
        while (statements.hasNext()){
            final RyaStatement ryaStatement = statements.next();
            final boolean canDelete = DocumentVisibilityUtil.doesUserHaveDocumentAccess(auths, ryaStatement.getColumnVisibility());
            if (canDelete) {
                coll.remove(storageStrategy.getQuery(ryaStatement));
                for (final RyaSecondaryIndexer index : secondaryIndexers) {
                    try {
                        index.deleteStatement(ryaStatement);
                    } catch (final IOException e) {
                        log.error("Unable to remove statement: " + ryaStatement.toString() + " from secondary indexer: " + index.getTableName(), e);
                    }
                }
            } else {
                throw new RyaDAOException("User does not have the required authorizations to delete statement");
            }
        }
    }

    @Override
    public String getVersion() throws RyaDAOException {
        return "1.0";
    }

    @Override
    public RyaQueryEngine<StatefulMongoDBRdfConfiguration> getQueryEngine() {
        return queryEngine;
    }

    @Override
    public RyaNamespaceManager<StatefulMongoDBRdfConfiguration> getNamespaceManager() {
        return nameSpaceManager;
    }

    @Override
    public void purge(final RdfCloudTripleStoreConfiguration configuration) {

    }

    @Override
    public void dropAndDestroy() throws RyaDAOException {
        db.dropDatabase(); // this is dangerous!
    }

    @Override
    public void flush() throws RyaDAOException {
        try {
            mongoDbBatchWriter.flush();
            flushIndexers();
        } catch (final MongoDbBatchWriterException e) {
            throw new RyaDAOException("Error flushing data.", e);
        }
    }

    private void flushIndexers() throws RyaDAOException {
        for (final MongoSecondaryIndex indexer : secondaryIndexers) {
            try {
                indexer.flush();
            } catch (final IOException e) {
                log.error("Error flushing data in indexer: " + indexer.getClass().getSimpleName(), e);
            }
        }
    }
}