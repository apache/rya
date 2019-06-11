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
import static org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy.TIMESTAMP;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.export.api.metadata.MergeParentMetadata;
import org.apache.rya.export.api.metadata.ParentMetadataExistsException;
import org.apache.rya.export.api.store.AddStatementException;
import org.apache.rya.export.api.store.ContainsStatementException;
import org.apache.rya.export.api.store.RemoveStatementException;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.api.store.UpdateStatementException;
import org.apache.rya.export.mongo.parent.MongoParentMetadataRepository;
import org.apache.rya.mongodb.MongoDBRyaDAO;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

/**
 * Mongo implementation of {@link RyaStatementStore}.  Allows for exporting and
 * importing rya statements from MongoDB.
 */
public class MongoRyaStatementStore implements RyaStatementStore {
    private static final Logger logger = LoggerFactory.getLogger(MongoRyaStatementStore.class);

    public static final String TRIPLES_COLLECTION = "rya__triples";
    public static final String METADATA_COLLECTION = "parent_metadata";
    protected final SimpleMongoDBStorageStrategy adapter;
    protected final MongoDatabase db;

    private final String ryaInstanceName;
    private final MongoClient client;
    private final MongoDBRyaDAO dao;
    private final MongoParentMetadataRepository parentMetadataRepo;

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
        db = this.client.getDatabase(ryaInstanceName);
        adapter = new SimpleMongoDBStorageStrategy();
        parentMetadataRepo = new MongoParentMetadataRepository(client, ryaInstance);
    }

    @Override
    public Iterator<RyaStatement> fetchStatements() {
        final Document sortObj = new Document(TIMESTAMP, 1);
        final MongoCollection<Document> coll = db.getCollection(TRIPLES_COLLECTION);
        final List<RyaStatement> statements = new ArrayList<>();
        try (final MongoCursor<Document> cur = coll.find().sort(sortObj).iterator()) {
            while(cur.hasNext()) {
                final RyaStatement statement = adapter.deserializeDocument(cur.next());
                statements.add(statement);
            }
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
        final Document doc = adapter.serialize(statement);
        return db.getCollection(TRIPLES_COLLECTION).countDocuments(doc) > 0;
    }

    /**
     * @return The {@link MongoClient} to connect to mongo.
     */
    public MongoClient getClient() {
        return client;
    }

    @Override
    public void updateStatement(final RyaStatement original, final RyaStatement update) throws UpdateStatementException {
        //Since mongo does not support visibility, this does nothing for mongo.
        //Do not want a throw a not-implemented exception since that could potentially
        //break stuff.
    }

    @Override
    public Optional<MergeParentMetadata> getParentMetadata() {
        MergeParentMetadata metadata = null;
        try {
            metadata = parentMetadataRepo.get();
        } catch (final Exception e) {
            // Catching any exception to ensure we always return Optional.ofNullable(metadata).
            // Logging at the debug level if exceptional behavior needs to be investigated while deployed.
            logger.debug("Parent metadata missing or exceptional behavior occurred.", e);
        }
        return Optional.ofNullable(metadata);
    }

    @Override
    public void setParentMetadata(final MergeParentMetadata metadata) throws ParentMetadataExistsException {
        parentMetadataRepo.set(metadata);
    }
}