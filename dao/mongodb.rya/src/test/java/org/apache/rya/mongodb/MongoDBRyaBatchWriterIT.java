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
package org.apache.rya.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.BasicConfigurator;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaStatement.RyaStatementBuilder;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.mongodb.batch.MongoDbBatchWriter;
import org.apache.rya.mongodb.batch.MongoDbBatchWriterConfig;
import org.apache.rya.mongodb.batch.MongoDbBatchWriterUtils;
import org.apache.rya.mongodb.batch.collection.DbCollectionType;
import org.apache.rya.mongodb.batch.collection.MongoCollectionType;
import org.apache.rya.mongodb.dao.MongoDBStorageStrategy;
import org.apache.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * Integration tests for the {@link MongoDbBatchWriter}.
 */
public class MongoDBRyaBatchWriterIT extends MongoTestBase {
    private MongoDBRyaDAO dao;

    private static void setupLogging() {
        BasicConfigurator.configure();
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        setupLogging();
    }

    @Before
    public void setUp() throws Exception {
        conf.setBoolean("rya.mongodb.dao.flusheachupdate", false);
        conf.setInt("rya.mongodb.dao.batchwriter.size", 50_000);
        conf.setLong("rya.mongodb.dao.batchwriter.flushtime", 100L);

        final MongoClient client = super.getMongoClient();
        dao = new MongoDBRyaDAO(conf, client);
    }

    @Test
    public void testDuplicateKeys() throws Exception {
        final List<RyaStatement> statements = new ArrayList<>();
        statements.add(statement(1));
        statements.add(statement(2));
        statements.add(statement(1));
        statements.add(statement(3));
        statements.add(statement(1));
        statements.add(statement(4));
        statements.add(statement(1));
        statements.add(statement(5));
        statements.add(statement(1));
        statements.add(statement(6));

        dao.add(statements.iterator());

        dao.flush();

        Assert.assertEquals(6, getRyaCollection().count());
    }

    @Test
    public void testDbCollectionFlush() throws Exception {
        final MongoDBStorageStrategy<RyaStatement> storageStrategy = new SimpleMongoDBStorageStrategy();

        final List<DBObject> objects = Lists.newArrayList(
                storageStrategy.serialize(statement(1)),
                storageStrategy.serialize(statement(2)),
                storageStrategy.serialize(statement(2)),
                null,
                storageStrategy.serialize(statement(3)),
                storageStrategy.serialize(statement(3)),
                storageStrategy.serialize(statement(4))
            );

        final DbCollectionType collectionType = new DbCollectionType(getRyaDbCollection());
        final MongoDbBatchWriterConfig mongoDbBatchWriterConfig = MongoDbBatchWriterUtils.getMongoDbBatchWriterConfig(conf);
        final MongoDbBatchWriter<DBObject> mongoDbBatchWriter = new MongoDbBatchWriter<DBObject>(collectionType, mongoDbBatchWriterConfig);

        mongoDbBatchWriter.start();
        mongoDbBatchWriter.addObjectsToQueue(objects);
        mongoDbBatchWriter.flush();
        Thread.sleep(1_000);
        mongoDbBatchWriter.addObjectsToQueue(objects);
        mongoDbBatchWriter.flush();
        Thread.sleep(1_000);
        mongoDbBatchWriter.shutdown();
        Assert.assertEquals(4, getRyaDbCollection().count());
    }

    @Test
    public void testMongoCollectionFlush() throws Exception {
        final MongoDBStorageStrategy<RyaStatement> storageStrategy = new SimpleMongoDBStorageStrategy();

        final List<Document> documents = Lists.newArrayList(
                toDocument(storageStrategy.serialize(statement(1))),
                toDocument(storageStrategy.serialize(statement(2))),
                toDocument(storageStrategy.serialize(statement(2))),
                null,
                toDocument(storageStrategy.serialize(statement(3))),
                toDocument(storageStrategy.serialize(statement(3))),
                toDocument(storageStrategy.serialize(statement(4)))
            );

        final MongoCollectionType mongoCollectionType = new MongoCollectionType(getRyaCollection());
        final MongoDbBatchWriterConfig mongoDbBatchWriterConfig = MongoDbBatchWriterUtils.getMongoDbBatchWriterConfig(conf);
        final MongoDbBatchWriter<Document> mongoDbBatchWriter = new MongoDbBatchWriter<Document>(mongoCollectionType, mongoDbBatchWriterConfig);

        mongoDbBatchWriter.start();
        mongoDbBatchWriter.addObjectsToQueue(documents);
        mongoDbBatchWriter.flush();
        Thread.sleep(1_000);
        mongoDbBatchWriter.addObjectsToQueue(documents);
        mongoDbBatchWriter.flush();
        Thread.sleep(1_000);
        mongoDbBatchWriter.shutdown();
        Assert.assertEquals(4, getRyaCollection().count());
    }

    private static Document toDocument(final DBObject dbObject) {
        if (dbObject == null) {
            return null;
        }
        final Document document = Document.parse(dbObject.toString());
        return document;
    }

    private static RyaURI ryaURI(final int v) {
        return new RyaURI("u:" + v);
    }

    private static RyaStatement statement(final int v) {
        final RyaStatementBuilder builder = new RyaStatementBuilder();
        builder.setPredicate(ryaURI(v));
        builder.setSubject(ryaURI(v));
        builder.setObject(ryaURI(v));
        return builder.build();
    }
}
