/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.mongodb;

import org.apache.hadoop.conf.Configuration;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;

/**
 * A base class that may be used when implementing Mongo DB tests that use the
 * JUnit framework.
 */
public class MongoTestBase {

    private static MongoClient mongoClient = null;
    protected static MongoDBRdfConfiguration conf;

    @Before
    public void setupTest() throws Exception {
        conf = new MongoDBRdfConfiguration( new Configuration() );
        conf.setBoolean("sc.useMongo", true);
        conf.setTablePrefix("test_");
        conf.setMongoDBName("testDB");
        mongoClient = EmbeddedMongoSingleton.getInstance();
        conf.setMongoClient(mongoClient);
    }

    @After
    public void cleanupTest() {
        // Remove any DBs that were created by the test.
        for(final String dbName : mongoClient.listDatabaseNames()) {
            mongoClient.dropDatabase(dbName);
        }
    }

    /**
     * @return A {@link MongoClient} that is connected to the embedded instance of Mongo DB.
     */
    public MongoClient getMongoClient() {
        return mongoClient;
    }

    /**
     * @return The Rya triples {@link MongoCollection}.
     */
    public MongoCollection<Document> getRyaCollection() {
        return mongoClient.getDatabase(conf.getMongoDBName()).getCollection(conf.getTriplesCollectionName());
    }

    /**
     * @return The Rya triples {@link DBCollection}.
     */
    public DBCollection getRyaDbCollection() {
        return mongoClient.getDB(conf.getMongoDBName()).getCollection(conf.getTriplesCollectionName());
    }
}