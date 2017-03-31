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
package org.apache.rya.indexing.geotemporal.mongo;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.geotemporal.GeoTemporalTestBase;
import org.apache.rya.mongodb.MockMongoFactory;
import org.apache.rya.mongodb.MongoConnectorFactory;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;

import com.mongodb.MongoClient;

/**
 * A base class that may be used when implementing Mongo DB integration tests that
 * use the JUnit framework.
 */
public class MongoITBase extends GeoTemporalTestBase {
    protected final MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration( new Configuration() );

    private MongoClient mongoClient = null;
    private Set<String> originalDbNames = null;

    @Before
    public void setupTest() throws Exception {
        conf.setMongoDBName("testDB");
        conf.setMongoInstance("testDB");
        conf.setMongoPort("27017");

        mongoClient = MockMongoFactory.newFactory().newMongoClient();
        conf.setMongoClient(mongoClient);;
        // Store the names of the DBs that are present before running the test.
        originalDbNames = new HashSet<>();
        for(final String name : mongoClient.listDatabaseNames()) {
            originalDbNames.add(name);
        }
    }

    @After
    public void cleanupTest() {
        // Remove any DBs that were created by the test.
        for(final String dbName : mongoClient.listDatabaseNames()) {
            if(!originalDbNames.contains(dbName)) {
                mongoClient.dropDatabase(dbName);
            }
        }
    }

    @AfterClass
    public static void shutdown() {
        MongoConnectorFactory.closeMongoClient();
    }

    /**
     * @return A {@link MongoClient} that is connected to the embedded instance of Mongo DB.
     */
    public MongoClient getMongoClient() {
        return mongoClient;
    }
}