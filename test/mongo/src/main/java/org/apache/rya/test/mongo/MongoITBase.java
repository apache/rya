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
package org.apache.rya.test.mongo;

import java.net.UnknownHostException;

import org.junit.Before;

import com.mongodb.MongoClient;

/**
 * A base class that may be used when implementing Mongo DB tests that use the
 * JUnit framework.
 */
public class MongoITBase {
    private MongoClient mongoClient = null;

    @Before
    public void setupTest() throws Exception {
        mongoClient = EmbeddedMongoSingleton.getNewMongoClient();

        // Remove any DBs that were created by previous tests.
        for(final String dbName : mongoClient.listDatabaseNames()) {
            if (!MongoUtils.ADMIN_DATABASE_NAME.equals(dbName)) {
                mongoClient.getDatabase(dbName).drop();
            }
        }

        // Let subclasses do more setup.
        beforeTest();
    }

    /**
     * Invoked before each test is run to allow tests to perform more setup.
     */
    protected void beforeTest() throws Exception {
        // Does nothing by default.
    }

    /**
     * @return A {@link MongoClient} that is connected to the embedded instance of Mongo DB.
     */
    public MongoClient getMongoClient() {
        return mongoClient;
    }

    /**
     * @return The embedded Mongo DB's hostname.
     * @throws UnknownHostException The host couldn't be presented as a String.
     */
    public String getMongoHostname() throws UnknownHostException {
        return EmbeddedMongoSingleton.getMongodConfig().net().getServerAddress().getHostAddress();
    }

    /**
     * @return The embedded Mongo DB's port.
     */
    public int getMongoPort() {
        return EmbeddedMongoSingleton.getMongodConfig().net().getPort();
    }
}