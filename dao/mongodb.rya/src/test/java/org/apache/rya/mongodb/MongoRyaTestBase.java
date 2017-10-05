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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rya.api.persist.RyaDAOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.mongodb.MongoClient;

import de.flapdoodle.embed.mongo.distribution.Version;

public class MongoRyaTestBase {

    private static final AtomicInteger db = new AtomicInteger(1);

    protected static EmbeddedMongoFactory testsFactory;
    protected MongoClient mongoClient;
    private int currentTestDb = -1;

    @BeforeClass()
    public static void beforeClass() throws Exception {
        testsFactory = EmbeddedMongoFactory.with(Version.Main.PRODUCTION);
    }

    @Before
    public void MongoRyaTestBaseSetUp() throws IOException, RyaDAOException {
        mongoClient = testsFactory.newMongoClient();
        currentTestDb = db.getAndIncrement();
    }

    @After
    public void MongoRyaTestBaseAfter() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
        currentTestDb = -1;
        MongoConnectorFactory.closeMongoClient();
    }

    @AfterClass()
    public static void afterClass() throws Exception {
        if (testsFactory != null) {
            testsFactory.shutdown();
        }
    }

    public String getDbName() {
        return "rya_" + currentTestDb;
    }

}
