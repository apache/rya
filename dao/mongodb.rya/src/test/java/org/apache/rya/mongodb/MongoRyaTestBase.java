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

import org.apache.rya.api.persist.RyaDAOException;
import org.junit.After;
import org.junit.Before;

import com.mongodb.MongoClient;

import de.flapdoodle.embed.mongo.distribution.Version;

public class MongoRyaTestBase {

    protected MockMongoFactory testsFactory;
    protected MongoClient mongoClient;

    @Before
    public void MongoRyaTestBaseSetUp() throws IOException, RyaDAOException {
        testsFactory = MockMongoFactory.with(Version.Main.PRODUCTION);
        mongoClient = testsFactory.newMongoClient();
    }

    @After
    public void MongoRyaTestBaseAfter() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (testsFactory != null) {
            testsFactory.shutdown();
        }
        MongoConnectorFactory.closeMongoClient();
    }

}
