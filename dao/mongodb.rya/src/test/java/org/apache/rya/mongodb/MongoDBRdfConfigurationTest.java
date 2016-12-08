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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.junit.Test;

public class MongoDBRdfConfigurationTest {

    @Test
    public void testBuilder() {
        String prefix = "prefix_";
        String auth = "U,V,W";
        String visibility = "U,W";
        String user = "user";
        String password = "password";
        boolean useMock = true;
        boolean useInference = true;
        boolean displayPlan = false;

        MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration().getBuilder()
                .setVisibilities(visibility)
                .setUseInference(useInference)
                .setDisplayQueryPlan(displayPlan)
                .setUseMockMongo(useMock)
                .setMongoCollectionPrefix(prefix)
                .setMongoDBName("dbname")
                .setMongoHost("host")
                .setMongoPort("1000")
                .setAuths(auth)
                .setMongoUser(user)
                .setMongoPassword(password).build();

        assertEquals(conf.getTablePrefix(), prefix);
        assertTrue(Arrays.equals(conf.getAuths(), new String[] { "U", "V", "W" }));
        assertEquals(conf.getCv(), visibility);
        assertEquals(conf.isInfer(), useInference);
        assertEquals(conf.isDisplayQueryPlan(), displayPlan);
        assertEquals(conf.getMongoInstance(), "host");
        assertEquals(conf.getBoolean(".useMockInstance", false), useMock);
        assertEquals(conf.getMongoPort(), "1000");
        assertEquals(conf.getMongoDBName(), "dbname");
        assertEquals(conf.getCollectionName(), "prefix_");
        assertEquals(conf.get(MongoDBRdfConfiguration.MONGO_USER), user);
        assertEquals(conf.get(MongoDBRdfConfiguration.MONGO_USER_PASSWORD), password);

    }

    @Test
    public void testBuilderFromProperties() throws FileNotFoundException, IOException {
        String prefix = "prefix_";
        String auth = "U";
        String visibility = "U";
        String user = "user";
        String password = "password";
        boolean useMock = true;
        boolean useInference = true;
        boolean displayPlan = false;

        Properties props = new Properties();
        props.load(new FileInputStream("src/test/resources/rya.properties"));

        MongoDBRdfConfiguration conf = MongoDBRdfConfiguration.fromProperties(props);

        assertEquals(conf.getTablePrefix(), prefix);
        assertTrue(Arrays.equals(conf.getAuths(), new String[] { auth }));
        assertEquals(conf.getCv(), visibility);
        assertEquals(conf.isInfer(), useInference);
        assertEquals(conf.isDisplayQueryPlan(), displayPlan);
        assertEquals(conf.getMongoInstance(), "host");
        assertEquals(conf.getBoolean(".useMockInstance", false), useMock);
        assertEquals(conf.getMongoPort(), "1000");
        assertEquals(conf.getMongoDBName(), "dbname");
        assertEquals(conf.getCollectionName(), "prefix_");
        assertEquals(conf.get(MongoDBRdfConfiguration.MONGO_USER), user);
        assertEquals(conf.get(MongoDBRdfConfiguration.MONGO_USER_PASSWORD), password);
    }

}
