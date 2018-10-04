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
package org.apache.rya.indexing.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.rya.indexing.mongodb.MongoIndexingConfiguration;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
import org.junit.Test;

public class MongoIndexingConfigurationTest {

    @Test
    public void testBuilder() {
        final String prefix = "prefix_";
        final String auth = "U,V,W";
        final String visibility = "U,W";
        final String user = "user";
        final String password = "password";
        final boolean useMock = true;
        final boolean useInference = true;
        final boolean displayPlan = false;

        final MongoIndexingConfiguration conf = MongoIndexingConfiguration.builder()
                .setVisibilities(visibility)
                .setUseInference(useInference)
                .setDisplayQueryPlan(displayPlan)
                .setUseMockMongo(useMock)
                .setMongoDBName("dbname")
                .setMongoHost("host")
                .setMongoPort("1000")
                .setAuths(auth)
                .setMongoUser(user)
                .setMongoPassword(password)
                .setUseMongoEntityIndex(true)
                .setUseMongoFreetextIndex(true)
                .setUseMongoTemporalIndex(true)
                .setMongoFreeTextPredicates("http://pred1", "http://pred2")
                .setMongoTemporalPredicates("http://pred3", "http://pred4")
                .build();

        assertTrue(Arrays.equals(conf.getAuths(), new String[] { "U", "V", "W" }));
        assertEquals(conf.getCv(), visibility);
        assertEquals(conf.isInfer(), useInference);
        assertEquals(conf.isDisplayQueryPlan(), displayPlan);
        assertEquals(conf.getMongoHostname(), "host");
        assertEquals(conf.getBoolean(".useMockInstance", false), useMock);
        assertEquals(conf.getMongoPort(), "1000");
        assertEquals(conf.getRyaInstanceName(), "dbname");
        assertEquals(conf.get(MongoDBRdfConfiguration.MONGO_USER), user);
        assertEquals(conf.get(MongoDBRdfConfiguration.MONGO_USER_PASSWORD), password);
        assertTrue(
                Arrays.equals(conf.getMongoFreeTextPredicates(), new String[] { "http://pred1", "http://pred2" }));
        assertTrue(
                Arrays.equals(conf.getMongoTemporalPredicates(), new String[] { "http://pred3", "http://pred4" }));

    }

    @Test
    public void testBuilderFromProperties() throws FileNotFoundException, IOException {
        final String auth = "U";
        final String visibility = "U";
        final String user = "user";
        final String password = "password";
        final boolean useMock = true;
        final boolean useInference = true;
        final boolean displayPlan = false;

        final Properties props = new Properties();
        props.load(new FileInputStream("src/test/resources/mongo_rya_indexing.properties"));

        final MongoIndexingConfiguration conf = MongoIndexingConfiguration.fromProperties(props);

        assertTrue(Arrays.equals(conf.getAuths(), new String[] { auth }));
        assertEquals(conf.getCv(), visibility);
        assertEquals(conf.isInfer(), useInference);
        assertEquals(conf.isDisplayQueryPlan(), displayPlan);
        assertEquals(conf.getMongoHostname(), "host");
        assertEquals(conf.getBoolean(".useMockInstance", false), useMock);
        assertEquals(conf.getMongoPort(), "1000");
        assertEquals(conf.getRyaInstanceName(), "dbname");
        assertEquals(conf.get(MongoDBRdfConfiguration.MONGO_USER), user);
        assertEquals(conf.get(MongoDBRdfConfiguration.MONGO_USER_PASSWORD), password);
        assertTrue(
                Arrays.equals(conf.getMongoFreeTextPredicates(), new String[] { "http://pred1", "http://pred2" }));
        assertTrue(
                Arrays.equals(conf.getMongoTemporalPredicates(), new String[] { "http://pred3", "http://pred4" }));
    }

}
