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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.test.mongo.MongoITBase;
import org.bson.Document;

import com.mongodb.DBCollection;
import com.mongodb.client.MongoCollection;

/**
 * A base class that may be used when implementing Mongo DB tests that use the
 * JUnit framework to talk to an instance of Rya. The configuration file, which contains
 * state information, is protected so test classes may access it directly.
 */
public class MongoRyaITBase extends MongoITBase {

    protected StatefulMongoDBRdfConfiguration conf;

    @Override
    protected void beforeTest() throws Exception {
        // Setup the configuration that will be used within the test.
        final MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration( new Configuration() );
        conf.setBoolean("sc.useMongo", true);
        conf.setRyaInstanceName("mongo_test");
        conf.setMongoHostname(getMongoHostname());
        conf.setMongoPort(getMongoPort() + "");

        // Let tests update the configuration.
        updateConfiguration(conf);

        // Create the stateful configuration object.
        final List<MongoSecondaryIndex> indexers = conf.getInstances("ac.additional.indexers", MongoSecondaryIndex.class);
        this.conf = new StatefulMongoDBRdfConfiguration(conf, getMongoClient(), indexers);
    }

    /**
     * Override this method if you would like to augment the configuration object that
     * will be used to initialize indexers and create the mongo client prior to running a test.
     *
     * @param conf - The configuration object that may be updated. (not null)
     */
    protected void updateConfiguration(final MongoDBRdfConfiguration conf) {
        // By default, do nothing.
    }

    /**
     * @return The Rya triples {@link MongoCollection}.
     */
    public MongoCollection<Document> getRyaCollection() {
        return getMongoClient().getDatabase(conf.getMongoDBName()).getCollection(conf.getTriplesCollectionName());
    }

    /**
     * @return The Rya triples {@link DBCollection}.
     */
    public DBCollection getRyaDbCollection() {
        return getMongoClient().getDB(conf.getMongoDBName()).getCollection(conf.getTriplesCollectionName());
    }
}
