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
package org.apache.rya.indexing.entity.update.mongo;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.entity.storage.EntityStorage;
import org.apache.rya.indexing.entity.storage.TypeStorage;
import org.apache.rya.indexing.entity.storage.mongo.MongoEntityStorage;
import org.apache.rya.indexing.entity.storage.mongo.MongoTypeStorage;
import org.apache.rya.indexing.entity.update.BaseEntityIndexer;
import org.apache.rya.indexing.entity.update.EntityIndexer;

import com.mongodb.MongoClient;

import mvm.rya.mongodb.MongoConnectorFactory;
import mvm.rya.mongodb.MongoDBRdfConfiguration;

/**
 * A Mongo DB implementation of {@link EntityIndexer}.
 */
@ParametersAreNonnullByDefault
public class MongoEntityIndexer extends BaseEntityIndexer {

    @Override
    public EntityStorage getEntityStorage(Configuration conf) {
        final MongoClient mongoClient = MongoConnectorFactory.getMongoClient(conf);
        final String ryaInstanceName = new MongoDBRdfConfiguration(conf).getMongoDBName();
        return new MongoEntityStorage(mongoClient, ryaInstanceName);
    }

    @Override
    public TypeStorage getTypeStorage(Configuration conf) {
        final MongoClient mongoClient = MongoConnectorFactory.getMongoClient(conf);
        final String ryaInstanceName = new MongoDBRdfConfiguration(conf).getMongoDBName();
        return new MongoTypeStorage(mongoClient, ryaInstanceName);
    }
}