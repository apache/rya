package org.apache.rya.mongodb;

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



import java.util.List;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.persist.index.RyaSecondaryIndexer;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;
import com.mongodb.MongoClient;

public class MongoDBRdfConfiguration extends RdfCloudTripleStoreConfiguration {
    public static final String MONGO_INSTANCE = "mongo.db.instance";
    public static final String MONGO_INSTANCE_PORT = "mongo.db.port";
    public static final String MONGO_GEO_MAXDISTANCE = "mongo.geo.maxdist";
    public static final String MONGO_DB_NAME = "mongo.db.name";
    public static final String MONGO_COLLECTION_PREFIX = "mongo.db.collectionprefix";
    public static final String MONGO_USER = "mongo.db.user";
    public static final String  MONGO_USER_PASSWORD = "mongo.db.userpassword";
    public static final String USE_TEST_MONGO = "mongo.db.test";
    public static final String CONF_ADDITIONAL_INDEXERS = "ac.additional.indexers";
	private MongoClient mongoClient;

    public MongoDBRdfConfiguration() {
        super();
    }

    public MongoDBRdfConfiguration(Configuration other) {
        super(other);
    }

    @Override
    public MongoDBRdfConfiguration clone() {
        return new MongoDBRdfConfiguration(this);
    }

    public boolean getUseTestMongo() {
        return this.getBoolean(USE_TEST_MONGO, false);
    }

    public void setUseTestMongo(boolean useTestMongo) {
        this.setBoolean(USE_TEST_MONGO, useTestMongo);
    }

    public String getTriplesCollectionName() {
        return this.get(MONGO_COLLECTION_PREFIX, "rya") + "_triples";
    }

    public String getCollectionName() {
        return this.get(MONGO_COLLECTION_PREFIX, "rya");
    }

    public void setCollectionName(String name) {
        this.set(MONGO_COLLECTION_PREFIX, name);
    }

    public String getMongoInstance() {
        return this.get(MONGO_INSTANCE, "localhost");
    }

    public void setMongoInstance(String name) {
        this.set(MONGO_INSTANCE, name);
    }

    public String getMongoPort() {
        return this.get(MONGO_INSTANCE_PORT, "27017");
    }

    public void setMongoPort(String name) {
        this.set(MONGO_INSTANCE_PORT, name);
    }

    public String getMongoDBName() {
        return this.get(MONGO_DB_NAME, "rya");
    }

    public void setMongoDBName(String name) {
        this.set(MONGO_DB_NAME, name);
    }

    public String getNameSpacesCollectionName() {
        return this.get(MONGO_COLLECTION_PREFIX, "rya") + "_ns";
    }
    
    public void setAdditionalIndexers(Class<? extends MongoSecondaryIndex>... indexers) {
        List<String> strs = Lists.newArrayList();
        for (Class<?> ai : indexers){
            strs.add(ai.getName());
        }
        
        setStrings(CONF_ADDITIONAL_INDEXERS, strs.toArray(new String[]{}));
    }

    public List<MongoSecondaryIndex> getAdditionalIndexers() {
        return getInstances(CONF_ADDITIONAL_INDEXERS, MongoSecondaryIndex.class);
    }    
    
    public void setMongoClient(MongoClient client){
    	this.mongoClient = client;
    }
    
    public MongoClient getMongoClient() {
    	return mongoClient;
    }

}
