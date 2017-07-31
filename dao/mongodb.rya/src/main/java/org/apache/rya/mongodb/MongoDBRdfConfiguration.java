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
import java.util.Properties;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;

public class MongoDBRdfConfiguration extends RdfCloudTripleStoreConfiguration {
    public static final String MONGO_INSTANCE = "mongo.db.instance";
    public static final String MONGO_INSTANCE_PORT = "mongo.db.port";
    public static final String MONGO_GEO_MAXDISTANCE = "mongo.geo.maxdist";
    public static final String MONGO_DB_NAME = "mongo.db.name";
    public static final String MONGO_COLLECTION_PREFIX = "mongo.db.collectionprefix";
    public static final String MONGO_USER = "mongo.db.user";
    public static final String MONGO_USER_PASSWORD = "mongo.db.userpassword";
    public static final String CONF_ADDITIONAL_INDEXERS = "ac.additional.indexers";
    public static final String USE_MOCK_MONGO = ".useMockInstance";
    public static final String CONF_FLUSH_EACH_UPDATE = "rya.mongodb.dao.flusheachupdate";

    private MongoClient mongoClient;

    public MongoDBRdfConfiguration() {
        super();
    }

    public MongoDBRdfConfiguration(final Configuration other) {
        super(other);
    }

    /**
     * Creates a MongoRdfConfiguration object from a Properties file. This
     * method assumes that all values in the Properties file are Strings and
     * that the Properties file uses the keys below.
     *
     * <br>
     * <ul>
     * <li>"mongo.auths" - String of Mongo authorizations.  Empty auths used by default.
     * <li>"mongo.visibilities" - String of Mongo visibilities assigned to ingested triples.
     * <li>"mongo.user" - Mongo user.  Empty by default.
     * <li>"mongo.password" - Mongo password.  Empty by default.
     * <li>"mongo.host" - Mongo host.  Default host is "localhost"
     * <li>"mongo.port" - Mongo port.  Default port is "27017".
     * <li>"mongo.db.name" - Name of MongoDB.  Default name is "rya_triples".
     * <li>"mongo.collection.prefix" - Mongo collection prefix. Default is "rya_".
     * <li>"mongo.rya.prefix" - Prefix for Mongo Rya instance.  Same as value of "mongo.collection.prefix".
     * <li>"use.mock" - Use a Embedded Mongo instance as back-end for Rya instance. False by default.
     * <li>"use.display.plan" - Display query plan during evaluation. Useful for debugging.  True by default.
     * <li>"use.inference" - Use backward chaining inference during query.  False by default.
     * </ul>
     * <br>
     *
     * @param props
     *            - Properties file containing Mongo specific configuration
     *            parameters
     * @return MongoRdfConfiguration with properties set
     */
    public static MongoDBRdfConfiguration fromProperties(final Properties props) {
        return MongoDBRdfConfigurationBuilder.fromProperties(props);
    }

    public MongoDBRdfConfigurationBuilder getBuilder() {
        return new MongoDBRdfConfigurationBuilder();
    }

    @Override
    public MongoDBRdfConfiguration clone() {
        return new MongoDBRdfConfiguration(this);
    }

    public Authorizations getAuthorizations() {
        final String[] auths = getAuths();
        if (auths == null || auths.length == 0) {
            return MongoDbRdfConstants.ALL_AUTHORIZATIONS;
        }
        return new Authorizations(auths);
    }

    /**
     * @return {@code true} if each statement added to the batch writer should
     * be flushed and written right away to the datastore. {@code false} if the
     * statements should be queued and written to the datastore when the queue
     * is full or after enough time has passed without a write.<p>
     * Defaults to {@code true} if nothing is specified.
     */
    public boolean flushEachUpdate(){
        return getBoolean(CONF_FLUSH_EACH_UPDATE, true);
    }

    /**
     * Sets the {@link #CONF_FLUSH_EACH_UPDATE} property of the configuration.
     * @param flush {@code true} if each statement added to the batch writer
     * should be flushed and written right away to the datastore. {@code false}
     * if the statements should be queued and written to the datastore when the
     * queue is full or after enough time has passed without a write.
     */
    public void setFlush(final boolean flush){
        setBoolean(CONF_FLUSH_EACH_UPDATE, flush);
    }

    /**
     * @return name of Mongo Collection containing Rya triples
     */
    public String getTriplesCollectionName() {
        return this.get(MONGO_COLLECTION_PREFIX, "rya") + "_triples";
    }

    /**
     * @return name of Mongo Collection
     */
    public String getCollectionName() {
        return this.get(MONGO_COLLECTION_PREFIX, "rya");
    }

    /**
     * Sets Mongo Collection name
     * @param name - name of Mongo Collection to connect to
     */
    public void setCollectionName(final String name) {
        Preconditions.checkNotNull(name);
        this.set(MONGO_COLLECTION_PREFIX, name);
    }

    /**
     * @return name of Mongo Host
     */
    public String getMongoInstance() {
        return this.get(MONGO_INSTANCE, "localhost");
    }

    /**
     * Sets name of Mongo Host
     * @param name - name of Mongo Host to connect to
     */
    public void setMongoInstance(final String name) {
        Preconditions.checkNotNull(name);
        this.set(MONGO_INSTANCE, name);
    }

    /**
     * @return port that Mongo is running on
     */
    public String getMongoPort() {
        return this.get(MONGO_INSTANCE_PORT, AbstractMongoDBRdfConfigurationBuilder.DEFAULT_MONGO_PORT);
    }

    /**
     * Sets port that Mongo will run on
     * @param name - Mongo port to connect to
     */
    public void setMongoPort(final String name) {
        Preconditions.checkNotNull(name);
        this.set(MONGO_INSTANCE_PORT, name);
    }

    /**
     * @return name of MongoDB
     */
    public String getMongoDBName() {
        return this.get(MONGO_DB_NAME, "rya");
    }

    /**
     * Sets name of MongoDB
     * @param name - name of MongoDB to connect to
     */
    public void setMongoDBName(final String name) {
        Preconditions.checkNotNull(name);
        this.set(MONGO_DB_NAME, name);
    }

    /**
     * Tells Rya to use an embedded Mongo instance as its backing
     * if set to true.  By default this is set to false.
     * @param useMock
     */
    public void setUseMock(final boolean useMock) {
        this.setBoolean(USE_MOCK_MONGO, useMock);
    }

    /**
     * Get whether an embedded Mongo is being used as the backing
     * for Rya.
     * @return true if embedded Mongo is being used, and false otherwise
     */
    public boolean getUseMock() {
        return getBoolean(USE_MOCK_MONGO, false);
    }

    /**
     * @return name of NameSpace Mongo Collection
     */
    public String getNameSpacesCollectionName() {
        return this.get(MONGO_COLLECTION_PREFIX, "rya") + "_ns";
    }

    /**
     * Sets name of Mongo User
     * @param user - name of Mongo user to connect to
     */
    public void setMongoUser(final String user) {
        Preconditions.checkNotNull(user);
        set(MONGO_USER, user);
    }

    /**
     * @return name of Mongo user
     */
    public String getMongoUser() {
        return get(MONGO_USER);
    }

    /**
     * Sets Mongo password
     * @param password - password to connect to Mongo
     */
    public void setMongoPassword(final String password) {
        Preconditions.checkNotNull(password);
        set(MONGO_USER_PASSWORD, password);
    }

    /**
     * @return Mongo password
     */
    public String getMongoPassword() {
        return get(MONGO_USER_PASSWORD);
    }

    public void setAdditionalIndexers(final Class<? extends MongoSecondaryIndex>... indexers) {
        final List<String> strs = Lists.newArrayList();
        for (final Class<?> ai : indexers){
            strs.add(ai.getName());
        }

        setStrings(CONF_ADDITIONAL_INDEXERS, strs.toArray(new String[]{}));
    }

    public List<MongoSecondaryIndex> getAdditionalIndexers() {
        return getInstances(CONF_ADDITIONAL_INDEXERS, MongoSecondaryIndex.class);
    }

    public void setMongoClient(final MongoClient client) {
        Preconditions.checkNotNull(client);
        this.mongoClient = client;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

}
