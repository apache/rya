/**
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

import static java.util.Objects.requireNonNull;

import java.util.Properties;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link RdfCloudTripleStoreConfiguration} that configures how Rya connects to a MongoDB Rya triple store.
 */
public class MongoDBRdfConfiguration extends RdfCloudTripleStoreConfiguration {

    // MongoDB Server connection values.
    public static final String MONGO_HOSTNAME = "mongo.db.instance";
    public static final String MONGO_PORT = "mongo.db.port";

    // MongoDB Database values.
    public static final String MONGO_DB_NAME = "mongo.db.name";
    public static final String MONGO_USER = "mongo.db.user";
    public static final String MONGO_USER_PASSWORD = "mongo.db.userpassword";

    // Rya Instance values.
    public static final String MONGO_COLLECTION_PREFIX = "mongo.db.collectionprefix";

    // Rya Sail configuration values.
    public static final String USE_MOCK_MONGO = ".useMockInstance";
    public static final String CONF_FLUSH_EACH_UPDATE = "rya.mongodb.dao.flusheachupdate";
    public static final String CONF_ADDITIONAL_INDEXERS = "ac.additional.indexers";
    public static final String MONGO_GEO_MAXDISTANCE = "mongo.geo.maxdist";

    /**
     * Constructs an empty instance of {@link MongoDBRdfConfiguration}.
     */
    public MongoDBRdfConfiguration() {
        super();
    }

    /**
     * Constructs an instance of {@link MongoDBRdfConfiguration} pre-loaded with values.
     *
     * @param other - The values that will be cloned into the constructed object. (not null)
     */
    public MongoDBRdfConfiguration(final Configuration other) {
        super( requireNonNull(other) );
    }

    /**
     * Reads a {@link Properties} object into a {@link MongoDBRdfConfiguration}.
     * See {@link MongoDBRdfConfigurationBuilder#fromProperties(Properties)} for which keys
     * are to be used within the properties object. This method will replace that object's keys
     * with the configuration object's keys since they are not the same.
     *
     * @param props - The properties containing Mongo specific configuration parameters. (not null)
     * @return A {@link } loaded with the values that were in {@code props}.
     */
    public static MongoDBRdfConfiguration fromProperties(final Properties props) {
        requireNonNull(props);
        return MongoDBRdfConfigurationBuilder.fromProperties(props);
    }

    /**
     * @return A new instance of {@link MongoDBRdfConfigurationBuilder}.
     */
    public static MongoDBRdfConfigurationBuilder getBuilder() {
        return new MongoDBRdfConfigurationBuilder();
    }

    @Override
    public MongoDBRdfConfiguration clone() {
        return new MongoDBRdfConfiguration(this);
    }

    /**
     * Set whether the Rya client should spin up an embedded MongoDB instance and connect to that
     * or if it should connect to a MongoDB Server that is running somewhere.
     *
     * @param useMock - {@true} to use an embedded Mongo DB instance; {@code false} to connect to a real server.
     */
    public void setUseMock(final boolean useMock) {
        this.setBoolean(USE_MOCK_MONGO, useMock);
    }

    /**
     * Indicates whether the Rya client should spin up an embedded MongoDB instance and connect to that
     * or if it should connect to a MongoDB Server that is running somewhere.
     *
     * @return {@true} to use an embedded Mongo DB instance; {@code false} to connect to a real server.
     */
    public boolean getUseMock() {
        return getBoolean(USE_MOCK_MONGO, false);
    }

    /**
     * @return The hostname of the MongoDB Server to connect to. (default: localhost)
     */
    public String getMongoHostname() {
        return get(MONGO_HOSTNAME, "localhost");
    }

    /**
     * @param hostname - The hostname of the MongoDB Server to connect to.
     */
    public void setMongoHostname(final String hostname) {
        requireNonNull(hostname);
        set(MONGO_HOSTNAME, hostname);
    }

    /**
     * @return The port of the MongoDB Server to connect to. (default: 27017)
     */
    public String getMongoPort() {
        return get(MONGO_PORT, AbstractMongoDBRdfConfigurationBuilder.DEFAULT_MONGO_PORT);
    }

    /**
     * @param port - The port of the MongoDB Server to connect to.
     */
    public void setMongoPort(final String port) {
        requireNonNull(port);
        set(MONGO_PORT, port);
    }

    /**
     * @return The name of the MongoDB Database to connect to. (default: rya)
     */
    public String getMongoDBName() {
        return get(MONGO_DB_NAME, "rya");
    }

    /**
     * @param database - The name of the MongoDb Database to connect to.
     */
    public void setMongoDBName(final String database) {
        requireNonNull(database);
        set(MONGO_DB_NAME, database);
    }

    /**
     * @param user - The user used to connect to the MongoDB Database that hosts the Rya Instance. (not null)
     */
    public void setMongoUser(final String user) {
        requireNonNull(user);
        set(MONGO_USER, user);
    }

    /**
     * @return The user used to connect to the MongoDB Database that hosts the Rya Instance.
     */
    public @Nullable String getMongoUser() {
        return get(MONGO_USER);
    }

    /**
     * @param password - The password used to connect to the MongoDB Database that hosts the Rya Instance.
     */
    public void setMongoPassword(final String password) {
        requireNonNull(password);
        set(MONGO_USER_PASSWORD, password);
    }

    /**
     * @return The password used to connect to the MongoDB Database that hosts the Rya Instance.
     */
    public @Nullable String getMongoPassword() {
        return get(MONGO_USER_PASSWORD);
    }

    /**
     * @return The name of the Rya instance to connect to. (default: rya)
     */
    public String getRyaInstanceName() {
        return get(MONGO_COLLECTION_PREFIX, "rya");
    }

    /**
     * @param name - The name of the Rya instance to connect to.
     */
    public void setRyaInstanceName(final String name) {
        requireNonNull(name);
        set(MONGO_COLLECTION_PREFIX, name);
    }

    /**
     * @return The name of the MongoDB Collection that contains Rya statements. (default: rya_triples)
     */
    public String getTriplesCollectionName() {
        return getRyaInstanceName() + "_triples";
    }

    /**
     * @return The name of the MongoDB Collection that contains the Rya namespace. (default: rya_ns)
     */
    public String getNameSpacesCollectionName() {
        return getRyaInstanceName() + "_ns";
    }

    /**
     * @return The authorizations that will be used when accessing data. (default: empty)
     */
    public Authorizations getAuthorizations() {
        final String[] auths = getAuths();
        if (auths == null || auths.length == 0) {
            return MongoDbRdfConstants.ALL_AUTHORIZATIONS;
        }
        return new Authorizations(auths);
    }

    /**
     * Indicates whether each statement added to the batch writer should be flushed and written
     * right away to the datastore or not. If this is turned off, then the statements will be
     * queued and written to the datastore when the queue is full or after enough time has
     * passed without a write.
     *
     * @return {@code true} if flushing after each updated is enabled; otherwise {@code false}. (default: true)
     */
    public boolean flushEachUpdate(){
        return getBoolean(CONF_FLUSH_EACH_UPDATE, true);
    }

    /**
     * Set whether each statement added to the batch writer should be flushed and written
     * right away to the datastore or not. If this is turned off, then the statements will be
     * queued and written to the datastore when the queue is full or after enough time has
     * passed without a write.
     *
     * @param flush - {@code true} if flushing after each updated is enabled; otherwise {@code false}.
     */
    public void setFlush(final boolean flush){
        setBoolean(CONF_FLUSH_EACH_UPDATE, flush);
    }
}