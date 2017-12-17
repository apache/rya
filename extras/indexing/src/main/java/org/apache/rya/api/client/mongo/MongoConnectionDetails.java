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
package org.apache.rya.api.client.mongo;

import static java.util.Objects.requireNonNull;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;

import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * The information the shell used to connect to Mongo DB.
 */
@DefaultAnnotation(NonNull.class)
public class MongoConnectionDetails {

    private final String username;
    private final char[] userPass;
    private final String hostname;
    private final int port;

    /**
     * Constructs an instance of {@link MongoConnectionDetails}.
     *
     * @param username - The username that was used to establish the connection. (not null)
     * @param password - The password that was used to establish the connection. (not null)
     * @param hostname - The hostname of the Mongo DB that was connected to. (not null)
     * @param port - The port of the Mongo DB that was connected to.
     */
    public MongoConnectionDetails(
            final String username,
            final char[] userPass,
            final String hostname,
            final int port) {
        this.username = requireNonNull(username);
        this.userPass = requireNonNull(userPass);
        this.hostname = requireNonNull(hostname);
        this.port = port;
    }

    /**
     * @return The username that was used to establish the connection.
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * @return The password that was used to establish the connection.
     */
    public char[] getPassword() {
        return this.userPass;
    }

    /**
     * @return The hostname of the Mongo DB that was connected to.
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * @return The port of the Mongo DB that was connected to.
     */
    public int getPort() {
        return port;
    }

    /**
     * Create a {@link MongoDBRdfConfiguration} that is using this object's values.
     *
     * @param ryaInstanceName - The Rya instance to connect to.
     * @return Constructs a new {@link MongoDBRdfConfiguration} object with values from this object.
     */
    public MongoDBRdfConfiguration build(final String ryaInstanceName) {
        return build(ryaInstanceName, null);
    }

    public MongoDBRdfConfiguration build(final String ryaInstanceName, final MongoClient mongoClient) {
        // Note, we don't use the MongoDBRdfConfigurationBuilder here because it explicitly sets
        // authorizations and visibilities to an empty string if they are not set on the builder.
        // If they are null in the MongoRdfConfiguration object, it may do the right thing.
        final MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration();
        conf.setBoolean(ConfigUtils.USE_MONGO, true);
        conf.setMongoInstance(hostname);
        conf.setMongoPort("" + port);
        conf.setMongoUser(username);
        conf.setMongoPassword(new String(userPass));
        conf.setMongoDBName(ryaInstanceName);

        // Both of these are ways to configure the collection prefixes.
        conf.setCollectionName(ryaInstanceName);
        conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, ryaInstanceName);
        if (mongoClient != null) {
            conf.setMongoClient(mongoClient);
        }
        return conf;
    }
}