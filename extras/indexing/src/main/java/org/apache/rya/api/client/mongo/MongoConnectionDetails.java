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

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.mongodb.MongoDBRdfConfiguration;
public class MongoConnectionDetails {
    private String username;
    private char[] userPass;
    private String instance;
    private String host;
    private String collectionName;

    public MongoConnectionDetails(String username, char[] userPass, String instance, String host, String ryaInstance) {
        this.username = username;
        this.userPass = userPass.clone();
        this.instance = instance;
        this.host = host;
        this.collectionName = requireNonNull(ryaInstance);
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return this.username;
    }

    /**
     * @return the password
     */
    public char[] getPassword() {
        return this.userPass;
    }

    /**
     * @return the instance
     */
    public String getInstance() {
        return instance;
    }

    /**
     * @return the host AKA MongoInstance
     */
    public String getHost() {
        return host;
    }
    /**
     * @return The Collection/Rya Prefix/Rya instance that was used to establish the connection.
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     *
     * @param ryaInstanceName
     *            - The Rya instance to connect to.
     * @return Constructs a new {@link AccumuloRdfConfiguration} object with values from this object.
     */
    public MongoDBRdfConfiguration build(final String ryaInstanceName) {

        // Note, we don't use the MongoDBRdfConfigurationBuilder here because it explicitly sets
        // authorizations and visibilities to an empty string if they are not set on the builder.
        // If they are null in the MongoRdfConfiguration object, it may do the right thing.
        final MongoDBRdfConfiguration conf = new MongoDBRdfConfiguration();
        conf.setMongoInstance(host);
        conf.setCollectionName(ryaInstanceName);
        conf.setMongoUser(username);
        conf.setMongoPassword(new String(userPass));
        return conf;
    }

}
