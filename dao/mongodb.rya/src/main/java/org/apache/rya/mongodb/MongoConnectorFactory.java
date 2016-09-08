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
import java.util.Arrays;

import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;

/**
 * Mongo convention generally allows for a single instance of a {@link MongoClient}
 * throughout the life cycle of an application.  This MongoConnectorFactory lazy
 * loads a Mongo Client and uses the same one whenever {@link MongoConnectorFactory#getMongoClient(Configuration)}
 * is invoked.
 */
public class MongoConnectorFactory {
    private static MongoClient mongoClient;

    private final static String MSG_INTRO = "Failed to connect to MongoDB: ";

    /**
     * @param conf The {@link Configuration} defining how to construct the MongoClient.
     * @return A {@link MongoClient}.  This client is lazy loaded and the same one
     * is used throughout the lifecycle of the application.
     * @throws IOException - if MongodForTestsFactory constructor has an io exception.
     * @throws ConfigurationRuntimeException - Thrown if the configured server, port, user, or others are missing.
     * @throws MongoException  if can't connect despite conf parameters are given
     */
    public static synchronized MongoClient getMongoClient(final Configuration conf)
            throws ConfigurationRuntimeException, MongoException {
        if (mongoClient == null) {
            createMongoClientForServer(conf);
        }
        return mongoClient;
    }

    /**
     * Silently closes the underlying Mongo client.
     */
    public static synchronized void closeMongoClient() {
        IOUtils.closeQuietly(mongoClient);
        mongoClient = null;
    }

    /**
     * Create a MongoDB client object and assign it to this class's static mongoClient
     * @param conf configuration containing connection parameters
     * @throws ConfigurationRuntimeException - Thrown if the configured server, port, user, or others are missing.
     * @throws MongoException  if can't connect despite conf parameters are given
     */
    private static void createMongoClientForServer(final Configuration conf)
            throws ConfigurationRuntimeException, MongoException {
        // Connect to a running Mongo server
        final String host = requireNonNull(conf.get(MongoDBRdfConfiguration.MONGO_INSTANCE), MSG_INTRO+"host name is required");
        final int port = requireNonNullInt(conf.get(MongoDBRdfConfiguration.MONGO_INSTANCE_PORT), MSG_INTRO+"Port number is required.");
        final ServerAddress server = new ServerAddress(host, port);
        // check for authentication credentials
        if (conf.get(MongoDBRdfConfiguration.MONGO_USER) != null) {
            final String username = conf.get(MongoDBRdfConfiguration.MONGO_USER);
            final String dbName = requireNonNull(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME),
                    MSG_INTRO + MongoDBRdfConfiguration.MONGO_DB_NAME + " is null but required configuration if "
                            + MongoDBRdfConfiguration.MONGO_USER + " is configured.");
            final char[] pswd = requireNonNull(conf.get(MongoDBRdfConfiguration.MONGO_USER_PASSWORD),
                    MSG_INTRO + MongoDBRdfConfiguration.MONGO_USER_PASSWORD + " is null but required configuration if "
                            + MongoDBRdfConfiguration.MONGO_USER + " is configured.").toCharArray();
            final MongoCredential cred = MongoCredential.createCredential(username, dbName, pswd);
            mongoClient = new MongoClient(server, Arrays.asList(cred));
        } else {
            // No user was configured:
            mongoClient = new MongoClient(server);
        }
    }

    /**
     * Throw exception for un-configured required values.
     *
     * @param required  String to check
     * @param message  throw configuration exception with this description
     * @return unaltered required string
     * @throws ConfigurationRuntimeException  if required is null
     */
    private static String requireNonNull(final String required, final String message) throws ConfigurationRuntimeException {
        if (required == null) {
            throw new ConfigurationRuntimeException(message);
        }
        return required;
    }

    /*
     * Same as above, check that it is a integer and return the parsed integer.
     */
    private static int requireNonNullInt(final String required, final String message) throws ConfigurationRuntimeException {
        if (required == null) {
            throw new ConfigurationRuntimeException(message);
        }
        try {
            return Integer.parseInt(required);
        } catch (final NumberFormatException e) {
            throw new ConfigurationRuntimeException(message);
        }
    }
}
