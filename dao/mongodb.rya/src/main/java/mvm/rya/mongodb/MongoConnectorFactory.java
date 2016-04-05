package mvm.rya.mongodb;

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

import java.net.UnknownHostException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

/**
 * Mongo convention generally allows for a single instance of a {@link MongoClient}
 * throughout the life cycle of an application.  This MongoConnectorFactory lazy
 * loads a Mongo Client and uses the same one whenever {@link MongoConnectorFactory#getMongoClient(Configuration)}
 * is invoked.
 */
public class MongoConnectorFactory {
    private static MongoClient mongoClient;

    /**
     * @param conf The {@link Configuration} defining how to construct the MongoClient.
     * @return A {@link MongoClient}.  This client is lazy loaded and the same one
     * is used throughout the lifecycle of the application.
     * @throws NumberFormatException - Thrown if the configured port is not a valid number
     * @throws UnknownHostException - The configured host cannot be found.
     */
    public static synchronized MongoClient getMongoClient(final Configuration conf) throws NumberFormatException, UnknownHostException {
        if(mongoClient == null) {
            final String host = conf.get(MongoDBRdfConfiguration.MONGO_INSTANCE);
            final int port = Integer.parseInt(conf.get(MongoDBRdfConfiguration.MONGO_INSTANCE_PORT));
            final ServerAddress server = new ServerAddress(host, port);

            //check for authentication credentials
            if (conf.get(MongoDBRdfConfiguration.MONGO_USER) != null) {
                final String username = conf.get(MongoDBRdfConfiguration.MONGO_USER);
                final String dbName = conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME);
                final char[] pswd = conf.get(MongoDBRdfConfiguration.MONGO_USER_PASSWORD).toCharArray();
                final MongoCredential cred = MongoCredential.createCredential(username, dbName, pswd);
                mongoClient = new MongoClient(server, Arrays.asList(cred));
            } else {
                mongoClient = new MongoClient(server);
            }
        }
        return mongoClient;
    }
}
