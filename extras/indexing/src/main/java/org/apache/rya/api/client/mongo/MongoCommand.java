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

import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An abstract class that holds onto Mongo access information. Extend this
 * when implementing a command that interacts with Mongo.
 */
@DefaultAnnotation(NonNull.class)
public abstract class MongoCommand {

    private final MongoConnectionDetails connectionDetails;
    private final MongoClient client;

    /**
     * Constructs an instance of {@link MongoCommand}.
     *
     * Details about the values that were used to create the client. (not null)
     * 
     * @param client
     *            - Provides programatic access to the instance of Mongo
     *            that hosts Rya instance. (not null)
     */
    public MongoCommand(final MongoConnectionDetails connectionDetails, final MongoClient client) {
        this.connectionDetails = requireNonNull(connectionDetails);
        this.client = requireNonNull(client);
    }

    /**
     * @return Details about the values that were used to create the connector to mongo. (not null)
     */
    public MongoConnectionDetails getMongoConnectionDetails() {
        return connectionDetails;
    }

    /**
     * @return Provides programatic access to the instance of Mongo that hosts Rya instance.
     */
    public MongoClient getClient() {
        return client;
    }
}