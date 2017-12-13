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

import org.apache.rya.api.client.RyaClient;

import com.mongodb.MongoClient;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Constructs instance of {@link RyaClient} that are connected to instance of
 * Rya hosted by Mongo clusters.
 */
@DefaultAnnotation(NonNull.class)
public class MongoRyaClientFactory {

    /**
     * Initialize a set of {@link RyaClient} that will interact with an instance of
     * Rya that is hosted by a MongoDB server.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - The MongoDB connector the commands will use. (not null)
     * @return The initialized commands.
     */
    public static RyaClient build(
            final MongoConnectionDetails connectionDetails,
            final MongoClient connector) {
        requireNonNull(connectionDetails);
        requireNonNull(connector);

        // Build the RyaCommands option with the initialized commands.
        return new RyaClient(//
                        new MongoInstall(connectionDetails, connector), //
                        new MongoCreatePCJ(connectionDetails, connector), //
                        new MongoDeletePCJ(connectionDetails, connector), //
                        null, null, null, null,
                        new MongoGetInstanceDetails(connectionDetails, connector), //
                        new MongoInstanceExists(connectionDetails, connector), //
                        new MongoListInstances(connectionDetails, connector), //
                        null, null,
                        new MongoUninstall(connectionDetails, connector), //
                        new MongoLoadStatements(connectionDetails, connector), //
                        new MongoLoadStatementsFile(connectionDetails, connector), //
                        null);// FIXME new MongoExecuteSparqlQuery(connectionDetails, connector));
    }
}