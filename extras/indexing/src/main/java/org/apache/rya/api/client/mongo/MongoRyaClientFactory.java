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

import java.util.Optional;

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
     * @param connectionDetails - Details about the values that were used to connect to Mongo DB. (not null)
     * @param adminClient - The MongoDB client the administrative commands will use. (not null)
     * @return The initialized commands.
     */
    public static RyaClient build(
            final MongoConnectionDetails connectionDetails,
            final MongoClient adminClient) {
        requireNonNull(connectionDetails);
        requireNonNull(adminClient);

        // Build the RyaCommands option with the initialized commands.
        final MongoInstanceExists instanceExists = new MongoInstanceExists(adminClient);

        return new RyaClient(
                new MongoInstall(connectionDetails, adminClient, instanceExists),
                new MongoCreatePCJ(adminClient, instanceExists),
                new MongoDeletePCJ(adminClient, instanceExists),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                new MongoBatchUpdatePCJ(connectionDetails, adminClient, instanceExists),
                new MongoGetInstanceDetails(adminClient, instanceExists),
                instanceExists,
                new MongoListInstances(adminClient),
                Optional.empty(),
                Optional.empty(),
                new MongoUninstall(adminClient, instanceExists),
                new MongoLoadStatements(connectionDetails, instanceExists),
                new MongoLoadStatementsFile(connectionDetails, instanceExists),
                new MongoExecuteSparqlQuery(connectionDetails, instanceExists));
    }
}