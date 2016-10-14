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
package org.apache.rya.api.client.accumulo;

import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.Connector;

import org.apache.rya.api.client.RyaClient;

/**
 * Constructs instance of {@link RyaClient} that are connected to instance of
 * Rya hosted by Accumulo clusters.
 */
@ParametersAreNonnullByDefault
public class AccumuloRyaClientFactory {

    /**
     * Initialize a set of {@link RyaClient} that will interact with an instance of
     * Rya that is hosted by an Accumulo cluster.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - The Accumulo connector the commands will use. (not null)
     * @return The initialized commands.
     */
    public static RyaClient build(
            final AccumuloConnectionDetails connectionDetails,
            final Connector connector) {
        requireNonNull(connectionDetails);
        requireNonNull(connector);

        // Build the RyaCommands option with the initialized commands.
        return new RyaClient(
                new AccumuloInstall(connectionDetails, connector),
                new AccumuloCreatePCJ(connectionDetails, connector),
                new AccumuloDeletePCJ(connectionDetails, connector),
                new AccumuloBatchUpdatePCJ(connectionDetails, connector),
                new AccumuloGetInstanceDetails(connectionDetails, connector),
                new AccumuloInstanceExists(connectionDetails, connector),
                new AccumuloListInstances(connectionDetails, connector));
    }
}