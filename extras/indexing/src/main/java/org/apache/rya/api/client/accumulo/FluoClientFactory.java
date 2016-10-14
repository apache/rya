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

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;

/**
 * Creates {@link FluoClient}s that are connected to a specific Fluo Application.
 */
@ParametersAreNonnullByDefault
public class FluoClientFactory {

    /**
     * Create a {@link FluoClient} that uses the provided connection details.
     *
     * @param username - The username the connection will use. (not null)
     * @param password - The password the connection will use. (not null)
     * @param instanceName - The name of the Accumulo instance. (not null)
     * @param zookeeperHostnames - A comma delimited list of the Zookeeper server hostnames. (not null)
     * @param fluoAppName - The Fluo Application the client will be connected to. (not null)
     * @return A {@link FluoClient} that may be used to access the Fluo Application.
     */
    public FluoClient connect(
            final String username,
            final String password,
            final String instanceName,
            final String zookeeperHostnames,
            final String fluoAppName) {
        requireNonNull(username);
        requireNonNull(password);
        requireNonNull(instanceName);
        requireNonNull(zookeeperHostnames);
        requireNonNull(fluoAppName);

        final FluoConfiguration fluoConfig = new FluoConfiguration();

        // Fluo configuration values.
        fluoConfig.setApplicationName( fluoAppName );
        fluoConfig.setInstanceZookeepers( zookeeperHostnames + "/fluo" );

        // Accumulo Connection Stuff.
        fluoConfig.setAccumuloZookeepers( zookeeperHostnames );
        fluoConfig.setAccumuloInstance( instanceName );
        fluoConfig.setAccumuloUser( username );
        fluoConfig.setAccumuloPassword( password );

        // Connect the client.
        return FluoFactory.newClient(fluoConfig);
    }
}
