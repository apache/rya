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
package org.apache.rya.shell.util;

import static java.util.Objects.requireNonNull;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

/**
 * Creates {@link Connector}s that are linked to an instance of Accumulo.
 */
@ParametersAreNonnullByDefault
public class ConnectorFactory {

    /**
     * Create a {@link Connector} that uses the provided connection details.
     *
     * @param username - The username the connection will use. (not null)
     * @param password - The password the connection will use. (not null)
     * @param instanceName - The name of the Accumulo instance. (not null)
     * @param zookeeperHostnames - A comma delimited list of the Zookeeper server hostnames. (not null)
     * @return A {@link Connector} that may be used to access the instance of Accumulo.
     * @throws AccumuloSecurityException Could not connect for security reasons.
     * @throws AccumuloException Could not connect for other reasons.
     */
    public Connector connect(
            final String username,
            final CharSequence password,
            final String instanceName,
            final String zookeeperHostnames) throws AccumuloException, AccumuloSecurityException {
        requireNonNull(username);
        requireNonNull(password);
        requireNonNull(instanceName);
        requireNonNull(zookeeperHostnames);

        // Setup the password token that will be used.
        final PasswordToken token = new PasswordToken( password );

        // Connect to the instance of Accumulo.
        final Instance instance = new ZooKeeperInstance(instanceName, zookeeperHostnames);
        return instance.getConnector(username, token);
    }
}