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
package org.apache.rya.accumulo.utils;

import static java.util.Objects.requireNonNull;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A utility class that helps create instances of {@link Connector}.
 */
@DefaultAnnotation(NonNull.class)
public class ConnectorFactory {

    /**
     * Private constructor to prevent instantiation.
     */
    private ConnectorFactory() { }

    /**
     * Create a {@link Connector} that connects to an Accumulo instance. The {@link AccumuloRdfConfiguration#USE_MOCK_INSTANCE}
     * flag must be set if the configuration information needs to connect to a mock instance of Accumulo. If this is
     * the case, then the Zookeepers information should not be set.
     *
     * @param config - The configuration that will be used to initialize the connector. (not null)
     * @return The {@link Connector} that was created by {@code config}.
     * @throws AccumuloException The connector couldn't be created because of an Accumulo problem.
     * @throws AccumuloSecurityException The connector couldn't be created because of an Accumulo security violation.
     */
    public static Connector connect(AccumuloRdfConfiguration config) throws AccumuloException, AccumuloSecurityException {
        requireNonNull(config);

        // Wrap the configuration as the Accumulo configuration so that we may have access
        // to Accumulo specific configuration values.
        final AccumuloRdfConfiguration accConf = new AccumuloRdfConfiguration(config);

        // Create the Mock or Zookeeper backed Instance depending on the configuration.
        final Instance instance;
        if(accConf.useMockInstance()) {
            instance = new MockInstance(accConf.getInstanceName());
        } else {
            instance = new ZooKeeperInstance(accConf.getInstanceName(), accConf.getZookeepers());
        }

        // Return a connector using the configured username and password.
        return instance.getConnector(accConf.getUsername(), new PasswordToken(accConf.getPassword()));
    }
}