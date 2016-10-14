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
import javax.annotation.concurrent.Immutable;

/**
 * The information that the shell used to connect to Accumulo.
 */
@Immutable
@ParametersAreNonnullByDefault
public class AccumuloConnectionDetails {
    private final String username;
    private final char[] password;
    private final String instanceName;
    private final String zookeepers;

    /**
     * Constructs an instance of {@link AccumuloConnectionDetails}.
     *
     * @param username - The username that was used to establish the connection. (not null)
     * @param password - The password that was used to establish the connection. (not null)
     * @param instanceName - The Accumulo instance name that was used to establish the connection. (not null)
     * @param zookeepers - The list of zookeeper hostname that were used to establish the connection. (not null)
     */
    public AccumuloConnectionDetails(
            final String username,
            final char[] password,
            final String instanceName,
            final String zookeepers) {
        this.username = requireNonNull(username);
        this.password = requireNonNull(password);
        this.instanceName = requireNonNull(instanceName);
        this.zookeepers = requireNonNull(zookeepers);
    }

    /**
     * @return The username that was used to establish the connection.
     */
    public String getUsername() {
        return username;
    }

    /**
     * @return The password that was used to establish the connection.
     */
    public char[] getPassword() {
        return password;
    }

    /**
     * @return The Accumulo instance name that was used to establish the connection.
     */
    public String getInstanceName() {
        return instanceName;
    }

    /**
     * @return The list of zookeeper hostname that were used to establish the connection.
     */
    public String getZookeepers() {
        return zookeepers;
    }
}