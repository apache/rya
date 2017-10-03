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

import org.apache.rya.accumulo.AccumuloRdfConfiguration;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

/**
 * The information that the shell used to connect to Accumulo.
 */
@Immutable
@DefaultAnnotation(NonNull.class)
public class AccumuloConnectionDetails {
    private final String username;
    private final char[] userpass;
    private final String instanceName;
    private final String zookeepers;

    /**
     * Constructs an instance of {@link AccumuloConnectionDetails}.
     *
     * @param username - The username that was used to establish the connection. (not null)
     * @param userpass - The userpass that was used to establish the connection. (not null)
     * @param instanceName - The Accumulo instance name that was used to establish the connection. (not null)
     * @param zookeepers - The list of zookeeper hostname that were used to establish the connection. (not null)
     */
    public AccumuloConnectionDetails(
            final String username,
            final char[] password,
            final String instanceName,
            final String zookeepers) {
        this.username = requireNonNull(username);
        this.userpass = requireNonNull(password);
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
    public char[] getUserPass() {
        return userpass;
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

    /**
     *
     * @param ryaInstanceName - The Rya instance to connect to.
     * @return Constructs a new {@link AccumuloRdfConfiguration} object with values from this object.
     */
    public AccumuloRdfConfiguration buildAccumuloRdfConfiguration(final String ryaInstanceName) {
        
        // Note, we don't use the AccumuloRdfConfigurationBuilder here because it explicitly sets 
        // authorizations and visibilities to an empty string if they are not set on the builder.
        // If they are null in the AccumuloRdfConfiguration object, then Accumulo uses the values stored in accumulo for the user.
        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
                conf.setTablePrefix(ryaInstanceName);
                conf.setAccumuloZookeepers(zookeepers);
                conf.setAccumuloInstance(instanceName);
                conf.setAccumuloUser(username);
                conf.setAccumuloPassword(new String(userpass));
        return conf;
    }
}