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

/**
 * An abstract class that holds onto Accumulo access information. Extend this
 * when implementing a command that interacts with Accumulo.
 */
@ParametersAreNonnullByDefault
public abstract class AccumuloCommand {

    private final AccumuloConnectionDetails connectionDetails;
    private final Connector connector;

    /**
     * Constructs an instance of {@link AccumuloCommand}.
     *
     * Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo
     *   that hosts Rya instance. (not null)
     */
    public AccumuloCommand(
            final AccumuloConnectionDetails connectionDetails,
            final Connector connector) {
        this.connectionDetails = requireNonNull( connectionDetails );
        this.connector = requireNonNull(connector);
    }

    /**
     * @return Details about the values that were used to create the connector to the cluster. (not null)
     */
    public AccumuloConnectionDetails getAccumuloConnectionDetails() {
        return connectionDetails;
    }

    /**
     * @return Provides programatic access to the instance of Accumulo that hosts Rya instance.
     */
    public Connector getConnector() {
        return connector;
    }
}