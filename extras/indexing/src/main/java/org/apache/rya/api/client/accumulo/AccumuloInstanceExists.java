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
import org.apache.accumulo.core.client.admin.TableOperations;

import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;

/**
 * An Accumulo implementation of the {@link InstanceExists} command.
 */
@ParametersAreNonnullByDefault
public class AccumuloInstanceExists extends AccumuloCommand implements InstanceExists {

    /**
     * Constructs an insatnce of {@link AccumuloInstanceExists}.
     *
     * @param connectionDetails - Details about the values that were used to create the connector to the cluster. (not null)
     * @param connector - Provides programatic access to the instance of Accumulo that hosts Rya instance. (not null)
     */
    public AccumuloInstanceExists(final AccumuloConnectionDetails connectionDetails, final Connector connector) {
        super(connectionDetails, connector);
    }

    @Override
    public boolean exists(final String instanceName) throws RyaClientException {
        requireNonNull( instanceName );

        final TableOperations tableOps = getConnector().tableOperations();

        // Newer versions of Rya will have a Rya Details table.
        final String ryaDetailsTableName = instanceName + AccumuloRyaInstanceDetailsRepository.INSTANCE_DETAILS_TABLE_NAME;
        if(tableOps.exists(ryaDetailsTableName)) {
            return true;
        }

        // However, older versions only have the data tables.
        final String spoTableName = instanceName + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX;
        final String posTableName = instanceName + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX;
        final String ospTableName = instanceName + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX;
        if(tableOps.exists(spoTableName) && tableOps.exists(posTableName) && tableOps.exists(ospTableName)) {
            return true;
        }

        return false;
    }
}