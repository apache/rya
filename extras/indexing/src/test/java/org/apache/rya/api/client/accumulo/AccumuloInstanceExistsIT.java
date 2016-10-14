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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.junit.Test;

import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.accumulo.instance.AccumuloRyaInstanceDetailsRepository;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.client.RyaClientException;

/**
 * Integration tests the methods of {@link AccumuloInstanceExists}.
 */
public class AccumuloInstanceExistsIT extends AccumuloITBase {

    @Test
    public void exists_ryaDetailsTable() throws AccumuloException, AccumuloSecurityException, RyaClientException, TableExistsException {
        final Connector connector = getConnector();
        final TableOperations tableOps = connector.tableOperations();

        // Create the Rya instance's Rya details table.
        final String instanceName = "test_instance_";
        final String ryaDetailsTable = instanceName + AccumuloRyaInstanceDetailsRepository.INSTANCE_DETAILS_TABLE_NAME;
        tableOps.create(ryaDetailsTable);

        // Verify the command reports the instance exists.
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final AccumuloInstanceExists instanceExists = new AccumuloInstanceExists(connectionDetails, getConnector());
        assertTrue( instanceExists.exists(instanceName) );
    }

    @Test
    public void exists_dataTables() throws AccumuloException, AccumuloSecurityException, RyaClientException, TableExistsException {
        final Connector connector = getConnector();
        final TableOperations tableOps = connector.tableOperations();

        // Create the Rya instance's Rya details table.
        final String instanceName = "test_instance_";

        final String spoTableName = instanceName + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX;
        final String ospTableName = instanceName + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX;
        final String poTableName = instanceName + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX;
        tableOps.create(spoTableName);
        tableOps.create(ospTableName);
        tableOps.create(poTableName);

        // Verify the command reports the instance exists.
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final AccumuloInstanceExists instanceExists = new AccumuloInstanceExists(connectionDetails, getConnector());
        assertTrue( instanceExists.exists(instanceName) );
    }

    @Test
    public void doesNotExist() throws RyaClientException, AccumuloException, AccumuloSecurityException {
        // Verify the command reports the instance does not exists.
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final AccumuloInstanceExists instanceExists = new AccumuloInstanceExists(connectionDetails, getConnector());
        assertFalse( instanceExists.exists("some_instance") );
    }
}