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

import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.junit.Test;

/**
 * Integration tests the methods of {@link AccumuloUninstall}.
 */
public class AccumuloUninstallIT extends AccumuloITBase {

    @Test(expected = InstanceDoesNotExistException.class)
    public void instanceDoesNotExist() throws Exception {
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        new AccumuloUninstall(connectionDetails, getConnector()).uninstall("instance_that_does_not_exist");
    }

    @Test
    public void uninstall() throws Exception {
        // Install an instance of Rya.
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(true)
                .setEnableEntityCentricIndex(true)
                .setEnableFreeTextIndex(true)
                .setEnableTemporalIndex(true)
                .setEnablePcjIndex(true)
                .setEnableGeoIndex(true)
                .build();

        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(connectionDetails, getConnector());
        ryaClient.getInstall().install(getRyaInstanceName(), installConfig);

        // Check that the instance exists.
        assertTrue( ryaClient.getInstanceExists().exists(getRyaInstanceName()) );

        // Uninstall the instance of Rya.
        ryaClient.getUninstall().uninstall(getRyaInstanceName());

        // Verify that it no longer exists.
        assertFalse( ryaClient.getInstanceExists().exists(getRyaInstanceName()) );
    }
}