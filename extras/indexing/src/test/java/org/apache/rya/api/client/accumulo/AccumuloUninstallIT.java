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
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.Uninstall;
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
                .setFluoPcjAppName("fluo_app_name")
                .build();

        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final Install install = new AccumuloInstall(connectionDetails, getConnector());
        final String ryaInstanceName = "testInstance_";
        install.install(ryaInstanceName, installConfig);

        // Check that the instance exists.
        final InstanceExists instanceExists = new AccumuloInstanceExists(connectionDetails, getConnector());
        assertTrue( instanceExists.exists(ryaInstanceName) );

        // Uninstall the instance of Rya.
        final Uninstall uninstall = new AccumuloUninstall(connectionDetails, getConnector());
        uninstall.uninstall(ryaInstanceName);

        // Verify that it no longer exists.
        assertFalse( instanceExists.exists(ryaInstanceName) );
    }
}