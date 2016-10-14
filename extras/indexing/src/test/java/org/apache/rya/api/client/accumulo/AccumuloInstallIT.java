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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.junit.Test;

import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetailsRepository.NotInitializedException;
import org.apache.rya.api.instance.RyaDetailsRepository.RyaDetailsRepositoryException;

/**
 * Integration tests the methods of {@link AccumuloInstall}.
 */
public class AccumuloInstallIT extends AccumuloITBase {

    @Test
    public void install() throws AccumuloException, AccumuloSecurityException, DuplicateInstanceNameException, RyaClientException, NotInitializedException, RyaDetailsRepositoryException {
        // Install an instance of Rya.
        final String instanceName = "testInstance_";
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setEnableEntityCentricIndex(false)
                .setEnableFreeTextIndex(false)
                .setEnableTemporalIndex(false)
                .setEnablePcjIndex(false)
                .setEnableGeoIndex(false)
                .setFluoPcjAppName("fluo_app_name")
                .build();

        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final Install install = new AccumuloInstall(connectionDetails, getConnector());
        install.install(instanceName, installConfig);

        // Check that the instance exists.
        final InstanceExists instanceExists = new AccumuloInstanceExists(connectionDetails, getConnector());
        instanceExists.exists(instanceName);
    }

    @Test(expected = DuplicateInstanceNameException.class)
    public void install_alreadyExists() throws DuplicateInstanceNameException, RyaClientException, AccumuloException, AccumuloSecurityException {
        // Install an instance of Rya.
        final String instanceName = "testInstance_";
        final InstallConfiguration installConfig = InstallConfiguration.builder().build();

        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final Install install = new AccumuloInstall(connectionDetails, getConnector());
        install.install(instanceName, installConfig);

        // Install it again.
        install.install(instanceName, installConfig);
    }
}