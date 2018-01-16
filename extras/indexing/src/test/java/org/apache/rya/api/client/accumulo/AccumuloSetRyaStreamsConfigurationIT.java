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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.instance.RyaDetails.RyaStreamsDetails;
import org.junit.Test;

/**
 * Integration tests the methods of {@link AccumuloSetRyaStreamsConfiguration}.
 */
public class AccumuloSetRyaStreamsConfigurationIT extends AccumuloITBase {

    @Test(expected = InstanceDoesNotExistException.class)
    public void instanceDoesNotExist() throws Exception {
        final String ryaInstance = getRyaInstanceName();
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(connectionDetails, getConnector());

        // Skip the install step to create error causing situation.
        final RyaStreamsDetails details = new RyaStreamsDetails("localhost", 6);
        ryaClient.getSetRyaStreamsConfiguration().setRyaStreamsConfiguration(ryaInstance, details);
    }

    @Test
    public void updatesRyaDetails() throws Exception {
        final String ryaInstance = getRyaInstanceName();
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());
        final RyaClient ryaClient = AccumuloRyaClientFactory.build(connectionDetails, getConnector());

        // Install an instance of Rya.
        final Install installRya = ryaClient.getInstall();
        final InstallConfiguration installConf = InstallConfiguration.builder()
                .build();
        installRya.install(ryaInstance, installConf);

        // Fetch its details and show they do not have any RyaStreamsDetails.
        com.google.common.base.Optional<RyaStreamsDetails> streamsDetails =
                ryaClient.getGetInstanceDetails().getDetails(ryaInstance).get().getRyaStreamsDetails();
        assertFalse(streamsDetails.isPresent());

        // Set the details.
        final RyaStreamsDetails details = new RyaStreamsDetails("localhost", 6);
        ryaClient.getSetRyaStreamsConfiguration().setRyaStreamsConfiguration(ryaInstance, details);

        // Fetch its details again and show that they are now filled in.
        streamsDetails = ryaClient.getGetInstanceDetails().getDetails(ryaInstance).get().getRyaStreamsDetails();
        assertEquals(details, streamsDetails.get());
    }
}