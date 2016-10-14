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

import java.util.Date;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.junit.Test;

import com.google.common.base.Optional;

import org.apache.rya.accumulo.AccumuloITBase;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.GeoIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;

/**
 * Tests the methods of {@link AccumuloGetInstanceDetails}.
 */
public class AccumuloGetInstanceDetailsIT extends AccumuloITBase {

    @Test
    public void getDetails() throws AccumuloException, AccumuloSecurityException, DuplicateInstanceNameException, RyaClientException {
        // Install an instance of Rya.
        final String instanceName = "instance_name";
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
        install.install(instanceName, installConfig);

        // Verify the correct details were persisted.
        final GetInstanceDetails getInstanceDetails = new AccumuloGetInstanceDetails(connectionDetails, getConnector());
        final Optional<RyaDetails> details = getInstanceDetails.getDetails(instanceName);

        final RyaDetails expectedDetails = RyaDetails.builder()
                .setRyaInstanceName(instanceName)

                // The version depends on how the test is packaged, so just grab whatever was stored.
                .setRyaVersion( details.get().getRyaVersion() )

                .setGeoIndexDetails( new GeoIndexDetails(true) )
                .setTemporalIndexDetails(new TemporalIndexDetails(true) )
                .setFreeTextDetails( new FreeTextIndexDetails(true) )
                .setEntityCentricIndexDetails( new EntityCentricIndexDetails(true) )
                .setPCJIndexDetails(
                        PCJIndexDetails.builder()
                            .setEnabled(true)
                            .setFluoDetails( new FluoDetails("fluo_app_name") ))
                .setProspectorDetails( new ProspectorDetails(Optional.<Date>absent()) )
                .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.<Date>absent()) )
                .build();

        assertEquals(expectedDetails, details.get());
    }

    @Test(expected = InstanceDoesNotExistException.class)
    public void getDetails_instanceDoesNotExist() throws AccumuloException, AccumuloSecurityException, InstanceDoesNotExistException, RyaClientException {
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final GetInstanceDetails getInstanceDetails = new AccumuloGetInstanceDetails(connectionDetails, getConnector());
        getInstanceDetails.getDetails("instance_name");
    }

    @Test
    public void getDetails_instanceDoesNotHaveDetails() throws AccumuloException, AccumuloSecurityException, InstanceDoesNotExistException, RyaClientException, TableExistsException {
        // Mimic a pre-details rya install.
        final String instanceName = "instance_name";

        final TableOperations tableOps = getConnector().tableOperations();

        final String spoTableName = instanceName + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX;
        final String ospTableName = instanceName + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX;
        final String poTableName = instanceName + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX;
        tableOps.create(spoTableName);
        tableOps.create(ospTableName);
        tableOps.create(poTableName);

        // Verify that the operation returns empty.
        final AccumuloConnectionDetails connectionDetails = new AccumuloConnectionDetails(
                getUsername(),
                getPassword().toCharArray(),
                getInstanceName(),
                getZookeepers());

        final GetInstanceDetails getInstanceDetails = new AccumuloGetInstanceDetails(connectionDetails, getConnector());
        final Optional<RyaDetails> details = getInstanceDetails.getDetails(instanceName);
        assertFalse( details.isPresent() );
    }
}