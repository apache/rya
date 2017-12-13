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
package org.apache.rya.api.client.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Date;

import org.apache.accumulo.core.client.TableExistsException;
import org.apache.rya.api.client.GetInstanceDetails;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;
import org.apache.rya.mongodb.MongoTestBase;
import org.junit.Test;

import com.google.common.base.Optional;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoException;

/**
 * Tests the methods of {@link MongoGetInstanceDetails}.
 */
public class MongoGetInstanceDetailsIT extends MongoTestBase {

    @Test
    public void getDetails() throws MongoException, RyaClientException {
        final String instanceName = "instance";
        // Install an instance of Rya.
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(true)
                .setEnableEntityCentricIndex(true)
                .setEnableFreeTextIndex(true)
                .setEnableTemporalIndex(true)
                .setEnableGeoIndex(true)
                .setEnablePcjIndex(false)
                .build();

        final Install install = new MongoInstall(getConnectionDetails(), this.getMongoClient());
        install.install(instanceName, installConfig);

        // Verify the correct details were persisted.
        final GetInstanceDetails getInstanceDetails = new MongoGetInstanceDetails(getConnectionDetails(), this.getMongoClient());
        final Optional<RyaDetails> details = getInstanceDetails.getDetails(instanceName);

        final RyaDetails expectedDetails = RyaDetails.builder()
                .setRyaInstanceName(instanceName)

                // The version depends on how the test is packaged, so just grab whatever was stored.
                .setRyaVersion( details.get().getRyaVersion() )

                        // FIXME .setGeoIndexDetails( new GeoIndexDetails(true) )
                .setTemporalIndexDetails(new TemporalIndexDetails(true) )
                .setFreeTextDetails( new FreeTextIndexDetails(true) )
                .setEntityCentricIndexDetails( new EntityCentricIndexDetails(false) )
                .setPCJIndexDetails(
                        PCJIndexDetails.builder()
                            .setEnabled(false))
                .setProspectorDetails( new ProspectorDetails(Optional.<Date>absent()) )
                .setJoinSelectivityDetails( new JoinSelectivityDetails(Optional.<Date>absent()) )
                .build();

        assertEquals(expectedDetails, details.get());
    }

    @Test(expected = InstanceDoesNotExistException.class)
    public void getDetails_instanceDoesNotExist() throws MongoException, RyaClientException {
        final GetInstanceDetails getInstanceDetails = new MongoGetInstanceDetails(getConnectionDetails(), conf.getMongoClient());
        getInstanceDetails.getDetails("instance_name");
    }

    @Test
    public void getDetails_instanceDoesNotHaveDetails() throws MongoException, TableExistsException, RyaClientException {
        // Mimic a pre-details rya install.
        final String instanceName = "instance_name";

        this.getMongoClient().getDB(instanceName).createCollection("rya_triples", new BasicDBObject());

        // Verify that the operation returns empty.
        final GetInstanceDetails getInstanceDetails = new MongoGetInstanceDetails(getConnectionDetails(), this.getMongoClient());
        final Optional<RyaDetails> details = getInstanceDetails.getDetails(instanceName);
        assertFalse( details.isPresent() );
    }
    
    /**
     * @return copy from conf to MongoConnectionDetails
     */
    private MongoConnectionDetails getConnectionDetails() {
        final MongoConnectionDetails connectionDetails = new MongoConnectionDetails(
                        conf.getMongoUser(), //
                        conf.getMongoPassword().toCharArray(), //
                        conf.getMongoDBName(), // aka instance
                        conf.getMongoInstance(), // aka hostname
                        conf.getCollectionName()
        );
        return connectionDetails;
    }
}