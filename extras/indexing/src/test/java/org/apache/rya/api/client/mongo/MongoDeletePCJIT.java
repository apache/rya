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

import static org.junit.Assert.assertNull;

import org.apache.rya.api.client.CreatePCJ;
import org.apache.rya.api.client.DeletePCJ;
import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloCreatePCJ;
import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.mongodb.MongoITBase;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

/**
 * Integration tests the methods of {@link AccumuloCreatePCJ}.
 */
public class MongoDeletePCJIT extends MongoITBase {
    @Test(expected = InstanceDoesNotExistException.class)
    public void instanceDoesNotExist() throws Exception {
        final RyaClient ryaClient = MongoRyaClientFactory.build(getConnectionDetails(), getMongoClient());
        // Skip the install step to create error causing situation.
        ryaClient.getDeletePCJ().deletePCJ(conf.getRyaInstanceName(), "doesn't matter, should fail before the pcjID is needed");
    }

    @Test
    public void deletePCJ() throws Exception {
        final MongoConnectionDetails connectionDetails = getConnectionDetails();
        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, getMongoClient());
        // Initialize the commands that will be used by this test.
        final CreatePCJ createPCJ = ryaClient.getCreatePCJ();
        final Install installRya = ryaClient.getInstall();
        final InstallConfiguration installConf = InstallConfiguration.builder()
                .setEnablePcjIndex(true)
                .build();
        installRya.install(conf.getRyaInstanceName(), installConf);

        System.out.println(getMongoClient().getDatabase(conf.getRyaInstanceName()).getCollection("instance_details").find().first().toJson());
        // Create a PCJ.
        final String sparql =
                "SELECT ?x " +
                        "WHERE { " +
                        "?x <http://talksTo> <http://Eve>. " +
                        "?x <http://worksAt> <http://TacoJoint>." +
                        "}";
        final String pcjId = createPCJ.createPCJ(conf.getRyaInstanceName(), sparql);

        final DeletePCJ deletePCJ = ryaClient.getDeletePCJ();
        deletePCJ.deletePCJ(conf.getRyaInstanceName(), pcjId);

        // Verify the RyaDetails were updated to include the new PCJ.
        final Optional<RyaDetails> ryaDetails = ryaClient.getGetInstanceDetails().getDetails(conf.getRyaInstanceName());
        final ImmutableMap<String, PCJDetails> details = ryaDetails.get().getPCJIndexDetails().getPCJDetails();
        final PCJDetails pcjDetails = details.get(pcjId);

        assertNull(pcjDetails);
    }

    private MongoConnectionDetails getConnectionDetails() {
        final java.util.Optional<char[]> password = conf.getMongoPassword() != null ?
                java.util.Optional.of(conf.getMongoPassword().toCharArray()) :
                    java.util.Optional.empty();

                return new MongoConnectionDetails(
                        conf.getMongoHostname(),
                        Integer.parseInt(conf.getMongoPort()),
                        java.util.Optional.ofNullable(conf.getMongoUser()),
                        password);
    }
}