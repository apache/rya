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

import static org.apache.rya.mongodb.instance.MongoRyaInstanceDetailsRepository.INSTANCE_DETAILS_COLLECTION_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.mongodb.MongoTestBase;
import org.junit.Test;

/**
 * Integration tests the methods of {@link MongoInstall}.
 */
public class MongoInstallIT extends MongoTestBase {

    @Test
    public void install() throws DuplicateInstanceNameException, RyaClientException {
        // Install an instance of Rya.
        final String ryaInstance = conf.getMongoDBName();

        // Setup the connection details that were used for the embedded Mongo DB instance we are testing with.
        final MongoConnectionDetails connectionDetails = getConnectionDetails();

        // Check that the instance does not exist.
        final InstanceExists instanceExists = new MongoInstanceExists(getMongoClient());
        assertFalse(instanceExists.exists(ryaInstance));

        // Install an instance of Rya with all the valid options turned on.
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(true)
                .setEnableFreeTextIndex(true)
                .setEnableTemporalIndex(true)
                .build();

        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, getMongoClient());
        final Install install = ryaClient.getInstall();
        install.install(ryaInstance, installConfig);

        // Check that the instance exists.
        assertTrue(instanceExists.exists(ryaInstance));

        // Show that the expected collections were created within the database.
        final List<String> expected = Arrays.asList(INSTANCE_DETAILS_COLLECTION_NAME, "rya_triples");
        int count = 0;
        final List<String> found = new ArrayList<>();
        for (final String collection : getMongoClient().getDatabase(conf.getMongoDBName()).listCollectionNames()) {
            count += expected.contains(collection) ? 1 : 0;
            found.add( collection );
        }
        assertTrue("Tables missing from:" + expected + " actual:" + found, expected.size() == count);
        assertTrue("Instance should exist.", instanceExists.exists(ryaInstance));
    }

    @Test(expected = DuplicateInstanceNameException.class)
    public void install_alreadyExists() throws DuplicateInstanceNameException, RyaClientException {
        // Install an instance of Rya.
        final String instanceName = conf.getRyaInstanceName();
        final InstallConfiguration installConfig = InstallConfiguration.builder().build();

        final MongoConnectionDetails connectionDetails = getConnectionDetails();

        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, getMongoClient());
        final Install install = ryaClient.getInstall();
        install.install(instanceName, installConfig);

        // Install it again throws expected error.
        install.install(instanceName, installConfig);
    }

    private MongoConnectionDetails getConnectionDetails() {
        final Optional<char[]> password = conf.getMongoPassword() != null ?
                Optional.of(conf.getMongoPassword().toCharArray()) :
                    Optional.empty();

        return new MongoConnectionDetails(
                conf.getMongoHostname(),
                Integer.parseInt(conf.getMongoPort()),
                Optional.ofNullable(conf.getMongoUser()),
                password);
    }
}