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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.accumulo.AccumuloInstall;
import org.apache.rya.mongodb.MongoTestBase;
import org.junit.Test;

/**
 * Integration tests the methods of {@link AccumuloInstall}.
 */
public class MongoInstallIT extends MongoTestBase {

    @Test
    public void install() throws DuplicateInstanceNameException, RyaClientException {
        // Install an instance of Rya.
        final String instanceName = conf.getCollectionName();
        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(false) //
                .setEnableEntityCentricIndex(false)//
                .setEnableFreeTextIndex(false)//
                .setEnableTemporalIndex(false)//
                .setEnablePcjIndex(false)//
                .setEnableGeoIndex(false)//
                .setFluoPcjAppName("fluo_app_name")//
                .build();

        final MongoConnectionDetails connectionDetails = new MongoConnectionDetails(
                        conf.getMongoUser(), //
                        conf.getMongoPassword().toCharArray(), //
                        conf.getMongoDBName(), // aka instance
                        conf.getMongoInstance(), // aka hostname
                        conf.getCollectionName()
        );

        // Check that the instance does not exist.
        assertFalse("Instance should NOT exist yet.", this.getMongoClient().getDatabaseNames().contains(instanceName));
        final InstanceExists instanceExists = new MongoInstanceExists(connectionDetails, this.getMongoClient());
        assertFalse("Instance should NOT exist yet.", instanceExists.exists(instanceName));

        final Install install = new MongoInstall(connectionDetails, this.getMongoClient());
        install.install(instanceName, installConfig);

        // Check that the instance exists.
        assertTrue("Instance should exist.", this.getMongoClient().getDatabaseNames().contains(instanceName));
        List<String> expected = Arrays.asList("instance_details", instanceName + "_triples");
        int count = 0;
        String found = "";
        for (String collection : this.getMongoClient().getDatabase(instanceName).listCollectionNames())
        {
            System.out.println("Collection names:" + collection);
            count += expected.contains(collection) ? 1 : 0;
            found += ", " + collection;
        }
        assertTrue("Tables missing from:" + expected + " actual:" + found, expected.size() == count);
        assertTrue("Instance should exist.", instanceExists.exists(instanceName));
    }

    @Test(expected = DuplicateInstanceNameException.class)
    public void install_alreadyExists() throws DuplicateInstanceNameException, RyaClientException {
        // Install an instance of Rya.
        final String instanceName = conf.getCollectionName();
        final InstallConfiguration installConfig = InstallConfiguration.builder().build();

        final MongoConnectionDetails connectionDetails = new MongoConnectionDetails(conf.getMongoUser(), //
                        conf.getMongoPassword().toCharArray(), //
                        conf.getMongoDBName(), // aka instance
                        conf.getMongoInstance(), // aka hostname
                        conf.getCollectionName()
        );

        final Install install = new MongoInstall(connectionDetails, this.getMongoClient());
        install.install(instanceName, installConfig);

        // Install it again throws expected error.
        install.install(instanceName, installConfig);
    }
}