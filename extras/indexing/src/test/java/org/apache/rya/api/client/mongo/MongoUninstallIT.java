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

import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.InstanceExists;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.client.Uninstall;
import org.apache.rya.mongodb.MongoTestBase;
import org.junit.Test;

import com.mongodb.MongoException;

/**
 * Integration tests the methods of {@link MongoUninstall}.
 */
public class MongoUninstallIT extends MongoTestBase {

    @Test
    public void uninstall() throws MongoException, RyaClientException {
        // Install an instance of Rya.
        final String instanceName = "testInstance_";
        final InstallConfiguration installConfig = InstallConfiguration.builder().setEnableTableHashPrefix(true).setEnableEntityCentricIndex(true).setEnableFreeTextIndex(true).setEnableTemporalIndex(true).setEnablePcjIndex(true).setEnableGeoIndex(true).setFluoPcjAppName("fluo_app_name").build();

        final Install install = new MongoInstall(getConnectionDetails(), conf.getMongoClient());
        install.install(instanceName, installConfig);

        // Show that the instance exists.
        final InstanceExists instanceExists = new MongoInstanceExists(getConnectionDetails(), conf.getMongoClient());
        assertTrue( instanceExists.exists(instanceName) );

        // Uninstall the instance
        final Uninstall uninstall = new MongoUninstall(getConnectionDetails(), conf.getMongoClient());
        uninstall.uninstall(instanceName);

        // Check that the instance no longer exists.
        assertFalse(instanceExists.exists(instanceName));
    }

    @Test(expected = InstanceDoesNotExistException.class)
    public void uninstall_instanceDoesNotExists() throws MongoException, RyaClientException {
        // Install an instance of Rya.
        final String instanceName = "testInstance_";

        // Uninstall the instance
        final Uninstall uninstall = new MongoUninstall(getConnectionDetails(), conf.getMongoClient());
        uninstall.uninstall(instanceName);
    }

    /**
     * @return copy from conf to MongoConnectionDetails
     */
    private MongoConnectionDetails getConnectionDetails() {//
        return new MongoConnectionDetails(
                conf.getMongoUser(),
                null,//conf.getMongoPassword().toCharArray(),
                conf.getMongoHostname(),
                Integer.parseInt(conf.getMongoPort()));
    }
}