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

import java.util.Collections;
import java.util.List;

import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.ListInstances;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.mongodb.MongoTestBase;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.mongodb.MongoException;

/**
 * Integration tests the methods of {@link MongoListInstances}.
 */
public class MongoListInstancesIT extends MongoTestBase {

    @Test
    public void listInstances_hasRyaDetailsTable() throws MongoException, DuplicateInstanceNameException, RyaClientException {
        // Install a few instances of Rya using the install command.
        final Install install = new MongoInstall(getConnectionDetails(), getMongoClient());
        install.install("instance1_", InstallConfiguration.builder().build());
        install.install("instance2_", InstallConfiguration.builder().build());
        install.install("instance3_", InstallConfiguration.builder().build());

        // Fetch the list and verify it matches what is expected.
        final ListInstances listInstances = new MongoListInstances(getConnectionDetails(), getMongoClient());
        final List<String> instances = listInstances.listInstances();
        Collections.sort(instances);

        final List<String> expected = Lists.newArrayList("instance1_", "instance2_", "instance3_");
        assertEquals(expected, instances);
    }

    /**
     * @return copy from conf to MongoConnectionDetails
     */
    private MongoConnectionDetails getConnectionDetails() {
        return new MongoConnectionDetails(
                conf.getMongoUser(),
                conf.getMongoPassword().toCharArray(),
                conf.getMongoInstance(),
                Integer.parseInt( conf.getMongoPort() ));
    }
}