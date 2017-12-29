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
import java.util.Optional;

import org.apache.rya.api.client.Install;
import org.apache.rya.api.client.Install.DuplicateInstanceNameException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.ListInstances;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.mongodb.MongoITBase;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.mongodb.MongoException;

/**
 * Integration tests the methods of {@link MongoListInstances}.
 */
public class MongoListInstancesIT extends MongoITBase {

    @Test
    public void listInstances_hasRyaDetailsTable() throws MongoException, DuplicateInstanceNameException, RyaClientException {
        // Install a few instances of Rya using the install command.
        final RyaClient ryaClient = MongoRyaClientFactory.build(getConnectionDetails(), getMongoClient());
        final Install install = ryaClient.getInstall();
        install.install("instance1_", InstallConfiguration.builder().build());
        install.install("instance2_", InstallConfiguration.builder().build());
        install.install("instance3_", InstallConfiguration.builder().build());

        // Fetch the list and verify it matches what is expected.
        final ListInstances listInstances = new MongoListInstances(getMongoClient());
        final List<String> instances = listInstances.listInstances();
        Collections.sort(instances);

        final List<String> expected = Lists.newArrayList("instance1_", "instance2_", "instance3_");
        assertEquals(expected, instances);
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