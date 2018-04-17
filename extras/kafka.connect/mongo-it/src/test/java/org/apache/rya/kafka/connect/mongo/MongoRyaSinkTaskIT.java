/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.kafka.connect.mongo;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.rya.api.client.Install.InstallConfiguration;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.mongo.MongoConnectionDetails;
import org.apache.rya.api.client.mongo.MongoRyaClientFactory;
import org.apache.rya.test.mongo.MongoITBase;
import org.junit.Test;

/**
 * Integration tests the methods of {@link MongoRyaSinkTask}.
 */
public class MongoRyaSinkTaskIT extends MongoITBase {

    @Test
    public void instanceExists() throws Exception {
        // Install an instance of Rya.
        final String ryaInstanceName = "rya";
        final MongoConnectionDetails connectionDetails = new MongoConnectionDetails(
                super.getMongoHostname(),
                super.getMongoPort(),
                Optional.empty(),
                Optional.empty());

        final InstallConfiguration installConfig = InstallConfiguration.builder()
                .setEnableTableHashPrefix(false)
                .setEnableEntityCentricIndex(false)
                .setEnableFreeTextIndex(false)
                .setEnableTemporalIndex(false)
                .setEnablePcjIndex(false)
                .setEnableGeoIndex(false)
                .build();

        final RyaClient ryaClient = MongoRyaClientFactory.build(connectionDetails, super.getMongoClient());
        ryaClient.getInstall().install(ryaInstanceName, installConfig);

        // Create the task that will be tested.
        final MongoRyaSinkTask task = new MongoRyaSinkTask();

        try {
            // Configure the task to use the embedded Mongo DB instance for Rya.
            final Map<String, String> config = new HashMap<>();
            config.put(MongoRyaSinkConfig.HOSTNAME, super.getMongoHostname());
            config.put(MongoRyaSinkConfig.PORT, "" + super.getMongoPort());
            config.put(MongoRyaSinkConfig.RYA_INSTANCE_NAME, "rya");

            // This will pass because the Rya instance exists.
            task.start(config);
        } finally {
            task.stop();
        }
    }

    @Test(expected = ConnectException.class)
    public void instanceDoesNotExist() throws Exception {
        // Create the task that will be tested.
        final MongoRyaSinkTask task = new MongoRyaSinkTask();

        try {
            // Configure the task to use the embedded Mongo DB instance for Rya.
            final Map<String, String> config = new HashMap<>();
            config.put(MongoRyaSinkConfig.HOSTNAME, super.getMongoHostname());
            config.put(MongoRyaSinkConfig.PORT, "" + super.getMongoPort());
            config.put(MongoRyaSinkConfig.RYA_INSTANCE_NAME, "instance-does-not-exist");

            // Starting the task will fail because the Rya instance does not exist.
            task.start(config);
        } finally {
            task.stop();
        }
    }
}