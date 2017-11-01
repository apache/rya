/*
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
package org.apache.rya.benchmark.periodic;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;
import org.apache.rya.api.client.accumulo.AccumuloRyaClientFactory;

import com.beust.jcommander.Parameter;
import com.google.common.base.Objects;

public class CommonOptions {

    @Parameter(names = { "-u", "--username" }, description = "Accumulo Username", required = true)
    private String username;

    @Parameter(names = { "-p", "--password" }, description = "Accumulo Password", required = true, password=true)
    private String password;

    @Parameter(names = { "-ai", "--accumulo-instance" }, description = "Accumulo Instance", required = true)
    private String accumuloInstance;

    @Parameter(names = { "-ri", "--rya-instance" }, description = "Rya Instance", required = true)
    private String ryaInstance;

    @Parameter(names = { "-z", "--zookeepers" }, description = "Accumulo Zookeepers", required = true)
    private String zookeepers;

    @Parameter(names = { "-k", "--kafka-bootstrap-servers" }, description = "Kafka bootstrap server string, for example: kafka1:9092,kafka2:9092", required = true)
    private String kafkaBootstrap;

    @Parameter(names = { "-o", "--output-directory" }, description = "The directory that output should be persisted to.", required = true)
    private File outputDirectory;

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getAccumuloInstance() {
        return accumuloInstance;
    }

    public String getRyaInstance() {
        return ryaInstance;
    }

    public String getZookeepers() {
        return zookeepers;
    }

    public String getKafkaBootstrap() {
        return kafkaBootstrap;
    }

    public File getOutputDirectory() {
        return outputDirectory;
    }

    public RyaClient buildRyaClient() throws AccumuloException, AccumuloSecurityException {
        final Instance instance = new ZooKeeperInstance(accumuloInstance, zookeepers);
        final AccumuloConnectionDetails accumuloDetails = new AccumuloConnectionDetails(username, password.toCharArray(), accumuloInstance, zookeepers);
        return AccumuloRyaClientFactory.build(accumuloDetails, instance.getConnector(username, new PasswordToken(password)));
    }

    public Properties getKafkaConsumerProperties() {
        final Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerProps.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "5000");  // reduce this value to 5 seconds for the scenario where we subscribe before the topic exists.
        consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerProps;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("username", username)
                .add("password", "[redacted]")
                .add("accumuloInstance", accumuloInstance)
                .add("ryaInstance", ryaInstance)
                .add("zookeepers", zookeepers)
                .add("kafkaBootstrap", kafkaBootstrap)
                .add("outputDirectory", outputDirectory)
                .toString();
    }
}
