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
package org.apache.rya.streams.kafka.interactor;

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.rya.streams.kafka.KafkaTopics;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Creates topics in Kafka.
 */
@DefaultAnnotation(NonNull.class)
public class CreateKafkaTopic {

    private final String zookeeperServers;

    /**
     * Constructs an instance of {@link CreateKafkaTopic}.
     *
     * @param zookeeperServers - The Zookeeper servers that are used to manage the Kafka instance. (not null)
     */
    public CreateKafkaTopic(final String zookeeperServers) {
        this.zookeeperServers = requireNonNull(zookeeperServers);
    }

    /**
     * Creates a set of Kafka topics for each topic that does not already exist.
     *
     * @param topicNames - The topics that will be created. (not null)
     * @param partitions - The number of partitions that each of the topics will have.
     * @param replicationFactor - The replication factor of the topics that are created.
     * @param topicProperties - The optional properties of the topics to create.
     */
    public void createTopics(
            final Set<String> topicNames,
            final int partitions,
            final int replicationFactor,
            final Optional<Properties> topicProperties) {
        KafkaTopics.createTopics(zookeeperServers, topicNames, partitions, replicationFactor, topicProperties);
    }
}