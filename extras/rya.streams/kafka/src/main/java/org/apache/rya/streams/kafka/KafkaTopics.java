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
package org.apache.rya.streams.kafka;

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.I0Itec.zkclient.ZkClient;
import org.apache.rya.streams.api.queries.QueryChangeLog;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

/**
 * Creates the Kafka topic names that are used for Rya Streams systems.
 */
@DefaultAnnotation(NonNull.class)
public class KafkaTopics {

    private static final String QUERY_CHANGE_LOG_TOPIC_SUFFIX = "-QueryChangeLog";

    /**
     * Creates the Kafka topic name that is used for a specific instance of Rya's {@link QueryChangeLog}.
     *
     * @param ryaInstance - The Rya instance the change log is for. (not null)
     * @return The name of the Kafka topic.
     */
    public static String queryChangeLogTopic(final String ryaInstance) {
        requireNonNull(ryaInstance);
        return ryaInstance + QUERY_CHANGE_LOG_TOPIC_SUFFIX;
    }

    /**
     * Get the Rya instance name from a Kafka topic name that has been used for a {@link QueryChnageLog}.
     * <p/>
     * This is the inverse function of {@link #queryChangeLogTopic(String)}.
     *
     * @param changeLogTopic - The topic to evaluate. (not null)
     * @return If the topic is well formatted, then the Rya instance name that was part of the topic name.
     */
    public static Optional<String> getRyaInstance(final String changeLogTopic) {
        requireNonNull(changeLogTopic);

        // Return absent if the provided topic does not represent a query change log topic.
        if(!changeLogTopic.endsWith(QUERY_CHANGE_LOG_TOPIC_SUFFIX)) {
            return Optional.empty();
        }

        // Everything before the suffix is the Rya instance name.
        final int endIndex = changeLogTopic.length() - QUERY_CHANGE_LOG_TOPIC_SUFFIX.length();
        return Optional.of( changeLogTopic.substring(0, endIndex) );
    }

    /**
     * Creates the Kafka topic name that is used to load statements into the Rya Streams system for a specific
     * instance of Rya.
     *
     * @param ryaInstance - The Rya instance the statements are for. (not null)
     * @return The name of the Kafka topic.
     */
    public static String statementsTopic(final String ryaInstance) {
        requireNonNull(ryaInstance);
        return ryaInstance + "-Statements";
    }

    /**
     * Creates the Kafka topic name that is used for a specific query that is managed within Rya Streams.
     *
     * @param queryId - The id of the query the topic is for.
     * @return The name of the Kafka topic.
     */
    public static String queryResultsTopic(final UUID queryId) {
        requireNonNull(queryId);
        return "QueryResults-" + queryId.toString();
    }

    /**
     * Creates a set of Kafka topics for each topic that does not already exist.
     *
     * @param zookeeperServers - The Zookeeper servers that are used by the Kafka Streams program. (not null)
     * @param topicNames - The topics that will be created. (not null)
     * @param partitions - The number of partitions that each of the topics will have.
     * @param replicationFactor - The replication factor of the topics that are created.
     */
    public static void createTopics(
            final String zookeeperServers,
            final Set<String> topicNames,
            final int partitions,
            final int replicationFactor) {
        requireNonNull(zookeeperServers);
        requireNonNull(topicNames);

        ZkUtils zkUtils = null;
        try {
            zkUtils = ZkUtils.apply(new ZkClient(zookeeperServers, 30000, 30000, ZKStringSerializer$.MODULE$), false);
            for(final String topicName : topicNames) {
                if(!AdminUtils.topicExists(zkUtils, topicName)) {
                    AdminUtils.createTopic(zkUtils, topicName, partitions, replicationFactor, new Properties(), RackAwareMode.Disabled$.MODULE$);
                }
            }
        }
        finally {
            if(zkUtils != null) {
                zkUtils.close();
            }
        }
    }
}