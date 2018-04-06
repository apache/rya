/*
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

import org.apache.kafka.common.config.ConfigException;

import kafka.log.LogConfig;

/**
 * Properties builder to be used when creating new Kafka Topics.
 *
 * Descriptions of properties can be found at
 * {@link https://kafka.apache.org/documentation/#topicconfigs}
 */
public class KafkaTopicPropertiesBuilder {
    private Optional<String> cleanupPolicy;
    /**
     * Sets the cleanup.policy of the Kafka Topic.
     * Valid properties are:
     * {@link LogConfig#compact()} and {@link LogConfig#Delete()}
     *
     * @param policy - The cleanup policy to use.
     * @return The builder.
     */
    public KafkaTopicPropertiesBuilder setCleanupPolicy(final String policy) {
        cleanupPolicy = Optional.of(requireNonNull(policy));
        return this;
    }

    /**
     * Builds the Kafka topic properties.
     * @return The {@link Properties} of the Kafka Topic.
     * @throws ConfigException - If any of the properties are misconfigured.
     */
    public Properties build() throws ConfigException {
        final Properties props = new Properties();

        if(cleanupPolicy.isPresent()) {
            props.setProperty(LogConfig.CleanupPolicyProp(), cleanupPolicy.get());
        }

        LogConfig.validate(props);
        return props;
    }
}
