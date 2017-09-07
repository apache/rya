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
package org.apache.rya.indexing.pcj.fluo.app.export.kafka;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.fluo.api.observer.Observer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.rya.indexing.pcj.fluo.app.export.ParametersBase;

/**
 * Provides read/write functions to the parameters map that is passed into an
 * {@link Observer#init(io.fluo.api.observer.Observer.Context)} method related
 * to PCJ exporting to a kafka topic.
 * Remember: if doesn't count unless it is added to params
 */

public class KafkaExportParameterBase extends ParametersBase {

    public KafkaExportParameterBase(final Map<String, String> params) {
        super(params);
    }

    /**
     * Sets the bootstrap servers for reading from and writing to Kafka
     * @param bootstrapServers - connect string for Kafka brokers
     */
    public void setKafkaBootStrapServers(String bootstrapServers) {
        params.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Objects.requireNonNull(bootstrapServers));
    }
    
    /**
     * @return Connect string for Kafka servers
     */
    public Optional<String> getKafkaBootStrapServers() {
        return Optional.ofNullable(params.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    }

    /**
     * Add the properties to the params, NOT keeping them separate from the other params.
     * Guaranteed by Properties: Each key and its corresponding value in the property list is a string.
     * 
     * @param producerConfig
     */
    public void addAllProducerConfig(final Properties producerConfig) {
        for (Object key : producerConfig.keySet().toArray()) {
            Object value = producerConfig.getProperty(key.toString());
            this.params.put(key.toString(), value.toString());
        }
    }

    /**
     * Collect all the properties
     * 
     * @return all the params (not just kafka producer Configuration) as a {@link Properties}
     */
    public Properties listAllConfig() {
        Properties props = new Properties();
        for (Object key : params.keySet().toArray()) {
            Object value = params.get(key.toString());
            props.put(key.toString(), value.toString());
        }
        return props;
    }

}