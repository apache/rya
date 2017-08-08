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
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.base.Preconditions;


public class KafkaSubGraphExporterParameters extends KafkaExportParameterBase {

    public static final String CONF_USE_KAFKA_SUBGRAPH_EXPORTER = "pcj.fluo.export.kafka.subgraph.enabled";
    public static final String CONF_KAFKA_SUBGRAPH_SERIALIZER = "pcj.fluo.export.kafka.subgraph.serializer";
    
    public KafkaSubGraphExporterParameters(final Map<String, String> params) {
        super(params);
    }
    
    /**
     * Instructs the Fluo application to use the Kafka BindingSet Exporter
     * and sets the appropriate Key/Value Serializer parameters for writing RyaSubGraphs to Kafka.
     * @param useExporter
     *            - {@code True} if the Fluo application should use the
     *            {@link KafkaRyaSubGraphExporter}; otherwise {@code false}.
     */
    public void setUseKafkaSubgraphExporter(final boolean useExporter) {
        setBoolean(params, CONF_USE_KAFKA_SUBGRAPH_EXPORTER, useExporter);
    }

    /**
     * @return {@code True} if the Fluo application should use the {@link KafkaRyaSubGraphExporter}; otherwise
     *         {@code false}. Defaults to {@code false} if no value is present.
     */
    public boolean getUseKafkaSubgraphExporter() {
        return getBoolean(params, CONF_USE_KAFKA_SUBGRAPH_EXPORTER, false);
    }
    
    /**
     * 
     * @param serializer - Used for Serializing RyaSubGraphs pushed to Kafka
     */
    public void setKafkaSubGraphSerializer(String serializer) {
        params.put(CONF_KAFKA_SUBGRAPH_SERIALIZER, Preconditions.checkNotNull(serializer));
    }
    
    /**
     * @return - Serializer used for Serializing RyaSubGraphs to Kafka
     */
    public String getKafkaSubGraphSerializer() {
        return params.getOrDefault(CONF_KAFKA_SUBGRAPH_SERIALIZER, RyaSubGraphKafkaSerDe.class.getName());
    }
    
    @Override
    public Properties listAllConfig() {
        Properties props = super.listAllConfig();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getKafkaSubGraphSerializer());
        return props;
    }
    
}
