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
package org.apache.rya.indexing.pcj.fluo.app.export.rya;

import java.util.Map;
import java.util.Properties;

import org.apache.fluo.api.observer.Observer;
import org.apache.rya.indexing.pcj.fluo.app.export.ParametersBase;

/**
 * Provides read/write functions to the parameters map that is passed into an
 * {@link Observer#init(io.fluo.api.observer.Observer.Context)} method related
 * to PCJ exporting to a kafka topic.
 * Remember: if doesn't count unless it is added to params
 */

public class KafkaExportParameters extends ParametersBase {

    public static final String CONF_EXPORT_TO_KAFKA = "pcj.fluo.export.kafka.enabled";

    public KafkaExportParameters(final Map<String, String> params) {
        super(params);
    }

    /**
     * @param isExportToKafka
     *            - {@code True} if the Fluo application should export
     *            to Kafka; otherwise {@code false}.
     */
    public void setExportToKafka(final boolean isExportToKafka) {
        setBoolean(params, CONF_EXPORT_TO_KAFKA, isExportToKafka);
    }

    /**
     * @return {@code True} if the Fluo application should export to Kafka; otherwise
     *         {@code false}. Defaults to {@code false} if no value is present.
     */
    public boolean isExportToKafka() {
        return getBoolean(params, CONF_EXPORT_TO_KAFKA, false);
    }

    /**
     * Add the properties to the params, NOT keeping them separate from the other params.
     * Guaranteed by Properties: Each key and its corresponding value in the property list is a string.
     * 
     * @param producerConfig
     */
    public void setProducerConfig(final Properties producerConfig) {
        for (Object key : producerConfig.keySet().toArray()) {
            Object value = producerConfig.getProperty(key.toString());
            this.params.put(key.toString(), value.toString());
        }
    }

    /**
     * @return all the params (not just kafka producer Configuration) as a {@link Properties}
     */
    public Properties getProducerConfig() {
        Properties props = new Properties();
        for (Object key : params.keySet().toArray()) {
            Object value = params.get(key.toString());
            props.put(key.toString(), value.toString());
        }
        return props;
    }

}