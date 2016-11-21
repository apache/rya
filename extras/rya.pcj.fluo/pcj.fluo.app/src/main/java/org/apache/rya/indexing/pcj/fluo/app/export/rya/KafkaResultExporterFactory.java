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

import org.apache.fluo.api.observer.Observer.Context;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporterFactory;

import com.google.common.base.Optional;

/**
 * Creates instances of {@link KafkaResultExporter}.
 * <p/>
 * Configure a Kafka producer by adding several required Key/values as described here:
 * http://kafka.apache.org/documentation.html#producerconfigs
 * <p/>
 * Here is a simple example:
 * <pre>
 *     Properties producerConfig = new Properties();
 *     producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
 *     producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
 *     producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
 * </pre>
 * 
 * @see ProducerConfig
 */
public class KafkaResultExporterFactory implements IncrementalResultExporterFactory {
    @Override
    public Optional<IncrementalResultExporter> build(Context context) throws IncrementalExporterFactoryException, ConfigurationException {
        final KafkaExportParameters exportParams = new KafkaExportParameters(context.getObserverConfiguration().toMap());
        System.out.println("KafkaResultExporterFactory.build(): params.isExportToKafka()=" + exportParams.isExportToKafka());
        if (exportParams.isExportToKafka()) {
            // Setup Kafka connection
            KafkaProducer<String, byte[]> producer = new KafkaProducer<String, byte[]>(exportParams.getProducerConfig());
            // Create the exporter
            final IncrementalResultExporter exporter = new KafkaResultExporter(producer);
            return Optional.of(exporter);
        } else {
            return Optional.absent();
        }
    }

}
