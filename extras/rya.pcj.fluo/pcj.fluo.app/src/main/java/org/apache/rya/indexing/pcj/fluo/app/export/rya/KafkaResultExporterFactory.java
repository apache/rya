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
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporterFactory;

import com.google.common.base.Optional;

/**
 * Creates instances of {@link KafkaResultExporter}.
 */
public class KafkaResultExporterFactory implements IncrementalResultExporterFactory {

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporterFactory#build(org.apache.fluo.api.observer.Observer.Context)
     */
    @Override
    public Optional<IncrementalResultExporter> build(Context context) throws IncrementalExporterFactoryException, ConfigurationException {
        final KafkaExportParameters params = new KafkaExportParameters(context.getObserverConfiguration().toMap());
        System.out.println("KafkaResultExporterFactory.build(): params.isExportToKafka()=" + params.isExportToKafka());
        if (params.isExportToKafka()) {
            // Setup Kafka connection
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(params)//TODO;
            // Create the exporter
            final IncrementalResultExporter exporter = new KafkaResultExporter(producer);
            return Optional.of(exporter);
        } else {
            return Optional.absent();
        }
    }

}
