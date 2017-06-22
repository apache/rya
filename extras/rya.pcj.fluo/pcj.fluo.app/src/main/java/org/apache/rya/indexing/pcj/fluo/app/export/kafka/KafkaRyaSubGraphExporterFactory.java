package org.apache.rya.indexing.pcj.fluo.app.export.kafka;
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
import org.apache.fluo.api.observer.Observer.Context;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory.ConfigurationException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporterFactory.IncrementalExporterFactoryException;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaSubGraphExporter;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaSubGraphExporterFactory;

import com.google.common.base.Optional;

/**
 * Factory for creating {@link KafkaRyaSubGraphExporter}s that are used for
 * exporting {@link RyaSubGraph}s from the Rya Fluo application to Kafka.
 *
 */
public class KafkaRyaSubGraphExporterFactory implements IncrementalRyaSubGraphExporterFactory {

    private static final Logger log = Logger.getLogger(KafkaRyaSubGraphExporterFactory.class);
    
    /**
     * Builds a {@link KafkaRyaSubGraphExporter}.
     * @param context - {@link Context} object used to pass configuration parameters
     * @return an Optional consisting of an IncrementalSubGraphExproter if it can be constructed
     * @throws IncrementalExporterFactoryException
     * @throws ConfigurationException
     */
    @Override
    public Optional<IncrementalRyaSubGraphExporter> build(Context context) throws IncrementalExporterFactoryException, ConfigurationException {
        final KafkaExportParameters exportParams = new KafkaExportParameters(context.getObserverConfiguration().toMap());
        log.debug("KafkaRyaSubGraphExporterFactory.build(): params.isExportToKafka()=" + exportParams.isExportToKafka());
        if (exportParams.isExportToKafka()) {
            // Setup Kafka connection
            KafkaProducer<String, RyaSubGraph> producer = new KafkaProducer<String, RyaSubGraph>(exportParams.listAllConfig());
            // Create the exporter
            final IncrementalRyaSubGraphExporter exporter = new KafkaRyaSubGraphExporter(producer);
            return Optional.of(exporter);
        } else {
            return Optional.absent();
        }
    }

}
