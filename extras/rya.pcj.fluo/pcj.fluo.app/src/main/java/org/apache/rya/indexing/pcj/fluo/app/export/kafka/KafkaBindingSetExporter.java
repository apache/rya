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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;

/**
 * Incrementally exports SPARQL query results to Kafka topics.
 */
public class KafkaBindingSetExporter implements IncrementalBindingSetExporter {
    private static final Logger log = Logger.getLogger(KafkaBindingSetExporter.class);

    private final KafkaProducer<String, VisibilityBindingSet> producer;

    /**
     * Constructs an instance given a Kafka producer.
     *
     * @param producer
     *            for sending result set alerts to a broker. (not null)
     *            Can be created and configured by {@link KafkaBindingSetExporterFactory}
     */
    public KafkaBindingSetExporter(KafkaProducer<String, VisibilityBindingSet> producer) {
        super();
        checkNotNull(producer, "Producer is required.");
        this.producer = producer;
    }

    /**
     * Send the results to the topic using the queryID as the topicname
     */
    @Override
    public void export(final TransactionBase fluoTx, final String queryId, final VisibilityBindingSet result) throws ResultExportException {
        checkNotNull(fluoTx);
        checkNotNull(queryId);
        checkNotNull(result);
        try {
            final String pcjId = fluoTx.gets(queryId, FluoQueryColumns.RYA_PCJ_ID);
            final String msg = "out to kafta topic: queryId=" + queryId + " pcjId=" + pcjId + " result=" + result;
            log.trace(msg);

            // Send the result to the topic whose name matches the PCJ ID.
            final ProducerRecord<String, VisibilityBindingSet> rec = new ProducerRecord<>(pcjId, result);
            final Future<RecordMetadata> future = producer.send(rec);

            // Don't let the export return until the result has been written to the topic. Otherwise we may lose results.
            future.get();

            log.debug("producer.send(rec) completed");

        } catch (final Throwable e) {
            throw new ResultExportException("A result could not be exported to Kafka.", e);
        }
    }

    @Override
    public void close() throws Exception {
        producer.close(5, TimeUnit.SECONDS);
    }
}