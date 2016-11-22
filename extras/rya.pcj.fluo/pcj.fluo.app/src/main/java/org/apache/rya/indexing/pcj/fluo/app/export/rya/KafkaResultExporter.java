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

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.charset.StandardCharsets;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalResultExporter;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;

/**
 * Incrementally exports SPARQL query results to Accumulo PCJ tables as they are defined by Rya.
 */
public class KafkaResultExporter implements IncrementalResultExporter {
    private final KafkaProducer<String, String/* serial-type */> producer;

    /**
     * Constructor
     * 
     * @param producer
     *            created by {@link KafkaResultExporterFactory}
     */
    public KafkaResultExporter(KafkaProducer<String, String/* serial-type */> producer) {
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
            String msg = "out to kafta topic: queryId=" + queryId + " pcjId=" + pcjId + " result=" + result;
            System.out.println(msg);


            BindingSetSerializer serializer = new BindingSetSerializer();
            byte[] bytes = serializer.serialize("", result);
            // Send result on topic
            ProducerRecord<String, String/* serial-type */> rec = new ProducerRecord<String, String/* serial-type */>(/* topicname= */ queryId, /* value= */ new String(bytes, StandardCharsets.UTF_8));
            // Can add a key if you need to:
            // ProducerRecord(String topic, K key, V value)
            producer.send(rec);
            System.out.println("kafa producer sent.");

        } catch (final Throwable e) {
            throw new ResultExportException("A result could not be exported to Kafka.", e);
        }
    }
}
