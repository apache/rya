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
package org.apache.rya.periodic.notification.exporter;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;
import org.apache.rya.periodic.notification.api.BindingSetExporter;
import org.apache.rya.periodic.notification.api.BindingSetRecord;
import org.apache.rya.periodic.notification.api.BindingSetRecordExportException;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.query.BindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Object that exports {@link BindingSet}s to the Kafka topic indicated by
 * the {@link BindingSetRecord}.
 *
 */
public class KafkaPeriodicBindingSetExporter implements BindingSetExporter, Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaPeriodicBindingSetExporter.class);
    private final KafkaProducer<String, BindingSet> producer;
    private final BlockingQueue<BindingSetRecord> bindingSets;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final int threadNumber;

    public KafkaPeriodicBindingSetExporter(final KafkaProducer<String, BindingSet> producer, final int threadNumber,
            final BlockingQueue<BindingSetRecord> bindingSets) {
        this.threadNumber = threadNumber;
        this.producer = Objects.requireNonNull(producer);
        this.bindingSets = Objects.requireNonNull(bindingSets);
    }

    /**
     * Exports BindingSets to Kafka.  The BindingSet and topic are extracted from
     * the indicated BindingSetRecord and the BindingSet is then exported to the topic.
     */
    @Override
    public void exportNotification(final BindingSetRecord record) throws BindingSetRecordExportException {
        try {
            log.info("Exporting {} records to Kafka to topic: {}", record.getBindingSet().size(), record.getTopic());
            final String bindingName = IncrementalUpdateConstants.PERIODIC_BIN_ID;

            final BindingSet bindingSet = record.getBindingSet();
            final String topic = record.getTopic();
            final long binId = ((Literal) bindingSet.getValue(bindingName)).longValue();

            final Future<RecordMetadata> future = producer
                .send(new ProducerRecord<String, BindingSet>(topic, Long.toString(binId), bindingSet));
            //wait for confirmation that results have been received
            future.get(5, TimeUnit.SECONDS);
        } catch (final Exception e) {  // catch all possible exceptional behavior and throw as our checked exception.
            throw new BindingSetRecordExportException(e.getMessage(), e);
        }
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                exportNotification(bindingSets.take());
            }
        } catch (InterruptedException | BindingSetRecordExportException e) {
            log.warn("Thread " + threadNumber + " is unable to process message.", e);
        }
    }


    public void shutdown() {
        closed.set(true);
    }

}
