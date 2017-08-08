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
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.CreatePCJ.QueryType;
import org.apache.rya.api.domain.RyaSubGraph;
import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalBindingSetExporter.ResultExportException;

import com.google.common.collect.Sets;

import org.apache.rya.indexing.pcj.fluo.app.export.IncrementalRyaSubGraphExporter;

/**
 * Exports {@link RyaSubGraph}s to Kafka from Rya Fluo Application 
 *
 */
public class KafkaRyaSubGraphExporter implements IncrementalRyaSubGraphExporter {

    private final KafkaProducer<String, RyaSubGraph> producer;
    private static final Logger log = Logger.getLogger(KafkaRyaSubGraphExporter.class);

    public KafkaRyaSubGraphExporter(KafkaProducer<String, RyaSubGraph> producer) {
        checkNotNull(producer);
        this.producer = producer;
    }
    
    /**
     * Exports the RyaSubGraph to a Kafka topic equivalent to the result returned by {@link RyaSubGraph#getId()}
     * @param subgraph - RyaSubGraph exported to Kafka
     * @param contructID - rowID of result that is exported. Used for logging purposes.
     */
    @Override
    public void export(String constructID, RyaSubGraph subGraph) throws ResultExportException {
        checkNotNull(constructID);
        checkNotNull(subGraph);
        try {
            // Send the result to the topic whose name matches the PCJ ID.
            final ProducerRecord<String, RyaSubGraph> rec = new ProducerRecord<>(subGraph.getId(), subGraph);
            final Future<RecordMetadata> future = producer.send(rec);

            // Don't let the export return until the result has been written to the topic. Otherwise we may lose results.
            future.get();

            log.debug("Producer successfully sent record with id: " + constructID + " and statements: " + subGraph.getStatements());

        } catch (final Throwable e) {
            throw new ResultExportException("A result could not be exported to Kafka.", e);
        }
    }

    /**
     * Closes exporter.
     */
    @Override
    public void close() throws Exception {
        producer.close(5, TimeUnit.SECONDS);
    }

    @Override
    public Set<QueryType> getQueryTypes() {
        return Sets.newHashSet(QueryType.Construct);
    }

    @Override
    public ExportStrategy getExportStrategy() {
        return ExportStrategy.Kafka;
    }

}
