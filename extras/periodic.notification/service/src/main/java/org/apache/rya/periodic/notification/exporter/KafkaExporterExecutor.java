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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;
import org.apache.rya.periodic.notification.api.BindingSetExporter;
import org.apache.rya.periodic.notification.api.BindingSetRecord;
import org.apache.rya.periodic.notification.api.LifeCycle;
import org.openrdf.query.BindingSet;

import jline.internal.Preconditions;

/**
 * Executor service that runs {@link KafkaPeriodicBindingSetExporter}s.  
 *
 */
public class KafkaExporterExecutor implements LifeCycle {

    private static final Logger log = Logger.getLogger(BindingSetExporter.class);
    private KafkaProducer<String, BindingSet> producer;
    private BlockingQueue<BindingSetRecord> bindingSets;
    private ExecutorService executor;
    private List<KafkaPeriodicBindingSetExporter> exporters;
    private int num_Threads;
    private boolean running = false;

    /**
     * Creates a KafkaExporterExecutor for exporting periodic query results to Kafka.
     * @param producer for publishing results to Kafka
     * @param num_Threads number of threads used to publish results
     * @param bindingSets - work queue containing {@link BindingSet}s to be published
     */
    public KafkaExporterExecutor(KafkaProducer<String, BindingSet> producer, int num_Threads, BlockingQueue<BindingSetRecord> bindingSets) {
        Preconditions.checkNotNull(producer);
        Preconditions.checkNotNull(bindingSets);
        this.producer = producer;
        this.bindingSets = bindingSets;
        this.num_Threads = num_Threads;
        this.exporters = new ArrayList<>();
    }

    @Override
    public void start() {
        if (!running) {
            executor = Executors.newFixedThreadPool(num_Threads);

            for (int threadNumber = 0; threadNumber < num_Threads; threadNumber++) {
                log.info("Creating exporter:" + threadNumber);
                KafkaPeriodicBindingSetExporter exporter = new KafkaPeriodicBindingSetExporter(producer, threadNumber, bindingSets);
                exporters.add(exporter);
                executor.submit(exporter);
            }
            running = true;
        }
    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdown();
        }

        if (exporters != null && exporters.size() > 0) {
            exporters.forEach(x -> x.shutdown());
        }

        if (producer != null) {
            producer.close();
        }

        running = false;
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                log.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.info("Interrupted during shutdown, exiting uncleanly");
        }
    }

    @Override
    public boolean currentlyRunning() {
        return running;
    }
}
