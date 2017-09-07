
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
 */package org.apache.rya.periodic.notification.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.periodic.notification.api.BindingSetRecord;
import org.apache.rya.periodic.notification.api.LifeCycle;
import org.apache.rya.periodic.notification.api.NodeBin;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executor service that runs {@link TimestampedNotificationProcessor}s with basic
 * functionality for starting, stopping, and determining whether notification processors are
 * being executed.
 *
 */
public class NotificationProcessorExecutor implements LifeCycle {

    private static final Logger log = LoggerFactory.getLogger(NotificationProcessorExecutor.class);
    private final BlockingQueue<TimestampedNotification> notifications;

    /**
     * entries to delete from Fluo
     */
    private final BlockingQueue<NodeBin> bins;

    /**
     * query results to export
     */
    private final BlockingQueue<BindingSetRecord> bindingSets;
    private final PeriodicQueryResultStorage periodicStorage;
    private final List<TimestampedNotificationProcessor> processors;
    private final int numberThreads;
    private ExecutorService executor;
    private boolean running = false;

    /**
     * Creates NotificationProcessorExecutor.
     * @param periodicStorage - storage layer that periodic results are read from
     * @param notifications - notifications are pulled from this queue, and the timestamp indicates which bin of results to query for
     * @param bins - after notifications are processed, they are added to the bin to be deleted
     * @param bindingSets - results read from the storage layer to be exported
     * @param numberThreads - number of threads used for processing
     */
    public NotificationProcessorExecutor(final PeriodicQueryResultStorage periodicStorage, final BlockingQueue<TimestampedNotification> notifications,
            final BlockingQueue<NodeBin> bins, final BlockingQueue<BindingSetRecord> bindingSets, final int numberThreads) {
        this.notifications = Objects.requireNonNull(notifications);
        this.bins = Objects.requireNonNull(bins);
        this.bindingSets = Objects.requireNonNull(bindingSets);
        this.periodicStorage = periodicStorage;
        this.numberThreads = numberThreads;
        processors = new ArrayList<>();
    }

    @Override
    public void start() {
        if (!running) {
            executor = Executors.newFixedThreadPool(numberThreads);
            for (int threadNumber = 0; threadNumber < numberThreads; threadNumber++) {
                log.info("Creating processor for thread: {}", threadNumber);
                final TimestampedNotificationProcessor processor = TimestampedNotificationProcessor.builder().setBindingSets(bindingSets)
                        .setBins(bins).setPeriodicStorage(periodicStorage).setNotifications(notifications).setThreadNumber(threadNumber)
                        .build();
                processors.add(processor);
                executor.submit(processor);
            }
            running = true;
        }
    }

    @Override
    public void stop() {
        if (processors != null && processors.size() > 0) {
            processors.forEach(x -> x.shutdown());
        }
        if (executor != null) {
            executor.shutdown();
        }
        running = false;
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.info("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                executor.shutdownNow();
            }
        } catch (final InterruptedException e) {
            log.info("Interrupted during shutdown, exiting uncleanly");
        }
    }

    @Override
    public boolean currentlyRunning() {
        return running;
    }

}
