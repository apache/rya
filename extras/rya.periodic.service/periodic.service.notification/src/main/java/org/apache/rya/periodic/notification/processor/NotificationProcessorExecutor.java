
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.periodic.notification.api.LifeCycle;
import org.apache.rya.periodic.notification.api.NodeBin;
import org.apache.rya.periodic.notification.exporter.BindingSetRecord;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;

import com.google.common.base.Preconditions;

/**
 * Executor service that runs {@link TimestampedNotificationProcessor}s with basic
 * functionality for starting, stopping, and determining whether notification processors are
 * being executed. 
 *
 */
public class NotificationProcessorExecutor implements LifeCycle {

    private static final Logger log = Logger.getLogger(TimestampedNotificationProcessor.class);
    private BlockingQueue<TimestampedNotification> notifications; // notifications
    private BlockingQueue<NodeBin> bins; // entries to delete from Fluo
    private BlockingQueue<BindingSetRecord> bindingSets; // query results to
                                                         // export
    private PeriodicQueryResultStorage periodicStorage;
    private List<TimestampedNotificationProcessor> processors;
    private int numberThreads;
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
    public NotificationProcessorExecutor(PeriodicQueryResultStorage periodicStorage, BlockingQueue<TimestampedNotification> notifications,
            BlockingQueue<NodeBin> bins, BlockingQueue<BindingSetRecord> bindingSets, int numberThreads) {
        this.notifications = Preconditions.checkNotNull(notifications);
        this.bins = Preconditions.checkNotNull(bins);
        this.bindingSets = Preconditions.checkNotNull(bindingSets);
        this.periodicStorage = periodicStorage;
        this.numberThreads = numberThreads;
        processors = new ArrayList<>();
    }

    @Override
    public void start() {
        if (!running) {
            executor = Executors.newFixedThreadPool(numberThreads);
            for (int threadNumber = 0; threadNumber < numberThreads; threadNumber++) {
                log.info("Creating exporter:" + threadNumber);
                TimestampedNotificationProcessor processor = TimestampedNotificationProcessor.builder().setBindingSets(bindingSets)
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
