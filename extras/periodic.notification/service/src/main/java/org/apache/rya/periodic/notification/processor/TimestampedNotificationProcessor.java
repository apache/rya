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
package org.apache.rya.periodic.notification.processor;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.PeriodicQueryResultStorage;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.CloseableIterator;
import org.apache.rya.periodic.notification.api.BinPruner;
import org.apache.rya.periodic.notification.api.BindingSetRecord;
import org.apache.rya.periodic.notification.api.NodeBin;
import org.apache.rya.periodic.notification.api.NotificationProcessor;
import org.apache.rya.periodic.notification.exporter.KafkaPeriodicBindingSetExporter;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.openrdf.query.BindingSet;

import com.google.common.base.Preconditions;

/**
 * Implementation of {@link NotificationProcessor} that uses the id indicated by
 * the {@link TimestampedNotification} to obtain results from the
 * {@link PeriodicQueryResultStorage} layer containing the results of the
 * Periodic Query. The TimestampedNotificationProcessor then parses the results
 * and adds them to work queues to be processed by the {@link BinPruner} and the
 * {@link KafkaPeriodicBindingSetExporter}.
 *
 */
public class TimestampedNotificationProcessor implements NotificationProcessor, Runnable {

    private static final Logger log = Logger.getLogger(TimestampedNotificationProcessor.class);
    private PeriodicQueryResultStorage periodicStorage;
    private BlockingQueue<TimestampedNotification> notifications; // notifications
                                                                  // to process
    private BlockingQueue<NodeBin> bins; // entries to delete from Fluo
    private BlockingQueue<BindingSetRecord> bindingSets; // query results to export
    private AtomicBoolean closed = new AtomicBoolean(false);
    private int threadNumber;
    

    public TimestampedNotificationProcessor(PeriodicQueryResultStorage periodicStorage,
            BlockingQueue<TimestampedNotification> notifications, BlockingQueue<NodeBin> bins, BlockingQueue<BindingSetRecord> bindingSets,
            int threadNumber) {
        this.notifications = Preconditions.checkNotNull(notifications);
        this.bins = Preconditions.checkNotNull(bins);
        this.bindingSets = Preconditions.checkNotNull(bindingSets);
        this.periodicStorage = periodicStorage;
        this.threadNumber = threadNumber;
    }

    /**
     * Processes the TimestampNotifications by scanning the PCJ tables for
     * entries in the bin corresponding to
     * {@link TimestampedNotification#getTimestamp()} and adding them to the
     * export BlockingQueue. The TimestampNotification is then used to form a
     * {@link NodeBin} that is passed to the BinPruner BlockingQueue so that the
     * bins can be deleted from Fluo and Accumulo.
     */
    @Override
    public void processNotification(TimestampedNotification notification) {

        String id = notification.getId();
        long ts = notification.getTimestamp().getTime();
        long period = notification.getPeriod();
        long bin = getBinFromTimestamp(ts, period);
        NodeBin nodeBin = new NodeBin(id, bin);

        try (CloseableIterator<BindingSet> iter = periodicStorage.listResults(id, Optional.of(bin));) {

            while(iter.hasNext()) {
                bindingSets.add(new BindingSetRecord(iter.next(), id));
            }
            // add NodeBin to BinPruner queue so that bin can be deleted from
            // Fluo and Accumulo
            bins.add(nodeBin);
        } catch (Exception e) {
            log.debug("Encountered error: " + e.getMessage() + " while accessing periodic results for bin: " + bin + " for query: " + id);
        }
    }

    /**
     * Computes left bin end point containing event time ts
     * 
     * @param ts - event time
     * @param start - time that periodic event began
     * @param period - length of period
     * @return left bin end point containing event time ts
     */
    private long getBinFromTimestamp(long ts, long period) {
        Preconditions.checkArgument(period > 0);
        return (ts / period) * period;
    }

    @Override
    public void run() {
        try {
            while(!closed.get()) {
                processNotification(notifications.take());
            }
        } catch (Exception e) {
            log.trace("Thread_" + threadNumber + " is unable to process next notification.");
            throw new RuntimeException(e);
        }

    }
    
    public void shutdown() {
        closed.set(true);
    }

    public static Builder builder() {
        return new Builder();
    }

  

    public static class Builder {

        private PeriodicQueryResultStorage periodicStorage;
        private BlockingQueue<TimestampedNotification> notifications; // notifications to process
        private BlockingQueue<NodeBin> bins; // entries to delete from Fluo
        private BlockingQueue<BindingSetRecord> bindingSets; // query results to export
                                                       
        private int threadNumber;

        /**
         * Set notification queue
         * @param notifications - work queue containing notifications to be processed
         * @return this Builder for chaining method calls
         */
        public Builder setNotifications(BlockingQueue<TimestampedNotification> notifications) {
            this.notifications = notifications;
            return this;
        }

        /**
         * Set nodeBin queue
         * @param bins - work queue containing NodeBins to be pruned
         * @return this Builder for chaining method calls
         */
        public Builder setBins(BlockingQueue<NodeBin> bins) {
            this.bins = bins;
            return this;
        }

        /**
         * Set BindingSet queue
         * @param bindingSets - work queue containing BindingSets to be exported
         * @return this Builder for chaining method calls
         */
        public Builder setBindingSets(BlockingQueue<BindingSetRecord> bindingSets) {
            this.bindingSets = bindingSets;
            return this;
        }

        /**
         * Sets the number of threads used by this processor
         * @param threadNumber - number of threads used by this processor
         * @return - number of threads used by this processor
         */
        public Builder setThreadNumber(int threadNumber) {
            this.threadNumber = threadNumber;
            return this;
        }
        
        /**
         * Set the PeriodicStorage layer
         * @param periodicStorage - periodic storage layer that periodic results are read from
         * @return - this Builder for chaining method calls
         */
        public Builder setPeriodicStorage(PeriodicQueryResultStorage periodicStorage) {
            this.periodicStorage = periodicStorage;
            return this;
        }

        /**
         * Builds a TimestampedNotificationProcessor
         * @return - TimestampedNotificationProcessor built from arguments passed to this Builder
         */
        public TimestampedNotificationProcessor build() {
            return new TimestampedNotificationProcessor(periodicStorage, notifications, bins, bindingSets, threadNumber);
        }

    }
}
