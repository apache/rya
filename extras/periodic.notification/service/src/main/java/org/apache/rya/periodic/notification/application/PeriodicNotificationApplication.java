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
package org.apache.rya.periodic.notification.application;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.rya.indexing.pcj.fluo.app.util.PeriodicQueryUtil;
import org.apache.rya.periodic.notification.api.BinPruner;
import org.apache.rya.periodic.notification.api.BindingSetRecord;
import org.apache.rya.periodic.notification.api.LifeCycle;
import org.apache.rya.periodic.notification.api.NodeBin;
import org.apache.rya.periodic.notification.api.NotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.exporter.KafkaExporterExecutor;
import org.apache.rya.periodic.notification.processor.NotificationProcessorExecutor;
import org.apache.rya.periodic.notification.pruner.PeriodicQueryPrunerExecutor;
import org.apache.rya.periodic.notification.registration.kafka.KafkaNotificationProvider;
import org.openrdf.query.algebra.evaluation.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * The PeriodicNotificationApplication runs the key components of the Periodic
 * Query Service. It consists of a {@link KafkaNotificationProvider}, a
 * {@link NotificationCoordinatorExecutor}, a
 * {@link NotificationProcessorExecutor}, a {@link KafkaExporterExecutor}, and a
 * {@link PeriodicQueryPrunerExecutor}. These services run in coordination with
 * one another to perform the following tasks in the indicated order: <br>
 * <li>Retrieve new requests to generate periodic notifications from Kafka
 * <li>Register them with the {@link NotificationCoordinatorExecutor} to
 * generate the periodic notifications
 * <li>As notifications are generated, they are added to a work queue that is
 * monitored by the {@link NotificationProcessorExecutor}.
 * <li>The processor processes the notifications by reading all of the query
 * results corresponding to the bin and query id indicated by the notification.
 * <li>After reading the results, the processor adds a {@link BindingSetRecord}
 * to a work queue monitored by the {@link KafkaExporterExecutor}.
 * <li>The processor then adds a {@link NodeBin} to a workqueue monitored by the
 * {@link BinPruner}
 * <li>The exporter processes the BindingSetRecord by exporing the result to
 * Kafka
 * <li>The BinPruner processes the NodeBin by cleaning up the results for the
 * indicated bin and query in Accumulo and Fluo. <br>
 * <br>
 * The purpose of this Periodic Query Service is to facilitate the ability to
 * answer Periodic Queries using the Rya Fluo application, where a Periodic
 * Query is any query requesting periodic updates about events that occurred
 * within a given window of time of this instant. This is also known as a
 * rolling window query. Period Queries can be expressed using SPARQL by
 * including the {@link Function} indicated by the URI
 * {@link PeriodicQueryUtil#PeriodicQueryURI}. The user must provide this
 * Function with the following arguments: the temporal variable in the query
 * that will be filtered on, the window of time that events must occur within,
 * the period at which the user wants to receive updates, and the time unit. The
 * following query requests all observations that occurred within the last
 * minute and requests updates every 15 seconds. It also performs a count on
 * those observations.
 * <p>
 * <pre>
 * PREFIX function: http://org.apache.rya/function#
 * PREFIX time: http://www.w3.org/2006/time#
 * SELECT (count(?obs) as ?total) WHERE {
 *     FILTER (function:periodic(?time, 1, .25, time:minutes))
 *     ?obs uri:hasTime ?time.
 *     ?obs uri:hasId ?id
 * }
 * </pre>
 */
public class PeriodicNotificationApplication implements LifeCycle {

    private static final Logger log = LoggerFactory.getLogger(PeriodicNotificationApplication.class);
    private final NotificationCoordinatorExecutor coordinator;
    private final KafkaNotificationProvider provider;
    private final PeriodicQueryPrunerExecutor pruner;
    private final NotificationProcessorExecutor processor;
    private final KafkaExporterExecutor exporter;
    private boolean running = false;
    private Optional<CompletableFuture<Void>> finished = Optional.empty();


    /**
     * Creates a PeriodicNotificationApplication
     * @param provider - {@link KafkaNotificationProvider} that retrieves new Notification requests from Kafka
     * @param coordinator - {NotificationCoordinator} that manages PeriodicNotifications.
     * @param processor - {@link NotificationProcessorExecutor} that processes PeriodicNotifications
     * @param exporter - {@link KafkaExporterExecutor} that exports periodic results
     * @param pruner - {@link PeriodicQueryPrunerExecutor} that cleans up old periodic bins
     */
    public PeriodicNotificationApplication(final KafkaNotificationProvider provider, final NotificationCoordinatorExecutor coordinator,
            final NotificationProcessorExecutor processor, final KafkaExporterExecutor exporter, final PeriodicQueryPrunerExecutor pruner) {
        this.provider = Preconditions.checkNotNull(provider);
        this.coordinator = Preconditions.checkNotNull(coordinator);
        this.processor = Preconditions.checkNotNull(processor);
        this.exporter = Preconditions.checkNotNull(exporter);
        this.pruner = Preconditions.checkNotNull(pruner);
    }

    @Override
    public void start() {
        if (!running) {
            log.info("Starting PeriodicNotificationApplication.");
            coordinator.start();
            provider.start();
            processor.start();
            pruner.start();
            exporter.start();
            running = true;
            finished = Optional.of(new CompletableFuture<>());
        }
    }

    /**
     * Blocks the current thread until another thread has called the {@link #stop()}.
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IllegalStateException
     */
    public void blockUntilFinished() throws ExecutionException, InterruptedException, IllegalStateException {
        if(finished.isPresent()) {
            finished.get().get();
        } else {
            throw new IllegalStateException("Cannot block if the application has not been started yet");
        }
    }

    @Override
    public void stop() {
        log.info("Stopping PeriodicNotificationApplication.");
        if(!finished.isPresent()) {
            throw new IllegalStateException("Cannot stop if the application has not been started yet");
        }
        provider.stop();
        coordinator.stop();
        processor.stop();
        pruner.stop();
        exporter.stop();
        running = false;
        finished.get().complete(null);
    }

    /**
     * @return boolean indicating whether the application is running
     */
    @Override
    public boolean currentlyRunning() {
        return running;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private PeriodicQueryPrunerExecutor pruner;
        private KafkaNotificationProvider provider;
        private NotificationProcessorExecutor processor;
        private KafkaExporterExecutor exporter;
        private NotificationCoordinatorExecutor coordinator;

        /**
         * Sets the PeriodicQueryPrunerExecutor.
         * @param pruner - PeriodicQueryPrunerExecutor for cleaning up old periodic bins
         * @return this Builder for chaining method calls
         */
        public Builder setPruner(final PeriodicQueryPrunerExecutor pruner) {
            this.pruner = pruner;
            return this;
        }

        /**
         * Sets the KafkaNotificationProvider
         * @param provider - KafkaNotificationProvider for retrieving new periodic notification requests from Kafka
         * @return this Builder for chaining method calls
         */
        public Builder setProvider(final KafkaNotificationProvider provider) {
            this.provider = provider;
            return this;
        }

        public Builder setProcessor(final NotificationProcessorExecutor processor) {
            this.processor = processor;
            return this;
        }

        /**
         * Sets KafkaExporterExecutor
         * @param exporter for exporting periodic query results to Kafka
         * @return this Builder for chaining method calls
         */
        public Builder setExporter(final KafkaExporterExecutor exporter) {
            this.exporter = exporter;
            return this;
        }

        /**
         * Sets NotificationCoordinatorExecutor
         * @param coordinator for managing and generating periodic notifications
         * @return this Builder for chaining method calls
         */
        public Builder setCoordinator(final NotificationCoordinatorExecutor coordinator) {
            this.coordinator = coordinator;
            return this;
        }

        /**
         * Creates a PeriodicNotificationApplication
         * @return PeriodicNotificationApplication for periodically polling Rya Fluo Application
         */
        public PeriodicNotificationApplication build() {
            return new PeriodicNotificationApplication(provider, coordinator, processor, exporter, pruner);
        }

    }

}
