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
package org.apache.rya.periodic.notification.coordinator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rya.periodic.notification.api.Notification;
import org.apache.rya.periodic.notification.api.NotificationCoordinatorExecutor;
import org.apache.rya.periodic.notification.api.NotificationProcessor;
import org.apache.rya.periodic.notification.notification.CommandNotification;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;
import org.apache.rya.periodic.notification.notification.TimestampedNotification;
import org.apache.rya.periodic.notification.notification.CommandNotification.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Implementation of {@link NotificationCoordinatorExecutor} that generates regular notifications
 * as indicated by {@link PeriodicNotification}s that are registered with this Object. When notifications
 * are generated they are placed on a work queue to be processed by the {@link NotificationProcessor}.
 *
 */
public class PeriodicNotificationCoordinatorExecutor implements NotificationCoordinatorExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PeriodicNotificationCoordinatorExecutor.class);
    private int numThreads;
    private ScheduledExecutorService producerThreadPool;
    private Map<String, ScheduledFuture<?>> serviceMap = new HashMap<>();
    private BlockingQueue<TimestampedNotification> notifications;
    private final ReentrantLock lock = new ReentrantLock(true);
    private boolean running = false;

    public PeriodicNotificationCoordinatorExecutor(int numThreads, BlockingQueue<TimestampedNotification> notifications) {
        this.numThreads = numThreads;
        this.notifications = notifications;
    }

    @Override
    public void processNextCommandNotification(CommandNotification notification) {
        lock.lock();
        try {
            processNotification(notification);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void start() {
        if (!running) {
            producerThreadPool = Executors.newScheduledThreadPool(numThreads);
            running = true;
        }
    }

    @Override
    public void stop() {

        if (producerThreadPool != null) {
            producerThreadPool.shutdown();
        }

        running = false;

        try {
            if (!producerThreadPool.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                producerThreadPool.shutdownNow();
            }
        } catch (Exception e) {
            LOG.info("Service Executor Shutdown has been called.  Terminating NotificationRunnable");
        }
    }

    private void processNotification(CommandNotification notification) {
        Command command = notification.getCommand();
        Notification periodic = notification.getNotification();
        switch (command) {
        case ADD:
            addNotification(periodic);
            break;
        case DELETE:
            deleteNotification(periodic);
            break;
        }
    }

    private void addNotification(Notification notification) {
        Preconditions.checkArgument(notification instanceof PeriodicNotification);
        PeriodicNotification notify = (PeriodicNotification) notification;
        if (!serviceMap.containsKey(notification.getId())) {
            ScheduledFuture<?> future = producerThreadPool.scheduleAtFixedRate(new NotificationProducer(notify), notify.getInitialDelay(),
                    notify.getPeriod(), notify.getTimeUnit());
            serviceMap.put(notify.getId(), future);
        }
    }

    private boolean deleteNotification(Notification notification) {
        if (serviceMap.containsKey(notification.getId())) {
            ScheduledFuture<?> future = serviceMap.remove(notification.getId());
            future.cancel(true);
            return true;
        }
        return false;
    }

    /**
     * Scheduled Task that places a {@link PeriodicNotification}
     * in the work queue at regular intervals. 
     *
     */
    class NotificationProducer implements Runnable {

        private PeriodicNotification notification;

        public NotificationProducer(PeriodicNotification notification) {
            this.notification = notification;
        }

        public void run() {
            try {
                notifications.put(new TimestampedNotification(notification));
            } catch (InterruptedException e) {
                LOG.info("Unable to add notification.  Process interrupted. ");
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public boolean currentlyRunning() {
        return running;
    }
}
