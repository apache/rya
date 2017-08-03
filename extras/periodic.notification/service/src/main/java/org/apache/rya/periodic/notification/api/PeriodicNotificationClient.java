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
package org.apache.rya.periodic.notification.api;

import java.util.concurrent.TimeUnit;

import org.apache.rya.periodic.notification.notification.BasicNotification;
import org.apache.rya.periodic.notification.notification.PeriodicNotification;

/**
 * Object to register {@link PeriodicNotification}s with an external queuing
 * service to be handled by a {@link NotificationCoordinatorExecutor} service.
 * The service will generate notifications to process Periodic Query results at regular
 * intervals corresponding the period of the PeriodicNotification.
 *
 */
public interface PeriodicNotificationClient extends AutoCloseable {

    /**
     * Adds a new notification to be registered with the {@link NotificationCoordinatorExecutor}
     * @param notification - notification to be added
     */
    public void addNotification(PeriodicNotification notification);
    
    /**
     * Deletes a notification from the {@link NotificationCoordinatorExecutor}.
     * @param notification - notification to be deleted
     */
    public void deleteNotification(BasicNotification notification);
    
    /**
     * Deletes a notification from the {@link NotificationCoordinatorExecutor}.
     * @param notification - id corresponding to the notification to be deleted
     */
    public void deleteNotification(String notificationId);
    
    /**
     * Adds a new notification with the indicated id and period to the {@link NotificationCoordinatorExecutor}
     * @param id - Periodic Query id
     * @param period - period indicating frequency at which notifications will be generated
     * @param delay - initial delay for starting periodic notifications
     * @param unit - time unit of delay and period
     */
    public void addNotification(String id, long period, long delay, TimeUnit unit);
    
    public void close();
    
}
