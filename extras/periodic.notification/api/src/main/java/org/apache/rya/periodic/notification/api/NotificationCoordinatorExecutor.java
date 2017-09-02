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

import java.util.concurrent.ScheduledExecutorService;

import org.apache.rya.periodic.notification.notification.CommandNotification;

/**
 * Object that manages the periodic notifications for the Periodic Query Service.
 * This Object processes new requests for periodic updates by registering them with
 * some sort of service that generates periodic updates (such as a {@link ScheduledExecutorService}).
 *
 */
public interface NotificationCoordinatorExecutor extends LifeCycle {

    /**
     * Registers or deletes a {@link CommandNotification}s with the periodic service to
     * generate notifications at a regular interval indicated by the CommandNotification.
     * @param notification - CommandNotification to be registered or deleted from the periodic update
     * service.
     */
    public void processNextCommandNotification(CommandNotification notification);

}
