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
package org.apache.rya.periodic.notification.notification;

import org.apache.rya.periodic.notification.api.Notification;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * This Object contains a Notification Object used by the Periodic Query Service
 * to inform workers to process results for a given Periodic Query with the
 * indicated id. Additionally, the CommandNotification contains a
 * {@link Command} about which action the
 * {@link NotificationCoordinatorExecutor} should take (adding or deleting).
 * CommandNotifications are meant to be added to an external work queue (such as
 * Kafka) to be processed by the NotificationCoordinatorExecutor.
 *
 */
public class CommandNotification implements Notification {

    private Notification notification;
    private Command command;

    public enum Command {
        ADD, DELETE
    };

    /**
     * Creates a new CommandNotification
     * @param command - the command associated with this notification (either add, update, or delete)
     * @param notification - the underlying notification associated with this command
     */
    public CommandNotification(Command command, Notification notification) {
        this.notification = Preconditions.checkNotNull(notification);
        this.command = Preconditions.checkNotNull(command);
    }

    @Override
    public String getId() {
        return notification.getId();
    }

    /**
     * Returns {@link Notification} contained by this CommmandNotification.
     * @return - Notification contained by this Object
     */
    public Notification getNotification() {
        return this.notification;
    }

    /**
     * @return Command contained by this Object (either add or delete)
     */
    public Command getCommand() {
        return this.command;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other instanceof CommandNotification) {
            CommandNotification cn = (CommandNotification) other;
            return Objects.equal(this.command, cn.command) && Objects.equal(this.notification, cn.notification);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(command, notification);
    }

    @Override
    public String toString() {
        return new StringBuilder().append("command").append("=").append(command.toString()).append(";")
                .append(notification.toString()).toString();
    }

}
