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

/**
 * Notification Object used by the Periodic Query Service
 * to inform workers to process results for a given Periodic
 * Query with the indicated id.
 *
 */
public class BasicNotification implements Notification {

    private String id;

    /**
     * Creates a BasicNotification
     * @param id - Fluo query id associated with this Notification
     */
    public BasicNotification(String id) {
        this.id = id;
    }

    /**
     * @return the Fluo Query Id that this notification will generate results for
     */
    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof BasicNotification) {
            BasicNotification not = (BasicNotification) other;
            return Objects.equal(this.id, not.id);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        return builder.append("id").append("=").append(id).toString();
    }

}
