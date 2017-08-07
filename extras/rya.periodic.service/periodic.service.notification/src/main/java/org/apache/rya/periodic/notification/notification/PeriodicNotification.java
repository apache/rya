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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.rya.periodic.notification.api.Notification;

import com.google.common.base.Preconditions;

/**
 * Notification Object used by the Periodic Query Service to inform workers to
 * process results for a given Periodic Query with the indicated id.
 * Additionally, this Object contains a period that indicates a frequency at
 * which regular updates are generated.
 *
 */
public class PeriodicNotification implements Notification {

    private String id;
    private long period;
    private TimeUnit periodTimeUnit;
    private long initialDelay;

    /**
     * Creates a PeriodicNotification.
     * @param id - Fluo Query Id that this notification is associated with
     * @param period - period at which notifications are generated
     * @param periodTimeUnit - time unit associated with the period and delay
     * @param initialDelay - amount of time to wait before generating the first notification
     */
    public PeriodicNotification(String id, long period, TimeUnit periodTimeUnit, long initialDelay) {
        this.id = Preconditions.checkNotNull(id);
        this.periodTimeUnit = Preconditions.checkNotNull(periodTimeUnit);
        Preconditions.checkArgument(period > 0 && initialDelay >= 0);
        this.period = period;
        this.initialDelay = initialDelay;
    }
    

    /**
     * Create a PeriodicNotification
     * @param other - other PeriodicNotification used in copy constructor
     */
    public PeriodicNotification(PeriodicNotification other) {
        this(other.id, other.period, other.periodTimeUnit, other.initialDelay);
    }

    public String getId() {
        return id;
    }

    /**
     * @return - period at which regular notifications are generated
     */
    public long getPeriod() {
        return period;
    }

    /**
     * @return time unit of period and initial delay
     */
    public TimeUnit getTimeUnit() {
        return periodTimeUnit;
    }

    /**
     * @return amount of time to delay before beginning to generate notifications
     */
    public long getInitialDelay() {
        return initialDelay;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        String delim = "=";
        String delim2 = ";";
        return builder.append("id").append(delim).append(id).append(delim2).append("period").append(delim).append(period).append(delim2)
                .append("periodTimeUnit").append(delim).append(periodTimeUnit).append(delim2).append("initialDelay").append(delim)
                .append(initialDelay).toString();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof PeriodicNotification)) {
            return false;
        }

        PeriodicNotification notification = (PeriodicNotification) other;
        return Objects.equals(this.id, notification.id) && (this.period == notification.period) 
                && Objects.equals(this.periodTimeUnit, notification.periodTimeUnit) && (this.initialDelay == notification.initialDelay);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, period, periodTimeUnit, initialDelay);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String id;
        private long period;
        private TimeUnit periodTimeUnit;
        private long initialDelay = 0;

        /**
         * @param id - periodic query id
         * @return - builder to chain method calls
         */
        public Builder id(String id) {
            this.id = id;
            return this;
        }

        /**
         * @param period of the periodic notification for generating regular notifications
         * @return - builder to chain method calls
         */
        public Builder period(long period) {
            this.period = period;
            return this;
        }

        /**
         * @param timeUnit of period and initial delay
          * @return - builder to chain method calls
         */
        public Builder timeUnit(TimeUnit timeUnit) {
            this.periodTimeUnit = timeUnit;
            return this;
        }

        /**
         * @param initialDelay - amount of time to wait before generating notifications
         * @return - builder to chain method calls
         */
        public Builder initialDelay(long initialDelay) {
            this.initialDelay = initialDelay;
            return this;
        }

        /**
         * Builds PeriodicNotification
         * @return PeriodicNotification constructed from Builder specified parameters
         */
        public PeriodicNotification build() {
            return new PeriodicNotification(id, period, periodTimeUnit, initialDelay);
        }

    }

}
