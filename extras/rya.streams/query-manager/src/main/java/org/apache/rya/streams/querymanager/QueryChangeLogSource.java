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
package org.apache.rya.streams.querymanager;

import org.apache.rya.streams.api.queries.QueryChangeLog;

import com.google.common.util.concurrent.Service;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link QueryChangeLog} source represents a storage where the change logs reside, but it
 * does not provide a way to create or remove them. Instead, subscribed {@link SourceListener}s
 * are notified whenever a change log is created or removed from the storage.
 */
@DefaultAnnotation(NonNull.class)
public interface QueryChangeLogSource extends Service {

    /**
     * Adds a {@link SourceListener} to be notified when {@link QueryChangeLog}s
     * have been found.
     * <p>
     * When a new subscriber is added, it will be notified with the existing {@link QueryChangeLog}s.
     *
     * @param listener - The {@link SourceListener} to add. (not null)
     */
    public void subscribe(final SourceListener listener);

    /**
     * Removes a {@link SourceListener}.
     *
     * @param listener - The {@link SourceListener} to remove. (not null)
     */
    public void unsubscribe(final SourceListener listener);

    /**
     * A listener that is notified when a {@link QueryChangeLog} has been added or
     * removed from a {@link QueryChangeLogSource}. The listener receives the only
     * copy of the change log and is responsible for shutting it down.
     */
    @DefaultAnnotation(NonNull.class)
    public interface SourceListener {
        /**
         * Notifies the subscriber that there is a new {@link QueryChangeLog} in the
         * {@link QueryChangeLogSource}.
         *
         * @param ryaInstanceName - The Rya Instance name the {@link QueryChangeLog} belongs to. (not null)
         * @param log - The new {@link QueryChangeLog}. (not null)
         */
        public void notifyCreate(final String ryaInstanceName, final QueryChangeLog log);

        /**
         * Notifies the subscriber that a {@link QueryChangeLog} has been deleted
         * from the {@link QueryChangeLogSource}.
         *
         * @param ryaInstanceName - The rya instance whose {@link QueryChangeLog} was removed. (not null)
         */
        public void notifyDelete(final String ryaInstanceName);
    }
}
