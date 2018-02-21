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
package org.apache.rya.streams.api.queries;

import java.util.Optional;

import org.apache.rya.streams.api.entity.StreamsQuery;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Listener to be notified when {@link QueryChange}s occur on a {@link QueryChangeLog}.
 */
@DefaultAnnotation(NonNull.class)
public interface QueryChangeLogListener {
    /**
     * Notifies the listener that a query change event has occurred in the change log.
     * <p>
     * <b>Note:</b>
     * <p>
     * The QueryRepository blocks when notifying this listener.  Long lasting operations
     * should not be performed within this function.  Doing so will block all operations
     * on the repository.
     *
     * @param queryChangeEvent - The event that occurred. (not null)
     * @param newQueryState - The new state of the query after the query change event, this will be
     *     absent if the change type is DELETE. (not null)
     */
    public void notify(final ChangeLogEntry<QueryChange> queryChangeEvent, final Optional<StreamsQuery> newQueryState);
}
