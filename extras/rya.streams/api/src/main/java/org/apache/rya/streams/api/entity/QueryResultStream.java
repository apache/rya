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
package org.apache.rya.streams.api.entity;

import java.util.Collection;
import java.util.UUID;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.api.exception.RyaStreamsException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An infinite stream of {@link VisibilityBindingSet}s that are the results of a query within Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public interface QueryResultStream extends AutoCloseable {

    /**
     * @return Identifies which query in Rya Streams this result stream is over.
     */
    public UUID getQueryId();

    /**
     * Wait at most {@code timeoutMs} milliseconds for the next collection of results.
     *
     * @param timeoutMs - The number of milliseconds to at most wait for the next collection of results. (not null)
     * @return The next collection of {@link VisibilityBindingSet}s that are the result of the query. Empty if
     *   there where no new results within the timout period.
     * @throws RyaStreamsException Could not fetch the next set of results.
     */
    public Collection<VisibilityBindingSet> poll(long timeoutMs) throws RyaStreamsException;
}