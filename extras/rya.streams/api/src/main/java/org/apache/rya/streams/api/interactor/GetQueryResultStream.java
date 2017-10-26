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
package org.apache.rya.streams.api.interactor;

import java.util.UUID;

import org.apache.rya.streams.api.entity.QueryResultStream;
import org.apache.rya.streams.api.exception.RyaStreamsException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Get a {@link QueryResultStream} over the results of a query that is being managed by Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public interface GetQueryResultStream {

    /**
     * Stream all of the results that have been produced by a query.
     *
     * @param queryId - Indicates which query results to stream. (not null)
     * @return A {@link QueryResultStream} that starts with the first result that was ever produced.
     * @throws RyaStreamsException Could not create the result stream.
     */
    public QueryResultStream fromStart(UUID queryId) throws RyaStreamsException;

    /**
     * Stream results that have been produced by a query after this method was invoked.
     *
     * @param queryId - Indicates which query results to stream. (not null)
     * @return A {@link QueryResultStream} that only returns results that were produced after this method is invoked.
     * @throws RyaStreamsException Could not create the result stream.
     */
    public QueryResultStream fromNow(UUID queryId) throws RyaStreamsException;
}