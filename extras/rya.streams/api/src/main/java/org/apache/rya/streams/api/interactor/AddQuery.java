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

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.exception.RyaStreamsException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Adds a SPARQL Query to be processed by Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public interface AddQuery {

    /**
     * Adds a query to the Rya Streams system.
     *
     * @param query - The SPARQL query that will be added. (not null)
     * @param isActive - {@code true} if the query needs to be maintained by
     *   Rya Streams; otherwise {@code false}.
     * @param isInsert - {@code true} if the query's reuslts need to be inserted into
     *   the Rya instance that originated the statements; otherwise {@code false}.
     * @return The {@link StreamsQuery} used by Rya Streams for this query.
     * @throws RyaStreamsException The query could not be added to Rya Streams.
     */
    public StreamsQuery addQuery(final String query, boolean isActive, boolean isInsert) throws RyaStreamsException;
}