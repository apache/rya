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

import java.util.List;
import java.util.UUID;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.exception.RyaStreamsException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Repository for adding, deleting, and listing active queries in Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public interface QueryRepository {
    /**
     * Adds a new query to Rya Streams.
     *
     * @param query - The SPARQL query to add. (not null)
     * @return The {@link StreamsQuery} used in Rya Streams.
     * @throws QueryRepositoryException Could not add the query.
     */
    public StreamsQuery add(final String query) throws QueryRepositoryException;

    /**
     * Removes an existing query from Rya Streams.
     *
     * @param queryID - The {@link UUID} of the query to remove. (not null)
     * @throws QueryRepositoryException Could not delete the query.
     */
    public void delete(UUID queryID) throws QueryRepositoryException;

    /**
     * Lists all existing queries in Rya Streams.
     *
     * @return - A List of the current {@link StreamsQuery}s
     * @throws QueryRepositoryException The {@link StreamsQuery}s could not be
     *         listed.
     */
    public List<StreamsQuery> list() throws QueryRepositoryException;

    /**
     * A function of {@link QueryRepository} was unable to perform a function.
     */
    public class QueryRepositoryException extends RyaStreamsException {
        private static final long serialVersionUID = 1L;

        public QueryRepositoryException(final String message) {
            super(message);
        }

        public QueryRepositoryException(final String message, final Throwable source) {
            super(message, source);
        }
    }
}
