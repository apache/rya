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
import java.util.Set;
import java.util.UUID;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.exception.RyaStreamsException;

import com.google.common.util.concurrent.Service;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Repository for adding, deleting, and listing active queries in Rya Streams.
 *
 * This service only needs to be started if it is being subscribed to. An
 * {@link IllegalStateException} will be thrown if the service is subscribed to
 * and used without being started.
 */
@DefaultAnnotation(NonNull.class)
public interface QueryRepository extends Service {

    /**
     * Adds a new query to Rya Streams.
     *
     * @param query - The SPARQL query to add. (not null)
     * @param isActive - {@code true} if the query should be processed after it is added
     *   otherwise {@code false}.
     * @return The {@link StreamsQuery} used in Rya Streams.
     * @throws QueryRepositoryException Could not add the query.
     * @throws IllegalStateException The Service has not been started, but has been subscribed to.
     */
    public StreamsQuery add(final String query, boolean isActive) throws QueryRepositoryException, IllegalStateException;

    /**
     * Updates the isActive state of a {@link StreamsQuery}. Setting this value to {@code true}
     * means Rya Streams will start processing the query. Setting it to {@code false} will stop
     * the processing.
     *
     * @param queryId - Identifies which query will be updated. (not null)
     * @param isActive - The new isActive state for the query.
     * @throws QueryRepositoryException If the query does not exist or something else caused the change to fail.
     * @throws IllegalStateException The Service has not been started, but has been subscribed to.
     */
    public void updateIsActive(UUID queryId, boolean isActive) throws QueryRepositoryException, IllegalStateException;

    /**
     * Get an existing query from Rya Streams.
     *
     * @param queryId - Identifies which query will be fetched.
     * @return the {@link StreamsQuery} for the id if one exists; otherwise empty.
     * @throws QueryRepositoryException The query could not be fetched.
     * @throws IllegalStateException The Service has not been started, but has been subscribed to.
     */
    public Optional<StreamsQuery> get(UUID queryId) throws QueryRepositoryException, IllegalStateException;

    /**
     * Removes an existing query from Rya Streams.
     *
     * @param queryID - The {@link UUID} of the query to remove. (not null)
     * @throws QueryRepositoryException Could not delete the query.
     * @throws IllegalStateException The Service has not been started, but has been subscribed to.
     */
    public void delete(UUID queryID) throws QueryRepositoryException, IllegalStateException;

    /**
     * Lists all existing queries in Rya Streams.
     *
     * @return - A List of the current {@link StreamsQuery}s
     * @throws QueryRepositoryException The {@link StreamsQuery}s could not be listed.
     * @throws IllegalStateException The Service has not been started, but has been subscribed to.
     */
    public Set<StreamsQuery> list() throws QueryRepositoryException, IllegalStateException;

    /**
     * Subscribes a {@link QueryChangeLogListener} to the {@link QueryRepository}.
     *
     * @param listener - The {@link QueryChangeLogListener} to subscribe to this {@link QueryRepository}. (not null)
     * @return The current state of the repository in the form of {@link StreamsQuery}s.
     */
    public Set<StreamsQuery> subscribe(final QueryChangeLogListener listener);

    /**
     * Unsubscribe a {@link QueryChangeLogListener} from the {@link QueryRepository}.
     *
     * @param listener - The {@link QueryChangeLogListener} to unsubscribe from this {@link QueryRepository}. (not null)
     */
    public void unsubscribe(final QueryChangeLogListener listener);

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