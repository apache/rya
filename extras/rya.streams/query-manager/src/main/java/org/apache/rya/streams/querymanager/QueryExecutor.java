/**
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

import java.util.Set;
import java.util.UUID;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.openrdf.model.Statement;

import com.google.common.util.concurrent.Service;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Represents the system that is responsible for ensuring active {@link StreamsQuery}s are
 * being processed.
 */
@DefaultAnnotation(NonNull.class)
public interface QueryExecutor extends Service {

    /**
     * Starts running a {@link StreamsQuery}.
     *
     * @param ryaInstanceName - The rya instance whose {@link Statement}s will be processed by the query. (not null)
     * @param query - The query to run. (not null)
     * @throws QueryExecutorException When the query fails to start.
     * @throws IllegalStateException The service has not been started yet.
     */
    public void startQuery(final String ryaInstanceName, final StreamsQuery query) throws QueryExecutorException, IllegalStateException;

    /**
     * Stops a {@link StreamsQuery}.
     *
     * @param queryID - The ID of the query to stop. (not null)
     * @throws QueryExecutorException When the query fails to stop.
     * @throws IllegalStateException The service has not been started yet.
     */
    public void stopQuery(final UUID queryID) throws QueryExecutorException, IllegalStateException;

    /**
     * Stops all {@link StreamsQuery} belonging to a specific rya instance.
     *
     * @param ryaInstanceName - The name of the rya instance to stop all queries for. (not null)
     * @throws QueryExecutorException When the queries fails to stop.
     * @throws IllegalStateException The service has not been started yet.
     */
    public void stopAll(final String ryaInstanceName) throws QueryExecutorException, IllegalStateException;

    /**
     * @return A set of {@link UUID}s representing the current active queries.
     * @throws QueryExecutorException Can't discover which queries are currently running.
     * @throws IllegalStateException The service has not been started yet.
     */
    public Set<UUID> getRunningQueryIds() throws QueryExecutorException, IllegalStateException;

    /**
     * Exception to be used by {@link QueryExecutor} when queries fail to start or stop.
     */
    public class QueryExecutorException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Creates a new {@link QueryExecutorException}.
         *
         * @param message - The exception message.
         * @param cause - The cause of this exception.
         */
        public QueryExecutorException(final String message, final Throwable cause) {
            super(message, cause);
        }

        /**
         * Creates a new {@link QueryExecutorException}.
         *
         * @param message - The exception message.
         */
        public QueryExecutorException(final String message) {
            super(message);
        }

        /**
         * Creates a new {@link QueryExecutorException}.
         *
         * @param cause - The cause of this exception.
         */
        public QueryExecutorException(final Throwable cause) {
            super(cause);
        }
    }
}