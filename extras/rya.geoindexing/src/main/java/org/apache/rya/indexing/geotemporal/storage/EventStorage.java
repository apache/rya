/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.geotemporal.storage;

import java.util.Collection;
import java.util.Optional;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.IndexingExpr;
import org.apache.rya.indexing.geotemporal.GeoTemporalIndexer;
import org.apache.rya.indexing.geotemporal.model.Event;
import org.apache.rya.indexing.mongodb.update.RyaObjectStorage;

public interface EventStorage extends RyaObjectStorage<Event> {
    /**
     * Search for {@link Event}s from the storage by its subject.
     * Will query based on present parameters.
     *
     * @param subject - The subject key to find events.
     * @param geoFilters - The geo filters to find Events.
     * @param temporalFilters - The temporal filters to find Events.
     * @return The {@link Event}, if one exists for the subject.
     * @throws ObjectStorageException A problem occurred while fetching the Entity from the storage.
     */
    public Collection<Event> search(final Optional<RyaURI> subject, Optional<Collection<IndexingExpr>> geoFilters, Optional<Collection<IndexingExpr>> temporalFilters) throws ObjectStorageException;

    /**
     * Indicates a problem while interacting with an {@link EventStorage}.
     */
    public static class EventStorageException extends ObjectStorageException {
        private static final long serialVersionUID = 1L;

        /**
         * Constructs a new exception with the specified detail message.  The
         * cause is not initialized, and may subsequently be initialized by
         * a call to {@link #initCause}.
         *
         * @param   message   the detail message. The detail message is saved for
         *          later retrieval by the {@link #getMessage()} method.
         */
        public EventStorageException(final String message) {
            super(message);
        }

        /**
         * Constructs a new exception with the specified detail message and
         * cause.  <p>Note that the detail message associated with
         * {@code cause} is <i>not</i> automatically incorporated in
         * this exception's detail message.
         *
         * @param  message the detail message (which is saved for later retrieval
         *         by the {@link #getMessage()} method).
         * @param  cause the cause (which is saved for later retrieval by the
         *         {@link #getCause()} method).  (A <tt>null</tt> value is
         *         permitted, and indicates that the cause is nonexistent or
         *         unknown.)
         */
        public EventStorageException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * An {@link Event} could not be created because one already exists for the Subject.
     */
    public static class EventAlreadyExistsException extends EventStorageException {
        private static final long serialVersionUID = 1L;

        public EventAlreadyExistsException(final String message) {
            super(message);
        }

        public EventAlreadyExistsException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * An {@link TypedEvent} could not be updated because the old state does not
     * match the current state.
     */
    public static class StaleUpdateException extends EventStorageException {
        private static final long serialVersionUID = 1L;

        public StaleUpdateException(final String message) {
            super(message);
        }

        public StaleUpdateException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    /**
     *  A {@link EventFilter} is a translation from an {@link IndexingExpr}
     *  to a format the {@link GeoTemporalIndexer} can use to easily determine which
     *  filter function is being used.
     *
     *   @param T - The type of
     */
    interface EventFilter<T> {
        /**
         * Gets the translated query friendly form of the filter.
         */
        public T getQueryObject();
    }

    /**
     * Factory for getting the {@link EventFilter} from an {@link IndexingExpr}.
     */
    interface EventFilterFactory<T> {
        public EventFilter<T> getSearchFunction(final IndexingExpr filter);
    }
}
