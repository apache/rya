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

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.QueryChangeLog.QueryChangeLogException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import info.aduna.iteration.CloseableIteration;

/**
 * An in memory implementation of {@link QueryRepository}. It is lazily initialized the first time one of its
 * functions is invoked.
 * </p>
 * Thread safe.
 */
@DefaultAnnotation(NonNull.class)
public class InMemoryQueryRepository implements QueryRepository {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryQueryRepository.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final Supplier<Map<UUID, StreamsQuery>> queriesCache;

    private final QueryChangeLog changeLog;

    /**
     * Constructs an instance of {@link InMemoryQueryRepository}.
     *
     * @param changeLog - The change log that this repository will maintain and be based on. (not null)
     */
    public InMemoryQueryRepository(final QueryChangeLog changeLog) {
        this.changeLog = requireNonNull(changeLog);

        // Lazily initialize the queries cache the first time you try to use it.
        queriesCache = Suppliers.memoize(() -> initializeCache(changeLog));
    }

    @Override
    public StreamsQuery add(final String query) throws QueryRepositoryException {
        requireNonNull(query);

        lock.lock();
        try {
            // First record the change to the log.
            final UUID queryId = UUID.randomUUID();
            final QueryChange change = QueryChange.create(queryId, query);
            changeLog.write(change);

            // Then update the view of the change log within the repository.
            final StreamsQuery streamsQuery = new StreamsQuery(queryId, query);
            queriesCache.get().put(queryId, streamsQuery);

            // Return the SreamsQuery that represents the just added query.
            return streamsQuery;

        } catch (final QueryChangeLogException e) {
            throw new QueryRepositoryException("Could not create a Rya Streams query for the SPARQL string: " + query, e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<StreamsQuery> get(final UUID queryId) throws QueryRepositoryException {
        requireNonNull(queryId);

        lock.lock();
        try {
            return Optional.ofNullable( queriesCache.get().get(queryId) );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(final UUID queryId) throws QueryRepositoryException {
        requireNonNull(queryId);

        lock.lock();
        try {
            // First record the change to the log.
            final QueryChange change = QueryChange.delete(queryId);
            changeLog.write(change);

            // Then update the view of the change log within the repository.
            queriesCache.get().remove(queryId);

        } catch (final QueryChangeLogException e) {
            throw new QueryRepositoryException("Could not delete a Rya Streams query for the Query ID: " + queryId, e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Set<StreamsQuery> list() throws QueryRepositoryException {
        lock.lock();
        try {
            // Our internal cache is already up to date, so just return it's values.
            return queriesCache.get().values()
                        .stream()
                        .collect(Collectors.toSet());

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        lock.lock();
        try {
            changeLog.close();
        } finally {
            lock.unlock();
        }
    }

    /**
     * A {@link Map} from query id to the {@link StreamsQuery} that is represented by that id based on what
     * is already in a {@link QueryChangeLog}.
     *
     * @param changeLog - The change log the cache will represent. (not null)
     * @return The most recent view of the change log.
     */
    private static Map<UUID, StreamsQuery> initializeCache(final QueryChangeLog changeLog) {
        requireNonNull(changeLog);

        // The Map that will be initialized and then returned by this supplier.
        final Map<UUID, StreamsQuery> queriesCache = new HashMap<>();

        CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> it = null;
        try {
            // Iterate over everything that is already in the change log.
            it = changeLog.readFromStart();

            // Apply each change to the cache.
            while(it.hasNext()) {
                final ChangeLogEntry<QueryChange> entry = it.next();
                final QueryChange change = entry.getEntry();
                final UUID queryId = change.getQueryId();

                switch(change.getChangeType()) {
                    case CREATE:
                        final StreamsQuery query = new StreamsQuery(queryId, change.getSparql().get());
                        queriesCache.put(queryId, query);
                        break;

                    case DELETE:
                        queriesCache.remove(queryId);
                        break;
                }
            }

            // Return the initialized cache.
            return queriesCache;

        } catch (final QueryChangeLogException e) {
            // Rethrow the exception because the object the supplier tried to create could not be created.
            throw new RuntimeException("Could not initialize the " + InMemoryQueryRepository.class.getName(), e);

        } finally {
            // Try to close the iteration if it was opened.
            try {
                if(it != null) {
                    it.close();
                }
            } catch (final QueryChangeLogException e) {
                LOG.error("Could not close the " + CloseableIteration.class.getName(), e);
            }
        }
    }
}