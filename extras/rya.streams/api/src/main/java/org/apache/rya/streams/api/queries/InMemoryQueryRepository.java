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

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import info.aduna.iteration.CloseableIteration;

/**
 * An in memory implementation of {@link QueryRepository}. It is lazily
 * initialized the first time one of its functions is invoked and it updates
 * its view of the {@link QueryChangeLog} any time a method is invoked that
 * requires the latest view of the queries.
 * </p>
 * Thread safe.
 */
@DefaultAnnotation(NonNull.class)
public class InMemoryQueryRepository implements QueryRepository {
    private static final Logger LOG = LoggerFactory.getLogger(InMemoryQueryRepository.class);

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * The change log that is the ground truth for describing what the queries look like.
     */
    private final QueryChangeLog changeLog;

    /**
     * Represents the position within the {@link QueryChangeLog} that {@code queriesCache} represents.
     */
    private Optional<Long> cachePosition = Optional.empty();

    /**
     * The most recently cached view of the queries within this repository.
     */
    private final Map<UUID, StreamsQuery> queriesCache = new HashMap<>();

    /**
     * Constructs an instance of {@link InMemoryQueryRepository}.
     *
     * @param changeLog - The change log that this repository will maintain and be based on. (not null)
     */
    public InMemoryQueryRepository(final QueryChangeLog changeLog) {
        this.changeLog = requireNonNull(changeLog);
    }

    @Override
    public StreamsQuery add(final String query, final boolean isActive) throws QueryRepositoryException {
        requireNonNull(query);

        lock.lock();
        try {
            // First record the change to the log.
            final UUID queryId = UUID.randomUUID();
            final QueryChange change = QueryChange.create(queryId, query, isActive);
            changeLog.write(change);

            // Update the cache to represent what is currently in the log.
            updateCache();

            // Return the StreamsQuery that represents the just added query.
            return queriesCache.get(queryId);

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
            // Update the cache to represent what is currently in the log.
            updateCache();

            return Optional.ofNullable( queriesCache.get(queryId) );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void updateIsActive(final UUID queryId, final boolean isActive) throws QueryRepositoryException {
        requireNonNull(queryId);

        lock.lock();
        try {
            // Update the cache to represent what is currently in the log.
            updateCache();

            // Ensure the query is in the log.
            if(!queriesCache.containsKey(queryId)) {
                throw new QueryRepositoryException("No query exists for ID " + queryId + ".");
            }

            // First record the change to the log.
            final QueryChange change = QueryChange.update(queryId, isActive);
            changeLog.write(change);

        } catch (final QueryChangeLogException e) {
            throw new QueryRepositoryException("Could not update the Rya Streams query for with ID: " + queryId, e);
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
            // Update the cache to represent what is currently in the log.
            updateCache();

            // Our internal cache is already up to date, so just return its values.
            return queriesCache.values()
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
     * Updates the {@link #queriesCache} to reflect the latest position within the {@link #changeLog}.
     */
    private void updateCache() {
        requireNonNull(changeLog);

        CloseableIteration<ChangeLogEntry<QueryChange>, QueryChangeLogException> it = null;
        try {
            // Iterate over everything since the last position that was handled within the change log.
            if(cachePosition.isPresent()) {
                it = changeLog.readFromPosition(cachePosition.get() + 1);
            } else {
                it = changeLog.readFromStart();
            }

            // Apply each change to the cache.
            while(it.hasNext()) {
                final ChangeLogEntry<QueryChange> entry = it.next();
                final QueryChange change = entry.getEntry();
                final UUID queryId = change.getQueryId();

                switch(change.getChangeType()) {
                    case CREATE:
                        final StreamsQuery query = new StreamsQuery(
                                queryId,
                                change.getSparql().get(),
                                change.getIsActive().get());
                        queriesCache.put(queryId, query);
                        break;

                    case UPDATE:
                        if(queriesCache.containsKey(queryId)) {
                            final StreamsQuery old = queriesCache.get(queryId);
                            final StreamsQuery updated = new StreamsQuery(
                                    old.getQueryId(),
                                    old.getSparql(),
                                    change.getIsActive().get());
                            queriesCache.put(queryId, updated);
                        }
                        break;

                    case DELETE:
                        queriesCache.remove(queryId);
                        break;
                }

                cachePosition = Optional.of( entry.getPosition() );
            }

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