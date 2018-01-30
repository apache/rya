/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.rya.streams.querymanager;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.ChangeLogEntry;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChange.ChangeType;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryChangeLogListener;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.querymanager.QueryChangeLogSource.SourceListener;
import org.apache.rya.streams.querymanager.QueryExecutor.QueryExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A service for managing {@link StreamsQuery} running on a Rya Streams system.
 * <p>
 * Only one QueryManager needs to be running to manage any number of rya
 * instances/rya streams instances.
 */
@DefaultAnnotation(NonNull.class)
public class QueryManager extends AbstractIdleService {
    private static final Logger LOG = LoggerFactory.getLogger(QueryManager.class);

    private final QueryExecutor queryExecutor;
    private final Scheduler scheduler;

    /**
     * Map of Rya Instance name to {@link QueryRepository}.
     */
    private final Map<String, QueryRepository> queryRepos = new HashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    private final QueryChangeLogSource source;

    /**
     * Creates a new {@link QueryManager}.
     *
     * @param queryExecutor - Runs the active {@link StreamsQuery}s. (not null)
     * @param source - The {@link QueryChangeLogSource} of QueryChangeLogs. (not null)
     * @param scheduler - The {@link Scheduler} used to discover query changes
     *      within the {@link QueryChangeLog}s (not null)
     */
    public QueryManager(final QueryExecutor queryExecutor, final QueryChangeLogSource source, final Scheduler scheduler) {
        this.source = requireNonNull(source);
        this.queryExecutor = requireNonNull(queryExecutor);
        this.scheduler = requireNonNull(scheduler);
    }

    /**
     * Starts running a query.
     *
     * @param ryaInstanceName - The Rya instance the query belongs to. (not null)
     * @param query - The query to run.(not null)
     */
    private void runQuery(final String ryaInstanceName, final StreamsQuery query) {
        requireNonNull(ryaInstanceName);
        requireNonNull(query);
        LOG.info("Starting Query: " + query.toString() + " on the rya instance: " + ryaInstanceName);

        try {
            queryExecutor.startQuery(ryaInstanceName, query);
        } catch (final QueryExecutorException e) {
            LOG.error("Failed to start query.", e);
        }
    }

    /**
     * Stops the specified query from running.
     *
     * @param queryId - The ID of the query to stop running. (not null)
     */
    private void stopQuery(final UUID queryId) {
        requireNonNull(queryId);

        LOG.info("Stopping query: " + queryId.toString());

        try {
            queryExecutor.stopQuery(queryId);
        } catch (final QueryExecutorException e) {
            LOG.error("Failed to stop query.", e);
        }
    }

    @Override
    protected void startUp() throws Exception {
        lock.lock();
        try {
            LOG.info("Starting Query Manager.");
            queryExecutor.startAndWait();
            source.startAndWait();

            // subscribe to the sources to be notified of changes.
            source.subscribe(new QueryManagerSourceListener());
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected void shutDown() throws Exception {
        lock.lock();
        try {
            LOG.info("Stopping Query Manager.");
            source.stopAndWait();
            queryExecutor.stopAndWait();
        } finally {
            lock.unlock();
        }
    }

    /**
     * An implementation of {@link QueryChangeLogListener} for the
     * {@link QueryManager}.
     * <p>
     * When notified of a {@link ChangeType} performs one of the following:
     * <li>{@link ChangeType#CREATE}: Creates a new query using the
     * {@link QueryExecutor} provided to the {@link QueryManager}</li>
     * <li>{@link ChangeType#DELETE}: Deletes a running query by stopping the
     * {@link QueryExecutor} service of the queryID in the event</li>
     * <li>{@link ChangeType#UPDATE}: If the query is running and the update is
     * to stop the query, stops the query. Otherwise, if the query is not
     * running, it is removed.</li>
     */
    private class QueryExecutionForwardingListener implements QueryChangeLogListener {
        private final String ryaInstanceName;

        /**
         * Creates a new {@link QueryExecutionForwardingListener}.
         *
         * @param ryaInstanceName - The rya instance the query change is
         *        performed on. (not null)
         */
        public QueryExecutionForwardingListener(final String ryaInstanceName) {
            this.ryaInstanceName = requireNonNull(ryaInstanceName);
        }

        @Override
        public void notify(final ChangeLogEntry<QueryChange> queryChangeEvent, final Optional<StreamsQuery> newQueryState) {
            LOG.debug("New query change event.");
            final QueryChange entry = queryChangeEvent.getEntry();

            lock.lock();
            try {

                switch (entry.getChangeType()) {
                    case CREATE:
                        if(!newQueryState.isPresent()) {
                            LOG.error("The query with ID: " + entry.getQueryId() + " must be present with the change to be created.");
                            LOG.debug("newQueryState is not allowed to be absent with a CREATE QueryChange, there might be a bug in the QueryRepository.");
                        } else {
                            runQuery(ryaInstanceName, newQueryState.get());
                        }
                        break;
                    case DELETE:
                        stopQuery(entry.getQueryId());
                        break;
                    case UPDATE:
                        if (!newQueryState.isPresent()) {
                            LOG.error("The query with ID: " + entry.getQueryId() + " must be provided with the update, cannot perform update.");
                            LOG.debug("newQueryState is not allowed to be absent with a UPDATE QueryChange, there might be a bug in the QueryRepository.");
                        } else {
                            final StreamsQuery updatedQuery = newQueryState.get();
                            if (updatedQuery.isActive()) {
                                runQuery(ryaInstanceName, updatedQuery);
                                LOG.info("Starting query: " + updatedQuery.toString());
                            } else {
                                stopQuery(updatedQuery.getQueryId());
                                LOG.info("Stopping query: " + updatedQuery.toString());
                            }
                        }
                        break;
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Listener used by the {@link QueryManager} to be notified when
     * {@link QueryChangeLog}s are created or deleted.
     */
    private class QueryManagerSourceListener implements SourceListener {
        @Override
        public void notifyCreate(final String ryaInstanceName, final QueryChangeLog log) {
            lock.lock();
            try {
                LOG.info("Discovered new Query Change Log for Rya Instance " + ryaInstanceName + ".");
                final QueryRepository repo = new InMemoryQueryRepository(log, scheduler);
                repo.startAndWait();
                final Set<StreamsQuery> queries = repo.subscribe(new QueryExecutionForwardingListener(ryaInstanceName));
                queries.forEach(query -> {
                    if (query.isActive()) {
                        try {
                            queryExecutor.startQuery(ryaInstanceName, query);
                        } catch (IllegalStateException | QueryExecutorException e) {
                            LOG.error("Unable to start query for rya instance " + ryaInstanceName, e);
                        }
                    }
                });
                queryRepos.put(ryaInstanceName, repo);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void notifyDelete(final String ryaInstanceName) {
            lock.lock();
            try {
                LOG.info("Notified of deleting QueryChangeLog, stopping all queries belonging to the change log for "
                        + ryaInstanceName + ".");
                queryExecutor.stopAll(ryaInstanceName);
            } catch (final QueryExecutorException e) {
                LOG.error("Failed to stop all queries belonging to: " + ryaInstanceName, e);
            } finally {
                lock.unlock();
            }
        }
    }
}
