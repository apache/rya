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

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.queries.ChangeLogEntry;
import org.apache.rya.streams.api.queries.InMemoryQueryRepository;
import org.apache.rya.streams.api.queries.QueryChange;
import org.apache.rya.streams.api.queries.QueryChangeLog;
import org.apache.rya.streams.api.queries.QueryChangeLogListener;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.apache.rya.streams.querymanager.QueryChangeLogSource.SourceListener;
import org.apache.rya.streams.querymanager.QueryExecutor.QueryExecutorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractScheduledService.Scheduler;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.UncheckedExecutionException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A service for managing {@link StreamsQuery} running on a Rya Streams system.
 * <p>
 * Only one QueryManager needs to be running to manage any number of rya
 * instances/rya streams instances.
 */
@DefaultAnnotation(NonNull.class)
public class QueryManager extends AbstractService {
    private static final Logger log = LoggerFactory.getLogger(QueryManager.class);

    /**
     * The source of {@link QueryChangeLog}s. Each log discovered is bound to a specific
     * Rya instnace.
     */
    private final QueryChangeLogSource changeLogSource;

    /**
     * The engine that is responsible for executing {@link StreamsQuery}s.
     */
    private final QueryExecutor queryExecutor;

    /**
     * How long blocking operations will be attempted before potentially trying again.
     */
    private final long blockingValue;

    /**
     * The units for {@link #blockingValue}.
     */
    private final TimeUnit blockingUnits;

    /**
     * Used to inform threads that the application is shutting down, so they must stop work.
     */
    private final AtomicBoolean shutdownSignal = new AtomicBoolean(false);

    /**
     * This thread pool manages the two thread used to work the {@link LogEvent}s
     * and the {@link QueryEvent}s.
     */
    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    /**
     * Creates a new {@link QueryManager}.
     *
     * @param queryExecutor - Runs the active {@link StreamsQuery}s. (not null)
     * @param source - The {@link QueryChangeLogSource} of QueryChangeLogs. (not null)
     * @param blockingValue - How long blocking operations will try before looping. (> 0)
     * @param blockingUnits - The units of the {@code blockingValue}. (not null)
     */
    public QueryManager(
            final QueryExecutor queryExecutor,
            final QueryChangeLogSource source,
            final long blockingValue,
            final TimeUnit blockingUnits) {
        this.changeLogSource = requireNonNull(source);
        this.queryExecutor = requireNonNull(queryExecutor);
        Preconditions.checkArgument(blockingValue > 0, "The blocking value must be > 0. Was: " + blockingValue);
        this.blockingValue = blockingValue;
        this.blockingUnits = requireNonNull(blockingUnits);
    }

    @Override
    protected void doStart() {
        log.info("Starting a QueryManager.");

        // A work queue of discovered Query Change Logs that need to be handled.
        // This queue exists so that the source notifying thread may be released
        // immediately instead of calling into blocking functions.
        final BlockingQueue<LogEvent> logEvents = new ArrayBlockingQueue<>(1024);

        // A work queue of discovered Query Changes from the monitored Query Change Logs
        // that need to be handled. This queue exists so that the Query Repository notifying
        // thread may be released immediately instead of calling into blocking functions.
        final BlockingQueue<QueryEvent> queryEvents = new ArrayBlockingQueue<>(1024);

        try {
            // Start up a LogEventWorker using the executor service.
            executor.submit(new LogEventWorker(logEvents, queryEvents, blockingValue, blockingUnits, shutdownSignal));

            // Start up a QueryEvent Worker using the executor service.
            executor.submit(new QueryEventWorker(queryEvents, queryExecutor, blockingValue, blockingUnits, shutdownSignal));

            // Start up the query execution framework.
            queryExecutor.startAndWait();

            // Startup the source that discovers new Query Change Logs.
            changeLogSource.startAndWait();

            // Subscribe the source a listener that writes to the LogEventWorker's work queue.
            changeLogSource.subscribe(new LogEventWorkGenerator(logEvents, blockingValue, blockingUnits, shutdownSignal));
        } catch(final RejectedExecutionException | UncheckedExecutionException e) {
            log.error("Could not start up a QueryManager.", e);
            notifyFailed(e);
        }

        // Notify the service was successfully started.
        notifyStarted();

        log.info("QueryManager has finished starting.");
    }

    @Override
    protected void doStop() {
        log.info("Stopping a QueryManager.");

        // Set the shutdown flag so that all components that rely on that signal will stop processing.
        shutdownSignal.set(true);

        // Stop the workers and wait for them to die.
        executor.shutdownNow();
        try {
            if(!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Waited 10 seconds for the worker threads to die, but they are still running.");
            }
        } catch (final InterruptedException e) {
            log.warn("Waited 10 seconds for the worker threads to die, but they are still running.");
        }

        // Stop the source of new Change Logs.
        try {
            changeLogSource.stopAndWait();
        } catch(final UncheckedExecutionException e) {
            log.warn("Could not stop the Change Log Source.", e);
        }

        // Stop the query execution framework.
        try {
            queryExecutor.stopAndWait();
        } catch(final UncheckedExecutionException e) {
            log.warn("Could not stop the Query Executor", e);
        }

        // Notify the service was successfully stopped.
        notifyStopped();

        log.info("QueryManager has finished stopping.");
    }

    /**
     * Offer a unit of work to a blocking queue until it is either accepted, or the
     * shutdown signal is set.
     *
     * @param workQueue - The blocking work queue to write to. (not null)
     * @param event - The event that will be offered to the work queue. (not null)
     * @param offerValue - How long to wait when offering new work.
     * @param offerUnits - The unit for the {@code offerValue}. (not null)
     * @param shutdownSignal - Used to signal application shutdown has started, so
     *   this method may terminate without ever placing the event on the queue. (not null)
     * @return {@code true} if the evet nwas added to the queue, otherwise false.
     */
    private static <T> boolean offerUntilAcceptedOrShutdown(
            final BlockingQueue<T> workQueue,
            final T event, final
            long offerValue,
            final TimeUnit offerUnits,
            final AtomicBoolean shutdownSignal) {
        requireNonNull(workQueue);
        requireNonNull(event);
        requireNonNull(shutdownSignal);

        boolean submitted = false;
        while(!submitted && !shutdownSignal.get()) {
            try {
                submitted = workQueue.offer(event, offerValue, offerUnits);
                if(!submitted) {
                    log.debug("An event could not be added to a work queue after waiting 5 seconds. Trying again...");
                }
            } catch (final InterruptedException e) {
                log.debug("An event could not be added to a work queue after waiting 5 seconds. Trying again...");
            }
        }
        return submitted;
    }

    /**
     * An observation that a {@link QueryChangeLog} was created within or
     * removed from a {@link QueryChangeLogSource}.
     */
    @DefaultAnnotation(NonNull.class)
    static class LogEvent {

        /**
         * The types of events that may be observed.
         */
        static enum LogEventType {
            /**
             * A {@link QueryChangeLog} was created within a {@link QueryChangeLogSource}.
             */
            CREATE,

            /**
             * A {@link QueryChangeLog} was deleted from a {@link QueryChangeLogSource}.
             */
            DELETE;
        }

        private final String ryaInstance;
        private final LogEventType eventType;
        private final Optional<QueryChangeLog> log;

        /**
         * Constructs an instance of {@link LogEvent}.
         *
         * @param ryaInstance - The Rya Instance the log is/was for. (not null)
         * @param eventType - The type of event that was observed. (not null)
         * @param log - The log if this is a create event. (not null)
         */
        private LogEvent(final String ryaInstance, final LogEventType eventType, final Optional<QueryChangeLog> log) {
            this.ryaInstance = requireNonNull(ryaInstance);
            this.eventType = requireNonNull(eventType);
            this.log = requireNonNull(log);
        }

        /**
         * @return The Rya Instance whose log was either created or deleted.
         */
        public String getRyaInstanceName() {
            return ryaInstance;
        }

        /**
         * @return The type of event that was observed.
         */
        public LogEventType getEventType() {
            return eventType;
        }

        /**
         * @return The {@link QueryChangeLog} if this is a CREATE event.
         */
        public Optional<QueryChangeLog> getQueryChangeLog() {
            return log;
        }

        @Override
        public String toString() {
            return "LogEvent {\n" +
                   "    Rya Instance: " + ryaInstance + ",\n" +
                   "    Event Type: " + eventType + "\n" +
                   "}";
        }

        /**
         * Make a {@link LogEvent} that indicates a {@link QueryChangeLog} was created within a
         * {@link QueryChangeLogSource}.
         *
         * @param ryaInstance - The Rya Instance the created log is for. (not null)
         * @param log - The created {@link QueryChangeLog. (not null)
         * @return A {@link LogEvent} built using the provided values.
         */
        public static LogEvent create(final String ryaInstance, final QueryChangeLog log) {
            return new LogEvent(ryaInstance, LogEventType.CREATE ,Optional.of(log));
        }

        /**
         * Make a {@link LogEvent} that indicates a {@link QueryChangeLog} was deleted from
         * a {@link QueryChangeLogSource}.
         *
         * @param ryaInstance - The Rya Instance whose log was deleted. (not null)
         * @return A {@link LogEvent} built using the provided values.
         */
        public static LogEvent delete(final String ryaInstance) {
            return new LogEvent(ryaInstance, LogEventType.DELETE, Optional.empty());
        }
    }

    /**
     * An observation that a {@link StreamsQuery} needs to be executing or not
     * via the provided {@link QueryExecutor}.
     */
    @DefaultAnnotation(NonNull.class)
    static class QueryEvent {

        /**
         * The type of events that may be observed.
         */
        public static enum QueryEventType {
            /**
             * Indicates a {@link StreamsQuery} needs to be executing.
             */
            EXECUTING,

            /**
             * Indicates a {@link StreamsQuery} needs to be stopped.
             */
            STOPPED,

            /**
             * Indicates all {@link StreamsQuery}s for a Rya instance need to be stopped.
             */
            STOP_ALL;
        }

        private final String ryaInstance;
        private final QueryEventType type;
        private final Optional<UUID> queryId;
        private final Optional<StreamsQuery> query;

        /**
         * Constructs an instance of {@link QueryEvent}.
         *
         * @param ryaInstance - The Rya instance that generated the event. (not null)
         * @param type - Indicates whether the query needs to be executing or not. (not null)
         * @param queryId - If stopped, the ID of the query that must not be running. (not null)
         * @param query - If executing, the StreamsQuery that defines what should be executing. (not null)
         */
        private QueryEvent(
                final String ryaInstance,
                final QueryEventType type,
                final Optional<UUID> queryId,
                final Optional<StreamsQuery> query) {
            this.ryaInstance = requireNonNull(ryaInstance);
            this.type = requireNonNull(type);
            this.queryId = requireNonNull(queryId);
            this.query = requireNonNull(query);
        }

        /**
         * @return The Rya instance that generated the event.
         */
        public String getRyaInstance() {
            return ryaInstance;
        }

        /**
         * @return Indicates whether the query needs to be executing or not.
         */
        public QueryEventType getType() {
            return type;
        }

        /**
         * @return If stopped, the ID of the query that must not be running. Otherwise absent.
         */
        public Optional<UUID> getQueryId() {
            return queryId;
        }

        /**
         * @return If executing, the StreamsQuery that defines what should be executing. Otherwise absent.
         */
        public Optional<StreamsQuery> getStreamsQuery() {
            return query;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ryaInstance, type, queryId, query);
        }

        @Override
        public boolean equals(final Object o) {
            if(o instanceof QueryEvent) {
                final QueryEvent other = (QueryEvent) o;
                return Objects.equals(ryaInstance, other.ryaInstance) &&
                        Objects.equals(type, other.type) &&
                        Objects.equals(queryId, other.queryId) &&
                        Objects.equals(query, other.query);
            }
            return false;
        }

        @Override
        public String toString() {
            final StringBuilder string = new StringBuilder();
            string.append("Query Event {\n")
                  .append("    Rya Instance:").append(ryaInstance).append(",\n")
                  .append("    Type: ").append(type).append(",\n");
            switch(type) {
                case EXECUTING:
                    append(string, query.get());
                    break;
                case STOPPED:
                    string.append("    Query ID: ").append(queryId.get()).append("\n");
                    break;
                case STOP_ALL:
                    break;
                default:
                    // Default to showing everything that is in the object.
                    string.append("    Query ID: ").append(queryId.get()).append("\n");
                    append(string, query.get());
                    break;
            }
            string.append("}");
            return string.toString();
        }

        private void append(final StringBuilder string, final StreamsQuery query) {
            requireNonNull(string);
            requireNonNull(query);
            string.append("    Streams Query {\n")
                  .append("        Query ID: ").append(query.getQueryId()).append(",\n")
                  .append("        Is Active: ").append(query.isActive()).append(",\n")
                  .append("        SPARQL: ").append(query.getSparql()).append("\n")
                  .append("    }");
        }

        /**
         * Create a {@link QueryEvent} that indicates a query needs to be executing.
         *
         * @param ryaInstance - The Rya instance that generated the event. (not null)
         * @param query - The StreamsQuery that defines what should be executing. (not null)
         * @return A {@link QueryEvent} built using the provided values.
         */
        public static QueryEvent executing(final String ryaInstance, final StreamsQuery query) {
            return new QueryEvent(ryaInstance, QueryEventType.EXECUTING, Optional.empty(), Optional.of(query));
        }

        /**
         * Create a {@link QueryEvent} that indicates a query needs to be stopped.
         *
         * @param ryaInstance - The Rya instance that generated the event. (not null)
         * @param queryId - The ID of the query that must not be running. (not null)
         * @return A {@link QueryEvent} built using the provided values.
         */
        public static QueryEvent stopped(final String ryaInstance, final UUID queryId) {
            return new QueryEvent(ryaInstance, QueryEventType.STOPPED, Optional.of(queryId), Optional.empty());
        }

        /**
         * Create a {@link QueryEvent} that indicates all queries for a Rya instance needs to be stopped.
         *
         * @param ryaInstance - The Rya instance that generated the event. (not null)
         * @return A {@link QueryEvent} built using the provided values.
         */
        public static QueryEvent stopALL(final String ryaInstance) {
            return new QueryEvent(ryaInstance, QueryEventType.STOP_ALL, Optional.empty(), Optional.empty());
        }
    }

    /**
     * Listens to a {@link QueryChangeLogSource} and adds observations to the provided
     * work queue. It does so until the provided shutdown signal is set.
     */
    @DefaultAnnotation(NonNull.class)
    static class LogEventWorkGenerator implements SourceListener {

        private final BlockingQueue<LogEvent> workQueue;
        private final AtomicBoolean shutdownSignal;
        private final long offerValue;
        private final TimeUnit offerUnits;

        /**
         * Constructs an instance of {@link QueryManagerSourceListener}.
         *
         * @param workQueue - A blocking queue that will have {@link LogEvent}s offered to it. (not null)
         * @param offerValue - How long to wait when offering new work.
         * @param offerUnits - The unit for the {@code offerValue}. (not null)
         * @param shutdownSignal - Indicates to this listener that it needs to stop adding events
         *   to the work queue because the application is shutting down. (not null)
         */
        public LogEventWorkGenerator(
                final BlockingQueue<LogEvent> workQueue,
                final long offerValue,
                final TimeUnit offerUnits,
                final AtomicBoolean shutdownSignal) {
            this.workQueue = requireNonNull(workQueue);
            this.shutdownSignal = requireNonNull(shutdownSignal);
            this.offerValue = offerValue;
            this.offerUnits = requireNonNull(offerUnits);
        }

        @Override
        public void notifyCreate(final String ryaInstanceName, final QueryChangeLog changeLog) {
            log.info("A new Query Change Log has been discovered for Rya Instance " + ryaInstanceName + ". All " +
                    "queries that are set to active within it will be started.");

            // Create an event that summarizes this notification.
            final LogEvent event = LogEvent.create(ryaInstanceName, changeLog);

            // Offer it to the worker until there is room for it in the work queue, or we are shutting down.
            offerUntilAcceptedOrShutdown(workQueue, event, offerValue, offerUnits, shutdownSignal);
        }

        @Override
        public void notifyDelete(final String ryaInstanceName) {
            log.info("The Query Change Log for Rya Instance " + ryaInstanceName + " has been deleted. All of the " +
                    "queries related to that instance will be stopped.");

            // Create an event that summarizes this notification.
            final LogEvent event = LogEvent.delete(ryaInstanceName);

            // Offer it to the worker until there is room for it in the work queue, or we are shutting down.
            offerUntilAcceptedOrShutdown(workQueue, event, offerValue, offerUnits, shutdownSignal);
        }
    }

    /**
     * Processes a work queue of {@link LogEvent}s.
     * <p/>
     * Whenever a new log has been created, then it registers a {@link QueryEventWorkGenerator}
     * that generates {@link QueryEvent}s based on the content and updates to the discovered
     * {@link QueryChagneLog}.
     * <p/>
     * Whenever a log is deleted, then the generator is stopped and a stop all {@link QueryEvent}
     * is written to the work queue.
     */
    @DefaultAnnotation(NonNull.class)
    static class LogEventWorker implements Runnable {

        /**
         * A map of Rya Instance name to he Query Repository for that instance.
         */
        private final Map<String, QueryRepository> repos = new HashMap<>();

        private final BlockingQueue<LogEvent> logWorkQueue;
        private final BlockingQueue<QueryEvent> queryWorkQueue;
        private final long blockingValue;
        private final TimeUnit blockingUnits;
        private final AtomicBoolean shutdownSignal;

        /**
         * Constructs an instance of {@link LogEventWorker}.
         *
         * @param logWorkQueue - A queue of {@link LogEvent}s that will be worked by this object. (not null)
         * @param queryWorkQueue - A queue where {@link QueryEvent}s will be placed by this object. (not null)
         * @param blockingValue - How long to wait when polling/offering new work.
         * @param blockingUnits - The unit for the {@code blockingValue}. (not null)
         * @param shutdownSignal - Indicates when the application has been shutdown, so the executing thread
         *   may exit the {@link #run()} method. (not null)
         */
        public LogEventWorker(
                final BlockingQueue<LogEvent> logWorkQueue,
                final BlockingQueue<QueryEvent> queryWorkQueue,
                final long blockingValue,
                final TimeUnit blockingUnits,
                final AtomicBoolean shutdownSignal) {
            this.logWorkQueue = requireNonNull(logWorkQueue);
            this.queryWorkQueue = requireNonNull(queryWorkQueue);
            this.blockingValue = blockingValue;
            this.blockingUnits = requireNonNull(blockingUnits);
            this.shutdownSignal = requireNonNull(shutdownSignal);
        }

        @Override
        public void run() {
            // Run until the shutdown signal is set.
            while(!shutdownSignal.get()) {
                try {
                    // Pull a unit of work from the queue.
                    log.debug("LogEventWorker - Polling the work queue for a new LogEvent.");
                    final LogEvent logEvent = logWorkQueue.poll(blockingValue, blockingUnits);
                    if(logEvent == null) {
                        // Poll again if nothing was found.
                        continue;
                    }

                    log.info("LogEventWorker - handling: \n" + logEvent);
                    final String ryaInstance = logEvent.getRyaInstanceName();

                    switch(logEvent.getEventType()) {
                        case CREATE:
                            // If we see a create message for a Rya Instance we are already maintaining,
                            // then don't do anything.
                            if(repos.containsKey(ryaInstance)) {
                                log.warn("LogEventWorker - A repository is already being managed for the Rya Instance " +
                                        ryaInstance + ". This message will be ignored.");
                                continue;
                            }

                            // Create and start a QueryRepository for the discovered log. Hold onto the repository
                            // so that it may be shutdown later.
                            final Scheduler scheduler = Scheduler.newFixedRateSchedule(0, blockingValue, blockingUnits);
                            final QueryRepository repo = new InMemoryQueryRepository(logEvent.getQueryChangeLog().get(), scheduler);
                            repo.startAndWait();
                            repos.put(ryaInstance, repo);

                            // Subscribe a worker that adds the Query Events to the queryWorkQueue queue.
                            // A count down latch is used to ensure the returned set of queries are handled
                            // prior to any notifications from the repository.
                            final CountDownLatch subscriptionWorkFinished = new CountDownLatch(1);
                            final QueryEventWorkGenerator queryWorkGenerator =
                                    new QueryEventWorkGenerator(ryaInstance, subscriptionWorkFinished, queryWorkQueue,
                                            blockingValue, blockingUnits, shutdownSignal);

                            log.debug("LogEventWorker - Setting up a QueryWorkGenerator...");
                            final Set<StreamsQuery> queries = repo.subscribe(queryWorkGenerator);
                            log.debug("LogEventWorker - Finished setting up a QueryWorkGenerator.");

                            // Handle the view of the queries within the repository as it existed when
                            // the subscription was registered.
                            queries.stream()
                            .forEach(query -> {
                                // Create a QueryEvent that represents the active state of the existing query.
                                final QueryEvent queryEvent = query.isActive() ?
                                        QueryEvent.executing(ryaInstance, query) : QueryEvent.stopped(ryaInstance, query.getQueryId());
                                log.debug("LogEventWorker - offering: " + queryEvent);

                                // Offer it to the worker until there is room for it in the work queue, or we are shutting down.
                                offerUntilAcceptedOrShutdown(queryWorkQueue, queryEvent, blockingValue, blockingUnits, shutdownSignal);
                            });

                            // Indicate the subscription work is finished so that the registered listener may start
                            // adding work to the queue.
                            log.info("LogEventWorker - Counting down the subscription work latch.");
                            subscriptionWorkFinished.countDown();
                            break;

                        case DELETE:
                            if(repos.containsKey(ryaInstance)) {
                                // Shut down the query repository for the Rya instance. This ensures the listener will
                                // not receive any more work that needs to be done.
                                final QueryRepository deletedRepo = repos.remove(ryaInstance);
                                deletedRepo.stopAndWait();

                                // Add work that stops all of the queries related to the instance.
                                final QueryEvent stopAllEvent = QueryEvent.stopALL(ryaInstance);
                                offerUntilAcceptedOrShutdown(queryWorkQueue, stopAllEvent, blockingValue, blockingUnits, shutdownSignal);
                            }
                            break;
                    }
                } catch (final InterruptedException e) {
                    log.debug("LogEventWorker did not see any new events over the past 5 seconds. Polling again...");
                }
            }

            log.info("LogEventWorker shutting down...");

            // Shutdown all of the QueryRepositories that were started.
            repos.values().forEach(repo -> repo.stopAndWait());

            log.info("LogEventWorker shut down.");
        }
    }

    /**
     * Listens to a {@link QueryRepository} and adds observations to the provided work queue.
     * It does so until the provided shutdown signal is set.
     */
    @DefaultAnnotation(NonNull.class)
    static class QueryEventWorkGenerator implements QueryChangeLogListener {

        private final String ryaInstance;
        private final CountDownLatch subscriptionWorkFinished;
        private final BlockingQueue<QueryEvent> queryWorkQueue;
        private final long blockingValue;
        private final TimeUnit blockingUnits;
        private final AtomicBoolean shutdownSignal;

        /**
         * Constructs an instance of {@link QueryEventWorkGenerator}.
         *
         * @param ryaInstance - The rya instance whose log this objects is watching. (not null)
         * @param subscriptionWorkFinished - Indicates when work that needs to be completed before this
         *   listener handles notifications is completed. (not null)
         * @param queryWorkQueue - A queue where {@link QueryEvent}s will be placed by this object. (not null)
         * @param blockingValue - How long to wait when polling/offering new work.
         * @param blockingUnits - The unit for the {@code blockingValue}. (not null)
         * @param shutdownSignal - Indicates to this listener that it needs to stop adding events
         *   to the work queue because the application is shutting down. (not null)
         */
        public QueryEventWorkGenerator(
                final String ryaInstance,
                final CountDownLatch subscriptionWorkFinished,
                final BlockingQueue<QueryEvent> queryWorkQueue,
                final long blockingValue,
                final TimeUnit blockingUnits,
                final AtomicBoolean shutdownSignal) {
            this.ryaInstance = requireNonNull(ryaInstance);
            this.subscriptionWorkFinished = requireNonNull(subscriptionWorkFinished);
            this.queryWorkQueue = requireNonNull(queryWorkQueue);
            this.blockingValue = blockingValue;
            this.blockingUnits = requireNonNull(blockingUnits);
            this.shutdownSignal = requireNonNull(shutdownSignal);
        }

        @Override
        public void notify(final ChangeLogEntry<QueryChange> queryChangeEvent, final Optional<StreamsQuery> newQueryState) {
            requireNonNull(queryChangeEvent);
            requireNonNull(newQueryState);

            // Wait for the subscription work to be finished.
            try {
                log.debug("Waiting for Subscription Work Finished latch to release...");
                while(!shutdownSignal.get() && !subscriptionWorkFinished.await(blockingValue, blockingUnits)) {
                    log.debug("Still waiting...");
                }
                log.debug("Subscription Work Finished latch to released.");
            } catch (final InterruptedException e) {
                log.warn("Interrupted while waiting for the Subscription Work Finished latch to be " +
                        "released. Shutting down?", e);
            }

            // If we left the loop because of a shutdown, return immediately.
            if(shutdownSignal.get()) {
                log.debug("Not processing notification. Shutting down.");
                return;
            }

            // Generate work from the notification.
            final QueryChange change = queryChangeEvent.getEntry();
            switch(change.getChangeType()) {
                case CREATE:
                    if(newQueryState.isPresent()) {
                        log.info("Rya Instance " + ryaInstance + " created Rya Streams query " + newQueryState + ".");
                        final StreamsQuery newQuery = newQueryState.get();
                        if(newQuery.isActive()) {
                            final QueryEvent executeNewQuery = QueryEvent.executing(ryaInstance, newQuery);
                            offerUntilAcceptedOrShutdown(queryWorkQueue, executeNewQuery, blockingValue, blockingUnits, shutdownSignal);
                        }
                    } else {
                        log.error("Received a CREATE QueryChange for Rya Instance: " + ryaInstance +
                                ", Query ID: " + change.getQueryId() + ", but the QueryRepository did not supply a " +
                                "StreamsQuery representing the created query. The query will not be processed.");
                    }
                    break;

                case DELETE:
                    final UUID deletedQueryId = change.getQueryId();
                    log.info("Rya Instance " + ryaInstance + " deleted Rya Streams query with ID " + deletedQueryId);
                    final QueryEvent stopDeletedQuery = QueryEvent.stopped(ryaInstance, deletedQueryId);
                    offerUntilAcceptedOrShutdown(queryWorkQueue, stopDeletedQuery, blockingValue, blockingUnits, shutdownSignal);
                    break;

                case UPDATE:
                    if(newQueryState.isPresent()) {
                        final StreamsQuery updatedQuery = newQueryState.get();
                        if(updatedQuery.isActive()) {
                            log.info("Rya Instance " + ryaInstance + " updated Rya Streams query with ID " +
                                    updatedQuery.getQueryId() + " to be active.");
                            final QueryEvent executeUpdatedQuery = QueryEvent.executing(ryaInstance, updatedQuery);
                            offerUntilAcceptedOrShutdown(queryWorkQueue, executeUpdatedQuery, blockingValue, blockingUnits, shutdownSignal);
                        } else {
                            log.info("Rya Instance " + ryaInstance + " updated Rya Streams query with ID " +
                                    updatedQuery.getQueryId() + " to be inactive.");
                            final QueryEvent stopUpdatedQuery = QueryEvent.stopped(ryaInstance, updatedQuery.getQueryId());
                            offerUntilAcceptedOrShutdown(queryWorkQueue, stopUpdatedQuery, blockingValue, blockingUnits, shutdownSignal);
                        }
                    } else {
                        log.error("Received an UPDATE QueryChange for Rya Instance: " + ryaInstance +
                                ", Query ID: " + change.getQueryId() + ", but the QueryRepository did not supply a " +
                                "StreamsQuery representing the created query. The query will not be processed.");
                    }
                    break;
            }
        }
    }

    /**
     * Processes a work queue of {@link QueryEvent}s.
     * <p/>
     * Each type of event maps the to corresponding method on {@link QueryExecutor} that is called into.
     */
    @DefaultAnnotation(NonNull.class)
    static class QueryEventWorker implements Runnable {

        private final BlockingQueue<QueryEvent> workQueue;
        private final QueryExecutor queryExecutor;
        private final long pollingValue;
        private final TimeUnit pollingUnits;
        private final AtomicBoolean shutdownSignal;

        /**
         * Constructs an instance of {@link QueryEventWorker}.
         *
         * @param logWorkQueue - A queue of {@link QueryEvent}s that will be worked by this object. (not null)
         * @param queryExecutor - Responsible for executing the {@link StreamsQuery}s. (not null)
         * @param pollingValue - How long to wait when polling for new work.
         * @param pollingUnits - The units for the {@code pollingValue}. (not null)
         * @param shutdownSignal - Indicates when the application has been shutdown, so the executing thread
         *   may exit the {@link #run()} method. (not null)
         */
        public QueryEventWorker(
                final BlockingQueue<QueryEvent> workQueue,
                final QueryExecutor queryExecutor,
                final long pollingValue,
                final TimeUnit pollingUnits,
                final AtomicBoolean shutdownSignal) {
            this.workQueue = requireNonNull(workQueue);
            this.queryExecutor = requireNonNull(queryExecutor);
            this.pollingValue = pollingValue;
            this.pollingUnits = requireNonNull(pollingUnits);
            this.shutdownSignal = requireNonNull(shutdownSignal);
        }

        @Override
        public void run() {
            log.info("QueryEventWorker starting.");

            // Run until the shutdown signal is set.
            while(!shutdownSignal.get()) {
                // Pull a unit of work from the queue.
                try {
                    log.debug("Polling the work queue for a new QueryEvent.");
                    final QueryEvent event = workQueue.poll(pollingValue, pollingUnits);
                    if(event == null) {
                        // Poll again if nothing was found.
                        continue;
                    }

                    log.info("QueryEventWorker handling:\n" + event);

                    // Ensure the state within the executor matches the query event's state.
                    switch(event.getType()) {
                        case EXECUTING:
                            try {
                                queryExecutor.startQuery(event.getRyaInstance(), event.getStreamsQuery().get());
                            } catch (final IllegalStateException | QueryExecutorException e) {
                                log.error("Could not start a query represented by the following work: " + event, e);
                            }
                            break;

                        case STOPPED:
                            try {
                                queryExecutor.stopQuery(event.getQueryId().get());
                            } catch (final IllegalStateException | QueryExecutorException e) {
                                log.error("Could not stop a query represented by the following work: " + event, e);
                            }
                            break;

                        case STOP_ALL:
                        try {
                            queryExecutor.stopAll(event.getRyaInstance());
                        } catch (final IllegalStateException | QueryExecutorException e) {
                            log.error("Could not stop all queries represented by the following work: " + event, e);
                        }
                        break;
                    }
                } catch (final InterruptedException e) {
                    log.debug("QueryEventWorker interrupted. Probably shutting down.");
                }
            }
            log.info("QueryEventWorker shut down.");
        }
    }
}