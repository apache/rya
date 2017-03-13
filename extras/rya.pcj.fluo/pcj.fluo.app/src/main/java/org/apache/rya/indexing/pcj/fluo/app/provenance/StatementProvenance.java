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
package org.apache.rya.indexing.pcj.fluo.app.provenance;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.openrdf.model.Statement;

import com.google.common.collect.ImmutableMap;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Keeps track of a {@link Statement}'s INSERT and DELETE history. We keep track
 * of all of the events because we are not guaranteed to encounter them in any
 * specific order once they have been added to the Fluo application. For example,
 * an application could store a statement and then seconds later delete it.
 * There is no guarantee that the Fluo application will see the INSERT operation
 * before it sees the DELETE operation.
 * </p>
 * These operations are represented as {@link StatementEvent}s. They utilize an
 * Operation Number that is monotonically increasing to order themselves. This
 * shifts the burden of ordering the operation outside of the Fluo application.
 * </p>
 * It is illegal for two events to occur at the same Operation Number. If this
 * happens, a {@link ConflictingEventException} is thrown. This exception is a
 * runtime exception because it isn't recoverable. It indicates a problem with
 * the service that is assigning ordering outside of this application.
 */
@DefaultAnnotation(NonNull.class)
public class StatementProvenance {

    /**
     * The event that has the highest Operation Number.
     */
    private Optional<StatementEvent> mostRecent = Optional.empty();

    /**
     * All events that have been encountered.
     */
    private final Map<Long, StatementEvent> events = new HashMap<>();

    /**
     * Update the provenance information for the Statement with a new {@link StatementEvent}.
     *
     * @param event - An event that describes when when an ADD or DELETE operation
     *   was performed for the {@link Statement}.
     * @throws ConflictingEventException The {@code event} conflicts with another
     *   event that has the same operation number.
     */
    public void update(final StatementEvent event) throws ConflictingEventException {
        requireNonNull(event);

        // Check if this event conflicts with a previously encountered one.
        final Long operationNumber = event.getOperationNumber();
        if(events.containsKey(operationNumber)) {
            final StatementEvent previousEvent = events.get(operationNumber);
            final boolean eventsConflict = ! previousEvent.equals(event);
            if(eventsConflict) {
                throw new ConflictingEventException("'" + event + "' conflicts with '" + previousEvent + "'. " +
                        "There may be a problem with the service that is being used to order events.");
            } else {
                // This event is already stored, so there is no work to do.
                return;
            }
        }

        // Update the provenance information.
        if(!mostRecent.isPresent()) {
            mostRecent = Optional.of(event);
        } else {
            final Long mostRecentOperationNumber = mostRecent.get().getOperationNumber();
            if(operationNumber.compareTo(mostRecentOperationNumber) > 0) {
                mostRecent = Optional.of( event );
            }
        }

        events.put(operationNumber, event);
    }

    /**
     * @return The {@link StatementEvent} with the highest operation number.
     */
    public Optional<StatementEvent> getMostRecentEvent() {
        return mostRecent;
    }

    /**
     * Operations that may be performed on a {@link Statement} that is stored in Rya.
     */
    public static enum Operation {
        /**
         * The {@link Statement} has been inserted into Rya.
         */
        INSERT(0),

        /**
         * The {@link Statement} has been deleted from Rya.
         */
        DELETE(1);

        private int id;

        private Operation(int id) {
            this.id = id;
        }

        /**
         * @return Uniquely identifies the operation.
         */
        public int getId() {
            return id;
        }

        private static final ImmutableMap<Integer, Operation> lookup;
        static {
            ImmutableMap.Builder<Integer, Operation> builder = ImmutableMap.builder();
            for(Operation operation : Operation.values()) {
                builder.put(operation.getId(), operation);
            }
            lookup = builder.build();
        }

        /**
         * Get the {@link Operation} that has a specific ID.
         *
         * @param id - The ID to lookup.
         * @return The {@link Operation} for the id, or {@code null} if none match.
         */
        public static @Nullable Operation getById(int id) {
            return lookup.get(id);
        }
    }

    /**
     * Represents when an {@link Operation} was executed for a specific {@link Statement} within Rya.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class StatementEvent {
        private final Long operationNumber;
        private final Operation operation;

        /**
         * Constructs an instance of {@link StatementEvent}.
         *
         * @param operationNumber - Indicates when this event happened in relation
         *   to the other events for the {@link Statement}.
         * @param operation - The type of operation that happened. (not null)
         */
        public StatementEvent(
                final Long operationNumber,
                final Operation operation) {
        this.operationNumber = operationNumber;
            this.operation = requireNonNull(operation);
        }

        /**
         * @return Indicates when this event happened in relation to the other events for the {@link Statement}.
         */
        public Long getOperationNumber() {
            return operationNumber;
        }

        /**
         * @return The type of operation that happened.
         */
        public Operation getOperation() {
            return operation;
        }

        @Override
        public int hashCode() {
            return Objects.hash(operationNumber, operation);
        }

        @Override
        public boolean equals(final Object other) {
            if(this == other) {
                return true;
            } else if(other instanceof StatementEvent) {
                final StatementEvent event = (StatementEvent) other;
                return Objects.equals(operationNumber, event.operationNumber) &&
                        Objects.equals(operation, event.operation);
            }
            return false;
        }

        @Override
        public String toString() {
            return operationNumber + ": " + operation;
        }
    }

    /**
     * The {@link StatementProvenance} could not be updated because the {@link StatementEvent}
     * conflicts with another event that has the same operation number.
     */
    public static final class ConflictingEventException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public ConflictingEventException(final String message) {
            super(message);
        }
    }
}