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
package org.apache.rya.streams.api.queries;

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

import com.google.common.base.Optional;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Represents a SPARQL query specific change within Rya Streams.
 * </p>
 * Immutable.
 */
@DefaultAnnotation(NonNull.class)
public final class QueryChange implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UUID queryId;
    private final ChangeType changeType;
    private final Optional<String> sparql;
    private final Optional<Boolean> isActive;

    /**
     * Constructs an instance of {@link QueryChange}. Use the {@link #create(UUID, String)} or {@link #delete(UUID)}
     * factory methods instead.
     *
     * @param queryId - Uniquely identifies the query within Rya Streams. (not null)
     * @param changeType - Indicates the type of change this object represents. (not null)
     * @param sparql - If this is a create change, then the SPARQL query that will be evaluated within Rya Streams. (not null)
     * @param isActive - If this is a create or update change, then the active state that defines if the
     *   query will be evaluated by RyaStreams. (not null)
     */
    private QueryChange(
            final UUID queryId,
            final ChangeType changeType,
            final Optional<String> sparql,
            final Optional<Boolean> isActive) {
        this.queryId = requireNonNull(queryId);
        this.changeType = requireNonNull(changeType);
        this.sparql = requireNonNull(sparql);
        this.isActive = requireNonNull(isActive);
    }

    /**
     * @return Uniquely identifies the query within Rya Streams.
     */
    public UUID getQueryId() {
        return queryId;
    }

    /**
     * @return Indicates the type of change this object represents.
     */
    public ChangeType getChangeType() {
        return changeType;
    }

    /**
     * @return If this is a create change, then the SPARQL query that will be evaluated within Rya Streams.
     */
    public Optional<String> getSparql() {
        return sparql;
    }

    /**
     * @return If this is a create or update change, then the active state that defines if the
     *   query will be evaluated by RyaStreams. (not null)
     */
    public Optional<Boolean> getIsActive() {
        return isActive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, changeType, sparql, isActive);
    }

    @Override
    public boolean equals(final Object o) {
        if(o instanceof QueryChange) {
            final QueryChange change = (QueryChange) o;
            return Objects.equals(queryId, change.queryId) &&
                    Objects.equals(changeType, change.changeType) &&
                    Objects.equals(sparql, change.sparql) &&
                    Objects.equals(isActive, change.isActive);
        }
        return false;
    }

    @Override
    public String toString() {
        return "QueryChange: {" +
               "    Query ID: " + queryId + ",\n" +
               "    Change Type: " + changeType + ",\n" +
               "    Is Active: " + isActive + ",\n" +
               "    SPARQL: " + sparql + "\n" +
               "}";
    }

    /**
     * Create a {@link QueryChange} that represents a new SPARQL query that will be managed by Rya Streams.
     *
     * @param queryId - Uniquely identifies the query within the streaming system. (not null)
     * @param sparql - The query that will be evaluated. (not null)
     * @param isActive - The active state that defines if the query will be evaluated by RyaStreams. (not null)
     * @return A {@link QueryChange} built using the provided values.
     */
    public static QueryChange create(final UUID queryId, final String sparql, final boolean isActive) {
        return new QueryChange(queryId, ChangeType.CREATE, Optional.of(sparql), Optional.of(isActive));
    }

    /**
     * Create a {@link QueryChange} that represents a query in Rya Streams whose active state has changed.
     *
     * @param queryId - Uniquely identifies the query within the streaming system. (not null)
     * @param isActive - The active state that defines if the query will be evaluated by RyaStreams. (not null)
     * @return A {@link QueryChange} built using the provided values.
     */
    public static QueryChange update(final UUID queryId, final boolean isActive) {
        return new QueryChange(queryId, ChangeType.UPDATE, Optional.absent(), Optional.of(isActive));
    }

    /**
     * Create a {@link QueryChange} that represents a SPARQL query that will not longer be managed by Rya Streams.
     *
     * @param queryId - Identifies which query that will not longer be processed. (not null)
     * @return A {@link QueryChange} built using the provided values.
     */
    public static QueryChange delete(final UUID queryId) {
        return new QueryChange(queryId, ChangeType.DELETE, Optional.absent(), Optional.absent());
    }

    /**
     * Indicates what type of change is being indicated by a {@link QueryChange} object.
     */
    public static enum ChangeType {
        /**
         * The {@link QueryChange} indicates a SPARQL query needs to be processed by Rya Streams.
         */
        CREATE,

        /**
         * The {@link QueryChange} indicates something about a registered query changed.
         */
        UPDATE,

        /**
         * The {@link QueryChange} indicates a SPARQL query no longer needs to be processed by Rya Streams.
         */
        DELETE;
    }
}