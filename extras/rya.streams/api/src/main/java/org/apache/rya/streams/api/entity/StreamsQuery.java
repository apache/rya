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
package org.apache.rya.streams.api.entity;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.UUID;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A SPARQL query that is being processed within Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public class StreamsQuery {

    private final UUID queryId;
    private final String sparql;
    private final boolean isActive;
    private final boolean isInsert;

    /**
     * Constructs an instance of {@link StreamsQuery}.
     *
     * @param queryId - Uniquely identifies the query within Rya Streams. (not null)
     * @param sparql - The SPARQL query that defines how statements will be processed. (not null)
     * @param isActive - {@code true} if Rya Streams should process this query; otherwise {@code false}.
     * @param isInsert - {@code true} if Rya Streams should insert the results of the query back into
     *   the Rya instance the statements originated from; otherwise {@code false}.
     */
    public StreamsQuery(
            final UUID queryId,
            final String sparql,
            final boolean isActive,
            final boolean isInsert) {
        this.queryId = requireNonNull(queryId);
        this.sparql = requireNonNull(sparql);
        this.isActive = isActive;
        this.isInsert = isInsert;
    }

    /**
     * @return Uniquely identifies the query within Rya Streams.
     */
    public UUID getQueryId() {
        return queryId;
    }

    /**
     * @return The SPARQL query that defines how statements will be processed.
     */
    public String getSparql() {
        return sparql;
    }

    /**
     * @return {@code true} if Rya Streams should process this query; otherwise {@code false}.
     */
    public boolean isActive() {
        return isActive;
    }

    /**
     * @return {@code true} if Rya Streams should insert the results of the query back into
     *   the Rya instance the statements originated from; otherwise {@code false}.
     */
    public boolean isInsert() {
        return isInsert;
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, sparql, isActive, isInsert);
    }

    @Override
    public boolean equals(final Object o) {
        if(o instanceof StreamsQuery) {
            final StreamsQuery other = (StreamsQuery) o;
            return Objects.equals(queryId, other.queryId) &&
                    Objects.equals(sparql, other.sparql) &&
                    isActive == other.isActive &&
                    isInsert == other.isInsert;
        }
        return false;
    }

    @Override
    public String toString() {
        return "ID: " + queryId + "\n" +
                "Query: " + sparql + "\n" +
                "Is Active: " + isActive + "\n" +
                "Is Insert: " + isInsert + "\n";
    }
}
