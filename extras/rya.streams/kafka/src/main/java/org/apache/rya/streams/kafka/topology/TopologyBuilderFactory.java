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
package org.apache.rya.streams.kafka.topology;

import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.rya.api.function.projection.BNodeIdFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Factory for building {@link TopologyBuilder}s from a SPARQL query.
 */
@DefaultAnnotation(NonNull.class)
public interface TopologyBuilderFactory {

    /**
     * Builds a {@link TopologyBuilder} based on the provided SPARQL query that
     * pulls from {@code statementsTopic} for input and writes the query's results
     * to {@code resultsTopic}.
     *
     * @param sparqlQuery - The SPARQL query to build a topology for. (not null)
     * @param statementsTopic - The topic for the source to read from. (not null)
     * @param resultsTopic - The topic for the sink to write to. (not null)
     * @param bNodeIdFactory - A factory that generates Blank Node IDs if any are required. (not null)
     * @return The created {@link TopologyBuilder}.
     * @throws MalformedQueryException - The provided query is not a valid SPARQL query.
     * @throws TopologyBuilderException - A problem occurred while constructing the topology.
     */
    public TopologyBuilder build(
            final String sparqlQuery,
            final String statementsTopic,
            final String resultsTopic,
            final BNodeIdFactory bNodeIdFactory) throws MalformedQueryException, TopologyBuilderException;

    /**
     * An Exception thrown when a problem occurs when constructing the processor
     * topology in the {@link TopologyFactory}.
     */
    public static class TopologyBuilderException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Creates a new instance of {@link TopologyBuilderException}.
         * @param message the detailed message.
         * @param cause the {@link Throwable} cause.
         */
        public TopologyBuilderException(final String message, final Throwable cause) {
            super(message, cause);
        }

        /**
         * Creates a new instance of {@link TopologyBuilderException}.
         * @param message the detailed message.
         */
        public TopologyBuilderException(final String message) {
            super(message);
        }
    }
}