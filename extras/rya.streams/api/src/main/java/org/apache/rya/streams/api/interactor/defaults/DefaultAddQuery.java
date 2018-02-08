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
package org.apache.rya.streams.api.interactor.defaults;

import static java.util.Objects.requireNonNull;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.AddQuery;
import org.apache.rya.streams.api.queries.QueryRepository;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.sparql.SPARQLParser;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Adds a query to Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public class DefaultAddQuery implements AddQuery {
    private final QueryRepository repository;

    private final SPARQLParser parser = new SPARQLParser();

    /**
     * Creates a new {@link DefaultAddQuery}.
     *
     * @param repository - The {@link QueryRepository} to add a query to. (not null)
     */
    public DefaultAddQuery(final QueryRepository repository) {
        this.repository = requireNonNull(repository);
    }

    @Override
    public StreamsQuery addQuery(final String query, final boolean isActive, final boolean isInsert) throws RyaStreamsException {
        requireNonNull(query);

        // Make sure the SPARQL is valid.
        try {
            parser.parseQuery(query, null);
        } catch (final MalformedQueryException e) {
            throw new RyaStreamsException("Could not add the query because the SPARQL is invalid.", e);
        }

        // If it is, then store it in the repository.
        return repository.add(query, isActive, isInsert);
    }
}