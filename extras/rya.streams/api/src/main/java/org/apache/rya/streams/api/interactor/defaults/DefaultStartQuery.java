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
package org.apache.rya.streams.api.interactor.defaults;

import static java.util.Objects.requireNonNull;

import java.util.UUID;

import org.apache.rya.streams.api.exception.RyaStreamsException;
import org.apache.rya.streams.api.interactor.StartQuery;
import org.apache.rya.streams.api.queries.QueryRepository;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Start a query that is managed by Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public class DefaultStartQuery implements StartQuery {

    private final QueryRepository repository;

    /**
     * Constructs an instance of {@link DefaultStartQuery}.
     *
     * @param repository - The {@link QueryRepository} that will be interacted with. (not null)
     */
    public DefaultStartQuery(final QueryRepository repository) {
        this.repository = requireNonNull(repository);
    }

    @Override
    public void start(final UUID queryId) throws RyaStreamsException {
        requireNonNull(queryId);
        repository.updateIsActive(queryId, true);
    }
}