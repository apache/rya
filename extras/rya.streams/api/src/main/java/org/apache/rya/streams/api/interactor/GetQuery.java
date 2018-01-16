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
package org.apache.rya.streams.api.interactor;

import java.util.Optional;
import java.util.UUID;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.exception.RyaStreamsException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Get a {@link StreamsQuery} from Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public interface GetQuery {

    /**
     * Get a {@link StreamsQuery} from Rya Streams.
     *
     * @param queryId - Identifies the query to fetch. (not null)
     * @return The {@link StreamsQuery} for the {@code queryId}; if one is stored for the ID.
     * @throws RyaStreamsException The query could not be fetched.
     */
    public Optional<StreamsQuery> getQuery(UUID queryId) throws RyaStreamsException;
}