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
package org.apache.rya.streams.api.interactor;

import java.util.List;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.apache.rya.streams.api.exception.RyaStreamsException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * List the SPARQL Queries that are managed by Rya Streams.
 */
@DefaultAnnotation(NonNull.class)
public interface ListQueries {

    /**
     * List all of the SPARQL Queries that are managed by Rya Streams.
     *
     * @return All of the queries that are managed.
     * @throws RyaStreamsException The queries could not be listed.
     */
    public List<StreamsQuery> all() throws RyaStreamsException;
}