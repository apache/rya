/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.streams.api;

import static java.util.Objects.requireNonNull;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.apache.rya.streams.api.interactor.AddQuery;
import org.apache.rya.streams.api.interactor.DeleteQuery;
import org.apache.rya.streams.api.interactor.GetQuery;
import org.apache.rya.streams.api.interactor.GetQueryResultStream;
import org.apache.rya.streams.api.interactor.ListQueries;
import org.apache.rya.streams.api.interactor.StartQuery;
import org.apache.rya.streams.api.interactor.StopQuery;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Provides access to a set of Rya Streams functions.Statement
 */
@DefaultAnnotation(NonNull.class)
public class RyaStreamsClient implements AutoCloseable {

    private final AddQuery addQuery;
    private final GetQuery getQuery;
    private final DeleteQuery deleteQuery;
    private final GetQueryResultStream<VisibilityStatement> getStatementResultStream;
    private final GetQueryResultStream<VisibilityBindingSet> getBindingSetResultStream;
    private final ListQueries listQueries;
    private final StartQuery startQuery;
    private final StopQuery stopQuery;

    /**
     * Constructs an instance of {@link RyaStreamsClient}.
     */
    public RyaStreamsClient(
            final AddQuery addQuery,
            final GetQuery getQuery,
            final DeleteQuery deleteQuery,
            final GetQueryResultStream<VisibilityStatement> getStatementResultStream,
            final GetQueryResultStream<VisibilityBindingSet> getBindingSetResultStream,
            final ListQueries listQueries,
            final StartQuery startQuery,
            final StopQuery stopQuery) {
        this.addQuery = requireNonNull(addQuery);
        this.getQuery = requireNonNull(getQuery);
        this.deleteQuery = requireNonNull(deleteQuery);
        this.getStatementResultStream = requireNonNull(getStatementResultStream);
        this.getBindingSetResultStream = requireNonNull(getBindingSetResultStream);
        this.listQueries = requireNonNull(listQueries);
        this.startQuery = requireNonNull(startQuery);
        this.stopQuery = requireNonNull(stopQuery);
    }

    /**
     * @return The connected {@link AddQuery} interactor.
     */
    public AddQuery getAddQuery() {
        return addQuery;
    }

    /**
     * @return The connected {@link GetQuery} interactor.
     */
    public GetQuery getGetQuery() {
        return getQuery;
    }

    /**
     * @return The connected {@link DeleteQuery} interactor.
     */
    public DeleteQuery getDeleteQuery() {
        return deleteQuery;
    }

    /**
     * @return The connected {@link GetQueryResultStream} interactor for a query that produces
     *   {@link VisibilityStatement}s.
     */
    public GetQueryResultStream<VisibilityStatement> getGetStatementResultStream() {
        return getStatementResultStream;
    }

    /**
     * @return The connected {@link GetQueryResultStream} interactor for a query that produces
     *   {@link VisibilityBindingSet}s.
     */
    public GetQueryResultStream<VisibilityBindingSet>  getGetBindingSetResultStream() {
        return getBindingSetResultStream;
    }

    /**
     * @return The connected {@link ListQueries} interactor.
     */
    public ListQueries getListQueries() {
        return listQueries;
    }

    /**
     * @return The connected {@link StartQuery} interactor.
     */
    public StartQuery getStartQuery() {
        return startQuery;
    }

    /**
     * @return The connected {@link StopQuery} interactor.
     */
    public StopQuery getStopQuery() {
        return stopQuery;
    }

    /**
     * By defualt, this client doesn't close anything. If an implementation of the client
     * requires closing components, then override this method.
     */
    @Override
    public void close() throws Exception { }
}