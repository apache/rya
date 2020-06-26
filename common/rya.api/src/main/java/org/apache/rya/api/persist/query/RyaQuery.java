package org.apache.rya.api.persist.query;

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.rya.api.domain.RyaStatement;

import java.util.Collections;
import java.util.Objects;

/**
 * Query domain object contains the query to run as a {@link RyaStatement} and options for running the query
 */
@Deprecated
public class RyaQuery extends RyaQueryOptions {

    protected Iterable<RyaStatement> queries;

    public RyaQuery(RyaStatement query) {
        Preconditions.checkNotNull(query, "Statement query cannot be null");
        this.queries = Collections.singleton(query);
    }

    protected RyaQuery(Iterable<RyaStatement> queries) {
        Preconditions.checkNotNull(queries, "Statement queries cannot be null");
        Preconditions.checkState(queries.iterator().hasNext(), "Statement queries cannot be empty");
        this.queries = queries;
    }

    public static RyaQueryBuilder builder(RyaStatement query) {
        return new RyaQueryBuilder(query);
    }

    public static class RyaQueryBuilder extends RyaOptionsBuilder<RyaQueryBuilder> {
        private RyaQuery ryaQuery;

        public RyaQueryBuilder(RyaStatement query) {
            this(new RyaQuery(query));
        }

        public RyaQueryBuilder(RyaQuery query) {
            super(query);
            this.ryaQuery = query;
        }

        public RyaQuery build() {
            return ryaQuery;
        }
    }

    public RyaStatement getQuery() {
        return queries.iterator().next();
    }

    public Iterable<RyaStatement> getQueries() {
        return queries;
    }

    public void setQueries(Iterable<RyaStatement> queries) {
        this.queries = queries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        BatchRyaQuery that = (BatchRyaQuery) o;

        return Objects.equals(queries, that.queries);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (queries != null ? queries.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "RyaQuery{" +
                "query=" + Iterables.toString(queries) +
                "options={" + super.toString() +
                '}' +
                '}';
    }
}
