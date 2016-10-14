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
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.domain.RyaStatement;

/**
 * Query domain object contains the query to run as a {@link org.apache.rya.api.domain.RyaStatement} and options for running the query
 */
public class BatchRyaQuery extends RyaQueryOptions {

    //queries
    private Iterable<RyaStatement> queries;

    //maximum number of ranges before we use a batchScanner
    private int maxRanges = 2;

    public BatchRyaQuery(Iterable<RyaStatement> queries) {
        Preconditions.checkNotNull(queries, "RyaStatement queries cannot be null");
        this.queries = queries;
    }

    public static RyaBatchQueryBuilder builder(Iterable<RyaStatement> queries) {
        return new RyaBatchQueryBuilder(queries);
    }

    public static class RyaBatchQueryBuilder extends RyaOptionsBuilder<RyaBatchQueryBuilder> {
        private BatchRyaQuery ryaQuery;

        public RyaBatchQueryBuilder(Iterable<RyaStatement> queries) {
            this(new BatchRyaQuery(queries));
        }

        public RyaBatchQueryBuilder(BatchRyaQuery query) {
            super(query);
            this.ryaQuery = query;
        }

        public RyaBatchQueryBuilder setMaxRanges(int maxRanges) {
            ryaQuery.setMaxRanges(maxRanges);
            return this;
        }

        public BatchRyaQuery build() {
            return ryaQuery;
        }
    }

    public Iterable<RyaStatement> getQueries() {
        return queries;
    }

    public void setQueries(Iterable<RyaStatement> queries) {
        this.queries = queries;
    }

    public int getMaxRanges() {
        return maxRanges;
    }

    public void setMaxRanges(int maxRanges) {
        this.maxRanges = maxRanges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        BatchRyaQuery that = (BatchRyaQuery) o;

        if (queries != null ? !queries.equals(that.queries) : that.queries != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (queries != null ? queries.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "BatchRyaQuery{" +
                "queries=" + Iterables.toString(queries) +
                "options={" + super.toString() +
                '}' +
                '}';
    }
}
