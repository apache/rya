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
package org.apache.rya.indexing.pcj.fluo.api;

import static com.google.common.base.Preconditions.checkNotNull;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import org.apache.rya.indexing.pcj.fluo.app.query.FilterMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQuery;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryMetadataDAO;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.StatementPatternMetadata;

import com.google.common.collect.ImmutableMap;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;

/**
 * Get a reports that indicates how many binding sets have been emitted for
 * the queries that is being managed by the fluo application.
 */
@ParametersAreNonnullByDefault
public class GetQueryReport {

    private final FluoQueryMetadataDAO metadataDao = new FluoQueryMetadataDAO();

    /**
     * Get a report that indicates how many binding sets have been emitted for
     * every query that is being managed by the fluo application.
     *
     * @param fluo - The connection to Fluo that will be used to fetch the metadata. (not null)
     * @return A map from Query ID to QueryReport that holds a report for all of
     *   the queries that are being managed within the fluo app.
     */
    public Map<String, QueryReport> getAllQueryReports(final FluoClient fluo) {
        checkNotNull(fluo);

        // Fetch the queries that are being managed by the Fluo.
        final List<String> queryIds = new ListQueryIds().listQueryIds(fluo);

        final Map<String, QueryReport> reports = new HashMap<>();
        for(final String queryId : queryIds) {
            final QueryReport report = getReport(fluo, queryId);
            reports.put(queryId, report);
        }
        return reports;
    }

    /**
     * Get a report that indicates how many biniding sets have been emitted for
     * a query that is being managed by the fluo application.
     *
     * @param fluo - The connection to Fluo that will be used to fetch the metadata. (not null)
     * @param queryId - The ID of the query to fetch. (not null)
     * @return A report that was built for the query.
     */
    public QueryReport getReport(final FluoClient fluo, final String queryId) {
        checkNotNull(fluo);
        checkNotNull(queryId);

        final QueryReport.Builder reportBuilder = QueryReport.builder();

        try(Snapshot sx = fluo.newSnapshot()) {
            final FluoQuery fluoQuery = metadataDao.readFluoQuery(sx, queryId);
            reportBuilder.setFluoQuery(fluoQuery);

            // Query results.
            BigInteger count = countBindingSets(sx, queryId, FluoQueryColumns.QUERY_BINDING_SET);
            reportBuilder.setCount(queryId, count);

            // Filter results.
            for(final FilterMetadata filter : fluoQuery.getFilterMetadata()) {
                final String filterId = filter.getNodeId();
                count = countBindingSets(sx, filterId, FluoQueryColumns.FILTER_BINDING_SET);
                reportBuilder.setCount(filterId, count);
            }

            // Join results.
            for(final JoinMetadata join : fluoQuery.getJoinMetadata()) {
                final String joinId = join.getNodeId();
                count = countBindingSets(sx, joinId, FluoQueryColumns.JOIN_BINDING_SET);
                reportBuilder.setCount(joinId, count);
            }

            // Statement Pattern results.
            for(final StatementPatternMetadata statementPattern : fluoQuery.getStatementPatternMetadata()) {
                final String patternId = statementPattern.getNodeId();
                count = countBindingSets(sx, patternId, FluoQueryColumns.STATEMENT_PATTERN_BINDING_SET);
                reportBuilder.setCount(patternId, count);
            }
        }

        return reportBuilder.build();
    }

    private BigInteger countBindingSets(final SnapshotBase sx, final String nodeId, final Column bindingSetColumn) {
        checkNotNull(sx);
        checkNotNull(nodeId);
        checkNotNull(bindingSetColumn);

        // Limit the scan to the binding set column and node id.
        final RowScanner rows = sx.scanner().over(Span.prefix(nodeId)).fetch(bindingSetColumn).byRow().build();

        BigInteger count = BigInteger.valueOf(0L);
        for (ColumnScanner columns : rows) {
        	 count = count.add( BigInteger.ONE );
		}
        
        return count;
    }

    /**
     * Contains all metadata that represents a SPARQL query within the Fluo app
     * as well as the number of Binding Sets that have been emitted for each of
     * the query nodes.
     */
    @Immutable
    @ParametersAreNonnullByDefault
    public static final class QueryReport {

        /**
         * Metadata about the nodes of the query.
         */
        private final FluoQuery fluoQuery;

        /**
         * The number of binding sets that match each of the nodes.
         * <p>
         * The key is the Node ID of a node in {@code fluoQuery}. <br/>
         * The value is the number of Binding Sets that have been emitted for the node.
         */
        private final ImmutableMap<String, BigInteger> counts;

        /**
         * Constructs an instance of {@link QueryReport}. Use the {@link Builder} instead.
         *
         * @param fluoQuery - Metadata about the nodes of the query. (not null)
         * @param counts - A map from Node ID to the number of binding sets that
         *   have been emitted for that Node ID in the fluo app. (not null)
         */
        private QueryReport(
                final FluoQuery fluoQuery,
                final ImmutableMap<String, BigInteger> counts) {
            this.fluoQuery = checkNotNull(fluoQuery);
            this.counts = checkNotNull(counts);
        }

        /**
         * @return Metadata about the nodes of the query.
         */
        public FluoQuery getFluoQuery() {
            return fluoQuery;
        }

        /**
         * Get the number of Binding Sets that have been emitted for a node.
         *
         * @param nodeId - The Node ID of the node that emits binding sets. (not null)
         * @return The number of Binding Sets that have been emitted for the node.
         */
        public BigInteger getCount(final String nodeId) {
            checkNotNull(nodeId);
            return counts.get(nodeId);
        }

        /**
         * @return A map from Node ID to the number of binding sets that
         *   have been emitted for that Node ID in the fluo app.
         */
        public ImmutableMap<String, BigInteger> getCounts() {
            return counts;
        }

        /**
         * @return An empty instance of {@link Builder}.
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * Builds instances of {@link QueryReport}.
         */
        @ParametersAreNonnullByDefault
        public static final class Builder {

            private FluoQuery fluoQuery = null;
            private final ImmutableMap.Builder<String, BigInteger> counts = ImmutableMap.builder();

            /**
             * Set the metadata about the nodes of the query.
             *
             * @param fluoQuery - The metadata about the nodes of the query.
             * @return This builder so that method invocations may be chained.
             */
            public Builder setFluoQuery(@Nullable final FluoQuery fluoQuery) {
                this.fluoQuery = fluoQuery;
                return this;
            }

            /**
             * Set the number of Binding Sets that have been emitted for a node.
             *
             * @param nodeId - The ID of the node.
             * @param count - the number of binding sets that have been emitted.
             * @return This builder so that method invocations may be chained.
             */
            public Builder setCount(@Nullable final String nodeId, @Nullable final BigInteger count) {
                counts.put(nodeId, count);
                return this;
            }

            /**
             * @return An instance of {@link QueryReport} built using this builder's values.
             */
            public QueryReport build() {
                return new QueryReport(fluoQuery, counts.build());
            }
        }
    }
}