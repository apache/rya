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
package org.apache.rya.shell.util;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.List;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.queryrender.sparql.SPARQLQueryRenderer;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Pretty formats {@link StreamsQuery} objects.
 */
@DefaultAnnotation(NonNull.class)
public final class StreamsQueryFormatter {

    /**
     * Pretty formats a {@link StreamsQuery}.
     *
     * @param query - The query to format. (not null)
     * @return The pretty formatted string.
     * @throws Exception A problem was encountered while pretty formatting the SPARQL.
     */
    public static String format(final StreamsQuery query) throws Exception {
        requireNonNull(query);

        // Pretty format the SPARQL query.
        final ParsedQuery parsedQuery = new SPARQLParser().parseQuery(query.getSparql(), null);
        final String prettySparql = new SPARQLQueryRenderer().render(parsedQuery);
        final String[] lines = prettySparql.split("\n");

        // Create the formatted string.
        query.getQueryId();
        query.isActive();

        String.format(" QueryId: %s", query.getQueryId());

        final StringBuilder builder = new StringBuilder();
        builder.append(" Query ID: ").append( query.getQueryId() ) .append("\n");
        builder.append("Is Active: ").append( query.isActive() ).append("\n");
        builder.append("Is Insert: ").append( query.isInsert() ).append("\n");
        builder.append("   SPARQL: ").append( lines[0] ).append("\n");

        for(int i = 1; i < lines.length; i++) {
            builder.append("           ").append(lines[i]).append("\n");
        }
        return builder.toString();
    }

    /**
     * Pretty formats a collection {@link StreamsQuery}s.
     * They will be sorted based on their Query IDs.
     *
     * @param queries - The queries to format. (not null)
     * @return The pretty formatted string.
     * @throws Exception A problem was encountered while pretty formatting the SPARQL.
     */
    public static String format(final Collection<StreamsQuery> queries) throws Exception {
        requireNonNull(queries);

        if(queries.size() == 1) {
            return format(queries.iterator().next());
        }

        // Sort the queries based on their IDs.
        final List<StreamsQuery> sorted = Lists.newArrayList(queries);
        sorted.sort((query1, query2) -> {
            final String id1 = query1.getQueryId().toString();
            final String id2 = query2.getQueryId().toString();
            return id1.compareTo(id2);
        });

        // Format a list of the queries.
        final StringBuilder builder = new StringBuilder();
        builder.append("-----------------------------------------------\n");
        for(final StreamsQuery query : sorted) {
            builder.append( format(query) );
            builder.append("-----------------------------------------------\n");
        }
        return builder.toString();
    }
}