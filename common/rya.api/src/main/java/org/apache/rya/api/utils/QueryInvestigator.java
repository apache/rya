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
package org.apache.rya.api.utils;

import static java.util.Objects.requireNonNull;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.parser.ParsedGraphQuery;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.query.parser.sparql.SPARQLParser;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A utility class that is used to glean insight into the structure of SPARQL queries.
 */
@DefaultAnnotation(NonNull.class)
public class QueryInvestigator {

    private static final SPARQLParser PARSER = new SPARQLParser();

    private QueryInvestigator() { }

    /**
     * Determines whether a SPARQL command is a CONSTRUCT or not.
     *
     * @param sparql - The SPARQL to evaluate. (not null)
     * @return {@code true} if the provided SPARQL is a CONSTRUCT query; otherwise {@code false}.
     * @throws MalformedQueryException The SPARQL is neither a well formed query or update.
     */
    public static boolean isConstruct(final String sparql) throws MalformedQueryException {
        requireNonNull(sparql);

        try {
            // Constructs are queries, so try to create a ParsedQuery.
            final ParsedQuery parsedQuery = PARSER.parseQuery(sparql, null);

            // Check to see if the SPARQL looks like a CONSTRUCT query.
            return parsedQuery instanceof ParsedGraphQuery;

        } catch(final MalformedQueryException queryE) {
            try {
                // Maybe it's an update.
                PARSER.parseUpdate(sparql, null);

                // It was, so return false.
                return false;

            } catch(final MalformedQueryException updateE) {
                // It's not. Actually malformed.
                throw queryE;
            }
        }
    }

    /**
     * Determines whether a SPARQL command is an INSERT with a WHERE clause or not.
     *
     * @param sparql - The SPARQL to evaluate. (not null)
     * @return {@code true} if the provided SPARQL is an INSERT update; otherwise {@code false}.
     * @throws MalformedQueryException The SPARQL is neither a well formed query or update.
     */
    public static boolean isInsertWhere(final String sparql) throws MalformedQueryException {
        requireNonNull(sparql);

        try {
            // Inserts are updated, so try to create a ParsedUpdate.
            PARSER.parseUpdate(sparql, null);
            final String strippedOperation = QueryParserUtil.removeSPARQLQueryProlog(sparql.toLowerCase());
            return strippedOperation.startsWith("insert");
        } catch(final MalformedQueryException updateE) {
            try {
                // Maybe it's a query.
                PARSER.parseQuery(sparql, null);

                // It was, so return false.
                return false;

            } catch(final MalformedQueryException queryE) {
                // It's not. Actually malformed.
                throw updateE;
            }
        }
    }
}