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

import static org.junit.Assert.assertEquals;

import java.util.Set;
import java.util.UUID;

import org.apache.rya.streams.api.entity.StreamsQuery;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Unit tests the methods of {@link StreamsQueryFormatter}.
 */
public class StreamsQueryFormatterTest {

    @Test
    public void formatQuery() throws Exception {
        // Format the query.
        final StreamsQuery query = new StreamsQuery(
                UUID.fromString("da55cea5-c21c-46a5-ab79-5433eef4efaa"),
                "SELECT * WHERE { ?a ?b ?c . }",
                true);
        final String formatted = StreamsQueryFormatter.format(query);

        // Ensure it has the expected format.
        final String expected =
                " Query ID: da55cea5-c21c-46a5-ab79-5433eef4efaa\n" +
                "Is Active: true\n" +
                "   SPARQL: select ?a ?b ?c\n" +
                "           where {\n" +
                "             ?a ?b ?c.\n" +
                "           }\n";

        assertEquals(expected, formatted);
    }

    @Test
    public void formatQueries() throws Exception {
        // Format the queries.
        final Set<StreamsQuery> queries = Sets.newHashSet(
                new StreamsQuery(
                        UUID.fromString("33333333-3333-3333-3333-333333333333"),
                        "SELECT * WHERE { ?person <urn:worksAt> ?business . }",
                        true),
                new StreamsQuery(
                        UUID.fromString("11111111-1111-1111-1111-111111111111"),
                        "SELECT * WHERE { ?a ?b ?c . }",
                        true),
                new StreamsQuery(
                        UUID.fromString("22222222-2222-2222-2222-222222222222"),
                        "SELECT * WHERE { ?d ?e ?f . }",
                        false));

        final String formatted = StreamsQueryFormatter.format(queries);

        // Ensure it has the expected format.
        final String expected =
                "-----------------------------------------------\n" +
                " Query ID: 11111111-1111-1111-1111-111111111111\n" +
                "Is Active: true\n" +
                "   SPARQL: select ?a ?b ?c\n" +
                "           where {\n" +
                "             ?a ?b ?c.\n" +
                "           }\n" +
                "-----------------------------------------------\n" +
                " Query ID: 22222222-2222-2222-2222-222222222222\n" +
                "Is Active: false\n" +
                "   SPARQL: select ?d ?e ?f\n" +
                "           where {\n" +
                "             ?d ?e ?f.\n" +
                "           }\n" +
                "-----------------------------------------------\n" +
                " Query ID: 33333333-3333-3333-3333-333333333333\n" +
                "Is Active: true\n" +
                "   SPARQL: select ?person ?business\n" +
                "           where {\n" +
                "             ?person <urn:worksAt> ?business.\n" +
                "           }\n" +
                "-----------------------------------------------\n";
        assertEquals(expected, formatted);
    }
}