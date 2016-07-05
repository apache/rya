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
package org.apache.rya.indexing.pcj.fluo.client;

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.rya.indexing.pcj.fluo.client.util.PcjMetadataRenderer;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Tests the methods of {@link PcjMetadataRenderer}.
 */
public class PcjMetadataRendererTest {

    @Test
    public void formatSingleMetadata() throws Exception {
        // Create the PcjMetadata that will be formatted as a report.
        final PcjMetadata metadata = new PcjMetadata(
                "SELECT ?x ?y " +
                    "WHERE { " +
                    "?x <http://talksTo> <http://Eve>. " +
                    "?y <http://worksAt> <http://Chipotle>." +
                "}",
                12233423L,
                Sets.<VariableOrder>newHashSet(
                    new VariableOrder("x", "y"),
                    new VariableOrder("y", "x")));

        // Run the test.
        final String expected =
                "---------------------------------------------------------------------\n" +
                "| Query ID               | query1                                   |\n" +
                "| Cardinality            | 12,233,423                               |\n" +
                "| Export Variable Orders | y;x                                      |\n" +
                "|                        | x;y                                      |\n" +
                "| SPARQL                 | select ?x ?y                             |\n" +
                "|                        | where {                                  |\n" +
                "|                        |   ?x <http://talksTo> <http://Eve>.      |\n" +
                "|                        |   ?y <http://worksAt> <http://Chipotle>. |\n" +
                "|                        | }                                        |\n" +
                "---------------------------------------------------------------------\n";

        final PcjMetadataRenderer formatter = new PcjMetadataRenderer();
        assertEquals(expected, formatter.render("query1", metadata));
    }

    @Test
    public void formatManyMetdata() throws Exception {
        // Create the PcjMetadata that will be formatted as a report.
        final PcjMetadata metadata1 = new PcjMetadata(
                "SELECT ?x ?y " +
                    "WHERE { " +
                    "?x <http://talksTo> <http://Eve>. " +
                    "?y <http://worksAt> <http://Chipotle>." +
                "}",
                12233423L,
                Sets.<VariableOrder>newHashSet(
                    new VariableOrder("x", "y"),
                    new VariableOrder("y", "x")));

        final PcjMetadata metadata2 = new PcjMetadata(
                "SELECT ?x " +
                    "WHERE { " +
                    "?x <http://likes> <http://cookies>" +
                "}",
                2342L,
                Sets.<VariableOrder>newHashSet(new VariableOrder("x")));

        final Map<String, PcjMetadata> metadata = new LinkedHashMap<>();
        metadata.put("query1", metadata1);
        metadata.put("query2", metadata2);

        // Run the test.
        final String expected =
                "---------------------------------------------------------------------\n" +
                "| Query ID               | query1                                   |\n" +
                "| Cardinality            | 12,233,423                               |\n" +
                "| Export Variable Orders | y;x                                      |\n" +
                "|                        | x;y                                      |\n" +
                "| SPARQL                 | select ?x ?y                             |\n" +
                "|                        | where {                                  |\n" +
                "|                        |   ?x <http://talksTo> <http://Eve>.      |\n" +
                "|                        |   ?y <http://worksAt> <http://Chipotle>. |\n" +
                "|                        | }                                        |\n" +
                "---------------------------------------------------------------------\n" +
                "\n" +
                "------------------------------------------------------------------\n" +
                "| Query ID               | query2                                |\n" +
                "| Cardinality            | 2,342                                 |\n" +
                "| Export Variable Orders | x                                     |\n" +
                "| SPARQL                 | select ?x                             |\n" +
                "|                        | where {                               |\n" +
                "|                        |   ?x <http://likes> <http://cookies>. |\n" +
                "|                        | }                                     |\n" +
                "------------------------------------------------------------------\n" +
                "\n";

        final PcjMetadataRenderer formatter = new PcjMetadataRenderer();
        assertEquals(expected, formatter.render(metadata));
    }
}