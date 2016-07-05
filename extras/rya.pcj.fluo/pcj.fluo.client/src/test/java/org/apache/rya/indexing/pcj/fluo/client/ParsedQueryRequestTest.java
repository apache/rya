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

import java.io.IOException;
import java.util.HashSet;

import org.apache.rya.indexing.pcj.fluo.client.util.ParsedQueryRequest;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * Tests the methods of {@link ParsedQueryRequest}.
 */
public class ParsedQueryRequestTest {

    @Test
    public void parseNoVarOrders() throws IOException {
        final String requestText =
                "SELECT * \n"+
                "WHERE { \n" +
                "  ?a <http://talksTo> ?b. \n" +
                "  ?b <http://talksTo> ?c. \n" +
                "}";

        final ParsedQueryRequest expected = new ParsedQueryRequest(
                "SELECT * \n"+
                "WHERE { \n" +
                "  ?a <http://talksTo> ?b. \n" +
                "  ?b <http://talksTo> ?c. \n" +
                "}",
                new HashSet<VariableOrder>());

        final ParsedQueryRequest request = ParsedQueryRequest.parse(requestText);
        assertEquals(expected, request);
    }

    @Test
    public void parseHasVarOrders() throws IOException {
        final String requestText =
                "#prefix a,    b,c\n" +
                "#prefix b, c, a\n" +
                "SELECT * \n"+
                "WHERE { \n" +
                "  ?a <http://talksTo> ?b. \n" +
                "  ?b <http://talksTo> ?c. \n" +
                "}";

        final ParsedQueryRequest expected = new ParsedQueryRequest(
                "SELECT * \n"+
                "WHERE { \n" +
                "  ?a <http://talksTo> ?b. \n" +
                "  ?b <http://talksTo> ?c. \n" +
                "}",
                Sets.newHashSet(
                        new VariableOrder("a", "b", "c"),
                        new VariableOrder("b", "c", "a")));

        final ParsedQueryRequest request = ParsedQueryRequest.parse(requestText);
        assertEquals(expected, request);
    }
}