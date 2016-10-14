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
package org.apache.rya.indexing.external.tupleSet;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.base.Optional;

/**
 * Tests the methods of {@link ParsedQueryUtil}.
 */
public class ParsedQueryUtilTest {

    @Test
    public void findProjection_distinctIsTopNode() throws MalformedQueryException {
        // A SPARQL query that uses the DISTINCT keyword.
        String sparql =
                "SELECT DISTINCT ?a ?b " +
                "WHERE {" +
                    "?a <http://talksTo> ?b" +
                "}";

        // Run the test.
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery query = parser.parseQuery(sparql, null);
        Optional<Projection> projection = new ParsedQueryUtil().findProjection(query);
        assertTrue(projection.isPresent());
    }

    @Test
    public void findProjection_projectionIsTopNode() throws MalformedQueryException {
        // A SPARQL query that will result in the Projection node being the top node.
        String sparql =
                "SELECT ?a ?b " +
                "WHERE {" +
                    "?a <http://talksTo> ?b" +
                "}";

        // Run the test.
        SPARQLParser parser = new SPARQLParser();
        ParsedQuery query = parser.parseQuery(sparql, null);
        Optional<Projection> projection = new ParsedQueryUtil().findProjection(query);
        assertTrue(projection.isPresent());
    }
}