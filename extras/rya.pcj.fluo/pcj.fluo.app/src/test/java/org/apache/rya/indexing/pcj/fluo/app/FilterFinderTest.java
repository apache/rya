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
package org.apache.rya.indexing.pcj.fluo.app;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.algebra.Compare;
import org.openrdf.query.algebra.Compare.CompareOp;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.ValueConstant;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;

import com.google.common.base.Optional;

/**
 * Tests the methods of {@link FilterFinder}.
 */
public class FilterFinderTest {

    @Test
    public void manyFilters() throws Exception {
        // The query that will be searched.
        final String sparql =
                "SELECT ?person ?age " +
                "{" +
                  "FILTER(?age < 30) . " +
                  "FILTER(?person = <http://Alice>)" +
                  "?person <http://hasAge> ?age" +
                "}";

        // Create the expected result.
        final ValueExpr[] expected = new ValueExpr[2];
        expected[0] =  new Compare(new Var("person"), new ValueConstant( new URIImpl("http://Alice") ));
        expected[1] = new Compare(new Var("age"), new ValueConstant( new LiteralImpl("30", XMLSchema.INTEGER) ), CompareOp.LT);

        // Run the test.
        final FilterFinder finder = new FilterFinder();
        final ValueExpr[] conditions = new ValueExpr[2];
        conditions[0] = finder.findFilter(sparql, 0).get().getCondition();
        conditions[1] = finder.findFilter(sparql, 1).get().getCondition();
        assertTrue( Arrays.equals(expected, conditions) );
    }

    @Test
    public void noFilterAtIndex() throws Exception {
        // The query that will be searched.
        final String sparql =
                "SELECT ?person ?age " +
                "{" +
                  "FILTER(?age < 30) . " +
                  "FILTER(?person = <http://Alice>)" +
                  "?person <http://hasAge> ?age" +
                "}";

        // Run the test.
        final FilterFinder finder = new FilterFinder();
        final Optional<Filter> filter = finder.findFilter(sparql, 4);
        assertFalse( filter.isPresent() );
    }
}