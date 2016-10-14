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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

/**
 * Tests {@link SimpleExternalTupleSet}.
 */
public class SimpleExternalTupleSetTest {

    @Test
    public void equals_equals() throws MalformedQueryException {
        // The common PCJ expression.
        final String sparql =
                "SELECT ?f ?m ?d { " +
                    "?f <urn:talksTo> ?m . " +
                    "?m <uri:associatesWith> ?d . " +
                "}";

        final ParsedQuery query = new SPARQLParser().parseQuery(sparql, null);
        final Projection pcjExpression = (Projection) query.getTupleExpr();

        // Create two SimpleExternalTupleSet pbjects using the same expression.
        final SimpleExternalTupleSet testSet = new SimpleExternalTupleSet(pcjExpression);
        final SimpleExternalTupleSet identicalTestSet = new SimpleExternalTupleSet(pcjExpression);

        // Show that they are equal.
        assertEquals(testSet, identicalTestSet);
    }

    @Test
    public void equals_notEquals() throws MalformedQueryException {
        // Create the first SimpleExternalTupleSet object.
        final String sparql1 =
                "SELECT ?f ?m ?d { " +
                    "?f <urn:talksTo> ?m . " +
                    "?m <uri:associatesWith> ?d . " +
                "}";

        final ParsedQuery query1 = new SPARQLParser().parseQuery(sparql1, null);
        final Projection pcjExpression1 = (Projection) query1.getTupleExpr();
        final SimpleExternalTupleSet set1 = new SimpleExternalTupleSet(pcjExpression1);

        // Create another one using a different expression.
        final String sparql2 =
                "SELECT ?f ?m ?d { " +
                    "?f <urn:talksTo> ?m . " +
                    "?m <uri:friendsWith> ?d . " +
                "}";

        final ParsedQuery query2 = new SPARQLParser().parseQuery(sparql2, null);
        final Projection pcjExpression2 = (Projection) query2.getTupleExpr();
        final SimpleExternalTupleSet set2 = new SimpleExternalTupleSet(pcjExpression2);

        // Show they are not equal.
        assertNotEquals(set1, set2);
    }

    @Test
    public void hashCode_same() throws MalformedQueryException {
        // The common PCJ expression.
        final String sparql =
                "SELECT ?f ?m ?d { " +
                    "?f <urn:talksTo> ?m . " +
                    "?m <uri:associatesWith> ?d . " +
                "}";

        final ParsedQuery query = new SPARQLParser().parseQuery(sparql, null);
        final Projection pcjExpression = (Projection) query.getTupleExpr();

        // Create two SimpleExternalTupleSet pbjects using the same expression.
        final SimpleExternalTupleSet testSet = new SimpleExternalTupleSet(pcjExpression);
        final SimpleExternalTupleSet identicalTestSet = new SimpleExternalTupleSet(pcjExpression);

        // Show that they are equal.
        assertEquals(testSet.hashCode(), identicalTestSet.hashCode());
    }

    public void hashCode_notSame() throws MalformedQueryException {
        // Create the first SimpleExternalTupleSet object.
        final String sparql1 =
                "SELECT ?f ?m ?d { " +
                    "?f <urn:talksTo> ?m . " +
                    "?m <uri:associatesWith> ?d . " +
                "}";

        final ParsedQuery query1 = new SPARQLParser().parseQuery(sparql1, null);
        final Projection pcjExpression1 = (Projection) query1.getTupleExpr();
        final SimpleExternalTupleSet set1 = new SimpleExternalTupleSet(pcjExpression1);

        // Create another one using a different expression.
        final String sparql2 =
                "SELECT ?f ?m ?d { " +
                    "?f <urn:talksTo> ?m . " +
                    "?m <uri:friendsWith> ?d . " +
                "}";

        final ParsedQuery query2 = new SPARQLParser().parseQuery(sparql2, null);
        final Projection pcjExpression2 = (Projection) query2.getTupleExpr();
        final SimpleExternalTupleSet set2 = new SimpleExternalTupleSet(pcjExpression2);

        // Show they are not equal.
        assertNotEquals(set1.hashCode(), set2.hashCode());
    }

    @Test
    public void getSupportedVariableOrderMap() throws MalformedQueryException {
        // Create the PCJ expression.
        final String sparql =
                "SELECT ?f ?m ?d { " +
                    "?f <urn:talksTo> ?m . " +
                    "?m <uri:associatesWith> ?d . " +
                "}";

        final ParsedQuery query = new SPARQLParser().parseQuery(sparql, null);
        final Projection pcjExpression = (Projection) query.getTupleExpr();

        // Create the object that is being tested.
        final SimpleExternalTupleSet testSet = new SimpleExternalTupleSet(pcjExpression);

        // Verify the correct Supported Variable Order Map is created.
        final Map<String, Set<String>> expected = new HashMap<>();

        String varOrder = "f";
        Set<String> vars = new HashSet<>();
        vars.add("f");
        expected.put(varOrder, vars);

        varOrder = "f;m";
        vars = new HashSet<>();
        vars.add("f");
        vars.add("m");
        expected.put(varOrder, vars);

        varOrder = "f;m;d";
        vars = new HashSet<>();
        vars.add("f");
        vars.add("m");
        vars.add("d");
        expected.put(varOrder, vars);

        assertEquals(expected, testSet.getSupportedVariableOrderMap());
    }
}