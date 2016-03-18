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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.impl.MapBindingSet;

import com.beust.jcommander.internal.Lists;

/**
 * Tests the methods of {@link FluoStringConverterTest}.
 */
public class FluoStringConverterTest {

    @Test
    public void varOrderToString() {
        // Setup the variable order that will be converted.
        final Collection<String> varOrder = Lists.newArrayList("x", "y", "z");

        // Convert it to a String.
        final String varOrderString = FluoStringConverter.toVarOrderString(varOrder);

        // Ensure it converted to the expected result.
        final String expected = "x;y;z";
        assertEquals(expected, varOrderString);
    }

    @Test
    public void stringToVarOrder() {
        // Setup the String that will be converted.
        final String varOrderString = "x;y;z";

        // Convert it to an array in variable order.
        final String[] varOrder = FluoStringConverter.toVarOrder(varOrderString);

        // Ensure it converted to the expected result.
        final String[] expected = {"x", "y", "z"};
        assertTrue( Arrays.equals(expected, varOrder) );
    }

	@Test
	public void bindingSetToString() {
		// Setup the binding set that will be converted.
		final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new URIImpl("http://a"));
        originalBindingSet.addBinding("y", new URIImpl("http://b"));
        originalBindingSet.addBinding("z", new URIImpl("http://c"));

        // Convert it to a String.
        final String[] varOrder = new String[] {"y", "z", "x" };
        final String bindingSetString = FluoStringConverter.toBindingSetString(originalBindingSet, varOrder);

        // Ensure it converted to the expected result.
        final String expected = "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://c<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        assertEquals(expected, bindingSetString);
	}

	@Test
	public void stringToBindingSet() {
	    // Setup the String that will be converted.
	    final String bindingSetString = "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://c<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

	    // Convert it to a BindingSet
	    final String[] varOrder = new String[] {"y", "z", "x" };
        final BindingSet bindingSet = FluoStringConverter.toBindingSet(bindingSetString, varOrder);

        // Ensure it converted to the expected result.
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("x", new URIImpl("http://a"));
        expected.addBinding("y", new URIImpl("http://b"));
        expected.addBinding("z", new URIImpl("http://c"));

        assertEquals(expected, bindingSet);
	}

	@Test
	public void statementPatternToString() throws MalformedQueryException {
        // Setup a StatementPattern that represents "?x <http://worksAt> <http://Chipotle>."
        final Var subject = new Var("x");
        final Var predicate = new Var("-const-http://worksAt", new URIImpl("http://worksAt"));
        predicate.setAnonymous(true);
        predicate.setConstant(true);
        final Var object = new Var("-const-http://Chipotle", new URIImpl("http://Chipotle"));
        object.setAnonymous(true);
        object.setConstant(true);
        final StatementPattern pattern = new StatementPattern(subject, predicate, object);

        // Convert the pattern to a String.
        final String spString = FluoStringConverter.toStatementPatternString(pattern);

        // Ensure it converted to the expected result.
        final String expected = "x:::" +
                "-const-http://worksAt<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "-const-http://Chipotle<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        assertEquals(spString, expected);
	}

    @Test
    public void stringToStatementPattern() {
        // Setup the String representation of a statement pattern.
        final String patternString = "x:::" +
                "-const-http://worksAt<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "-const-http://Chipotle<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        // Convert it to a StatementPattern.
        final StatementPattern statementPattern = FluoStringConverter.toStatementPattern(patternString);

        // Enusre it converted to the expected result.
        final Var subject = new Var("x");
        final Var predicate = new Var("-const-http://worksAt", new URIImpl("http://worksAt"));
        predicate.setAnonymous(true);
        predicate.setConstant(true);
        final Var object = new Var("-const-http://Chipotle", new URIImpl("http://Chipotle"));
        object.setAnonymous(true);
        object.setConstant(true);
        final StatementPattern expected = new StatementPattern(subject, predicate, object);

        assertEquals(expected, statementPattern);
    }

    @Test
    public void toVar_uri() {
        // Setup the string representation of the variable.
        final String varString = "-const-http://Chipotle<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        // Convert it to a Var object.
        final Var var = FluoStringConverter.toVar(varString);

        // Ensure it converted to the expected result.
        final Var expected = new Var("-const-http://Chipotle", new URIImpl("http://Chipotle"));
        expected.setAnonymous(true);
        expected.setConstant(true);

        assertEquals(expected, var);
    }

    @Test
    public void toVar_int() throws MalformedQueryException {
        // Setup the string representation of the variable.
        final String varString = "-const-5<<~>>http://www.w3.org/2001/XMLSchema#integer";

        // Convert it to a Var object.
        final Var result = FluoStringConverter.toVar(varString);

        // Ensure it converted to the expected result.
        final Var expected = new Var("-const-5", new LiteralImpl("5", XMLSchema.INTEGER));
        expected.setAnonymous(true);
        expected.setConstant(true);

        assertEquals(expected, result);
    }

    @Test
    public void toVar_string() {
        // Setup the string representation of the variable.
        final String varString = "-const-Chipotle<<~>>http://www.w3.org/2001/XMLSchema#string";

        // Convert it to a Var object.
        final Var result = FluoStringConverter.toVar(varString);

        // Ensure it converted to the expected result.
        final Var expected = new Var("-const-Chipotle", new LiteralImpl("Chipotle", XMLSchema.STRING));
        expected.setAnonymous(true);
        expected.setConstant(true);

        assertEquals(expected, result);
    }
}