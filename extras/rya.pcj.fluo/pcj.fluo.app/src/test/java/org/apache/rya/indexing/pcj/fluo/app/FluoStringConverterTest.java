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

import static org.apache.rya.api.domain.VarNameUtils.prependConstant;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.TYPE_DELIM;
import static org.junit.Assert.assertEquals;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;
import org.junit.Test;

/**
 * Tests the methods of {@link FluoStringConverterTest}.
 */
public class FluoStringConverterTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

	@Test
	public void statementPatternToString() throws MalformedQueryException {
        // Setup a StatementPattern that represents "?x <http://worksAt> <http://Chipotle>."
        final Var subject = new Var("x");
        final Var predicate = new Var(prependConstant("http://worksAt"), VF.createIRI("http://worksAt"));
        predicate.setConstant(true);
        final Var object = new Var(prependConstant("http://Chipotle"), VF.createIRI("http://Chipotle"));
        object.setConstant(true);
        final StatementPattern pattern = new StatementPattern(subject, predicate, object);

        // Convert the pattern to a String.
        final String spString = FluoStringConverter.toStatementPatternString(pattern);

        // Ensure it converted to the expected result.
        final String expected = "x:::" +
                prependConstant("http://worksAt<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::") +
                prependConstant("http://Chipotle<<~>>http://www.w3.org/2001/XMLSchema#anyURI");

        assertEquals(spString, expected);
	}

    @Test
    public void stringToStatementPattern() {
        // Setup the String representation of a statement pattern.
        final String patternString = "x:::" +
                prependConstant("http://worksAt<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::") +
                prependConstant("http://Chipotle<<~>>http://www.w3.org/2001/XMLSchema#anyURI");

        // Convert it to a StatementPattern.
        final StatementPattern statementPattern = FluoStringConverter.toStatementPattern(patternString);

        // Enusre it converted to the expected result.
        final Var subject = new Var("x");
        final Var predicate = new Var(prependConstant("http://worksAt"), VF.createIRI("http://worksAt"));
        predicate.setConstant(true);
        final Var object = new Var(prependConstant("http://Chipotle"), VF.createIRI("http://Chipotle"));
        object.setConstant(true);
        final StatementPattern expected = new StatementPattern(subject, predicate, object);

        assertEquals(expected, statementPattern);
    }

    @Test
    public void toVar_uri() {
        // Setup the string representation of the variable.
        final String varString = String.format(prependConstant("http://Chipotle%s%s"),TYPE_DELIM,XMLSchema.ANYURI );

        // Convert it to a Var object.
        final Var var = FluoStringConverter.toVar(varString);

        // Ensure it converted to the expected result.
        final Var expected = new Var(prependConstant("http://Chipotle"), VF.createIRI("http://Chipotle"));
        expected.setConstant(true);

        assertEquals(expected, var);
    }

    @Test
    public void toVar_int() throws MalformedQueryException {
        // Setup the string representation of the variable.
        final String varString = prependConstant("5<<~>>http://www.w3.org/2001/XMLSchema#integer");

        // Convert it to a Var object.
        final Var result = FluoStringConverter.toVar(varString);

        // Ensure it converted to the expected result.
        final Var expected = new Var(prependConstant("5"), VF.createLiteral("5", XMLSchema.INTEGER));
        expected.setConstant(true);

        assertEquals(expected, result);
    }

    @Test
    public void toVar_string() {
        // Setup the string representation of the variable.
        final String varString = prependConstant("Chipotle<<~>>http://www.w3.org/2001/XMLSchema#string");

        // Convert it to a Var object.
        final Var result = FluoStringConverter.toVar(varString);

        // Ensure it converted to the expected result.
        final Var expected = new Var(prependConstant("Chipotle"), VF.createLiteral("Chipotle", XMLSchema.STRING));
        expected.setConstant(true);

        assertEquals(expected, result);
    }
}