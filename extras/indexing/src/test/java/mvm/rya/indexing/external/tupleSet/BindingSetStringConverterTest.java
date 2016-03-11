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

package mvm.rya.indexing.external.tupleSet;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.Test;
import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.model.impl.DecimalLiteralImpl;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

import mvm.rya.indexing.external.tupleSet.PcjTables.VariableOrder;

/**
 * Tests the methods of {@link BindingSetStringConverter}.
 */
public class BindingSetStringConverterTest {

    @Test
    public void toString_URIs() {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new URIImpl("http://a"));
        originalBindingSet.addBinding("y", new URIImpl("http://b"));
        originalBindingSet.addBinding("z", new URIImpl("http://c"));

        // Convert it to a String.
        final VariableOrder varOrder = new VariableOrder("y", "z", "x");
        final String bindingSetString = BindingSetStringConverter.toString(originalBindingSet, varOrder);

        // Ensure it converted to the expected result.
        final String expected =
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://c<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        assertEquals(expected, bindingSetString);
    }

    @Test
    public void toString_Decimal() {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new DecimalLiteralImpl(new BigDecimal(2.5)));

        // Convert it to a String.
        final VariableOrder varOrder = new VariableOrder("x");
        final String bindingSetString = BindingSetStringConverter.toString(originalBindingSet, varOrder);

        // Ensure it converted to the expected result.
        final String expected = "2.5<<~>>http://www.w3.org/2001/XMLSchema#decimal";
        assertEquals(expected, bindingSetString);
    }

    @Test
    public void toString_Boolean() {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new BooleanLiteralImpl(true));

        // Convert it to a String.
        final VariableOrder varOrder = new VariableOrder("x");
        final String bindingSetString = BindingSetStringConverter.toString(originalBindingSet, varOrder);

        // Ensure it converted to the expected result.
        final String expected = "true<<~>>http://www.w3.org/2001/XMLSchema#boolean";
        assertEquals(expected, bindingSetString);
    }

    @Test
    public void toString_Integer() {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new IntegerLiteralImpl(BigInteger.valueOf(5)));

        // Convert it to a String.
        final VariableOrder varOrder = new VariableOrder("x");
        final String bindingSetString = BindingSetStringConverter.toString(originalBindingSet, varOrder);

        // Ensure it converted to the expected result.
        final String expected = "5<<~>>http://www.w3.org/2001/XMLSchema#integer";
        assertEquals(expected, bindingSetString);
    }

    @Test(expected = IllegalArgumentException.class)
    public void toString_varOrderTooShort() {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new URIImpl("http://a"));
        originalBindingSet.addBinding("y", new URIImpl("http://b"));

        // This variable order that is too short.
        final VariableOrder varOrder = new VariableOrder("y");

        // The conversion should throw an exception.
        BindingSetStringConverter.toString(originalBindingSet, varOrder);
    }

    @Test(expected = IllegalArgumentException.class)
    public void toString_varOrderTooLong() {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new URIImpl("http://a"));
        originalBindingSet.addBinding("y", new URIImpl("http://b"));

        // This variable order is too long.
        final VariableOrder varOrder = new VariableOrder("x", "y", "z");

        // The conversion should throw an exception.
        BindingSetStringConverter.toString(originalBindingSet, varOrder);
    }

    @Test(expected = IllegalArgumentException.class)
    public void toString_varOrderWrongBindingNames() {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new URIImpl("http://a"));
        originalBindingSet.addBinding("y", new URIImpl("http://b"));

        // This variable order has the wrong binding names.
        final VariableOrder varOrder = new VariableOrder("x", "a");

        // The conversion should throw an exception.
        BindingSetStringConverter.toString(originalBindingSet, varOrder);
    }

    @Test
    public void fromString() {
        // Setup the String that will be converted.
        final String bindingSetString =
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://c<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        // Convert it to a BindingSet
        final VariableOrder varOrder = new VariableOrder("y", "z", "x");
        final BindingSet bindingSet = BindingSetStringConverter.fromString(bindingSetString, varOrder);

        // Ensure it converted to the expected result.
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("x", new URIImpl("http://a"));
        expected.addBinding("y", new URIImpl("http://b"));
        expected.addBinding("z", new URIImpl("http://c"));

        assertEquals(expected, bindingSet);
    }

    @Test
    public void fromString_Decimal() {
        // Setup the String that will be converted.
        final String bindingSetString = "2.5<<~>>http://www.w3.org/2001/XMLSchema#decimal";

        // Convert it to a BindingSet
        final BindingSet bindingSet = BindingSetStringConverter.fromString(bindingSetString, new VariableOrder("x"));

        // Ensure it converted to the expected result.
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("x", new DecimalLiteralImpl(new BigDecimal(2.5)));

        assertEquals(expected, bindingSet);
    }

    @Test
    public void fromString_Boolean() {
        // Setup the String that will be converted.
        final String bindingSetString = "true<<~>>http://www.w3.org/2001/XMLSchema#boolean";

        // Convert it to a BindingSet
        final BindingSet bindingSet = BindingSetStringConverter.fromString(bindingSetString, new VariableOrder("x"));

        // Ensure it converted to the expected result.
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("x", new BooleanLiteralImpl(true));

        assertEquals(expected, bindingSet);
    }

    @Test
    public void fromString_Integer() {
        // Setup the String that will be converted.
        final String bindingSetString = "5<<~>>http://www.w3.org/2001/XMLSchema#integer";

        // Convert it to a BindingSet
        final BindingSet bindingSet = BindingSetStringConverter.fromString(bindingSetString, new VariableOrder("x"));

        // Ensure it converted to the expected result.
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("x", new IntegerLiteralImpl(BigInteger.valueOf(5)));

        assertEquals(expected, bindingSet);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromString_varOrderTooShort() {
        // Setup the String that will be converted.
        final String bindingSetString =
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        // This variable order is too short.
        VariableOrder varOrder = new VariableOrder("x");

        // The conversion should throw an exception.
        BindingSetStringConverter.fromString(bindingSetString, varOrder);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromString_varOrderTooLong() {
        // Setup the String that will be converted.
        final String bindingSetString =
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        // This variable order is too long.
        VariableOrder varOrder = new VariableOrder("x", "y", "z");

        // The conversion should throw an exception.
        BindingSetStringConverter.fromString(bindingSetString, varOrder);
    }
}