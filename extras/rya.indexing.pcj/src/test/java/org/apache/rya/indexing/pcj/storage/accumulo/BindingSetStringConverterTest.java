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
package org.apache.rya.indexing.pcj.storage.accumulo;

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;
import org.openrdf.model.impl.BooleanLiteralImpl;
import org.openrdf.model.impl.DecimalLiteralImpl;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

/**
 * Tests the methods of {@link BindingSetStringConverter}.
 */
public class BindingSetStringConverterTest {

    @Test
    public void toString_URIs() throws BindingSetConversionException {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new URIImpl("http://a"));
        originalBindingSet.addBinding("y", new URIImpl("http://b"));
        originalBindingSet.addBinding("z", new URIImpl("http://c"));

        // Convert it to a String.
        final VariableOrder varOrder = new VariableOrder("y", "z", "x");
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        final String bindingSetString = converter.convert(originalBindingSet, varOrder);

        // Ensure it converted to the expected result.l
        final String expected =
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://c<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        assertEquals(expected, bindingSetString);
    }

    @Test
    public void toString_Decimal() throws BindingSetConversionException {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new DecimalLiteralImpl(new BigDecimal(2.5)));

        // Convert it to a String.
        final VariableOrder varOrder = new VariableOrder("x");
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        final String bindingSetString = converter.convert(originalBindingSet, varOrder);

        // Ensure it converted to the expected result.
        final String expected = "2.5<<~>>http://www.w3.org/2001/XMLSchema#decimal";
        assertEquals(expected, bindingSetString);
    }

    @Test
    public void toString_Boolean() throws BindingSetConversionException {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new BooleanLiteralImpl(true));

        // Convert it to a String.
        final VariableOrder varOrder = new VariableOrder("x");
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        final String bindingSetString = converter.convert(originalBindingSet, varOrder);

        // Ensure it converted to the expected result.
        final String expected = "true<<~>>http://www.w3.org/2001/XMLSchema#boolean";
        assertEquals(expected, bindingSetString);
    }

    @Test
    public void toString_Integer() throws BindingSetConversionException {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new IntegerLiteralImpl(BigInteger.valueOf(5)));

        // Convert it to a String.
        final VariableOrder varOrder = new VariableOrder("x");
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        final String bindingSetString = converter.convert(originalBindingSet, varOrder);

        // Ensure it converted to the expected result.
        final String expected = "5<<~>>http://www.w3.org/2001/XMLSchema#integer";
        assertEquals(expected, bindingSetString);
    }

    /**
     * All of the Bindings in the BindingSet exactly match the variable order.
     * This is the simplest case and is legal.
     */
    @Test
    public void toString_bindingsMatchVarOrder() throws BindingSetConversionException {
        // Setup the Binding Set.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new URIImpl("http://a"));
        originalBindingSet.addBinding("y", new URIImpl("http://b"));

        // Setup the variable order.
        final VariableOrder varOrder = new VariableOrder("x", "y");

        // Create the String representation of the BindingSet.
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        final String bindingSetString = converter.convert(originalBindingSet, varOrder);

        // Ensure the expected value was created.
        final String expected =
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI";
        assertEquals(expected, bindingSetString);
    }

    /**
     * The BindingSet has fewer Bindings than there are variables in the variable
     * order, but they are all in the variable order. This is the case where
     * the missing bindings were optional.
     */
    @Test
    public void toString_bindingsSubsetOfVarOrder() throws BindingSetConversionException {
        // Setup the Binding Set.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new URIImpl("http://a"));
        originalBindingSet.addBinding("y", new URIImpl("http://b"));

        // Setup the variable order.
        final VariableOrder varOrder = new VariableOrder("x", "a", "y", "b");

        // Create the String representation of the BindingSet.
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        final String bindingSetString = converter.convert(originalBindingSet, varOrder);

        // Ensure the expected value was created.
        final String expected =
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                BindingSetStringConverter.NULL_VALUE_STRING + ":::" +
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                BindingSetStringConverter.NULL_VALUE_STRING;
        assertEquals(expected, bindingSetString);
    }

    /**
     * The BindingSet has a Binding whose name is not in the variable order.
     * This is illegal.
     */
    @Test(expected = IllegalArgumentException.class)
    public void toString_bindingNotInVariableOrder() throws BindingSetConversionException {
        // Setup the Binding Set.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new URIImpl("http://a"));
        originalBindingSet.addBinding("y", new URIImpl("http://b"));
        originalBindingSet.addBinding("z", new URIImpl("http://d"));

        // Setup the variable order.
        final VariableOrder varOrder = new VariableOrder("x", "y");

        // Create the String representation of the BindingSet. This will throw an exception.
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        converter.convert(originalBindingSet, varOrder);
    }

    @Test
    public void fromString() throws BindingSetConversionException {
        // Setup the String that will be converted.
        final String bindingSetString =
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://c<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        // Convert it to a BindingSet
        final VariableOrder varOrder = new VariableOrder("y", "z", "x");
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        final BindingSet bindingSet = converter.convert(bindingSetString, varOrder);

        // Ensure it converted to the expected result.
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("x", new URIImpl("http://a"));
        expected.addBinding("y", new URIImpl("http://b"));
        expected.addBinding("z", new URIImpl("http://c"));

        assertEquals(expected, bindingSet);
    }

    /**
     * Ensures that when a binding set is converted from a String back to a
     * BindingSet, null values do not get converted into Bindings.
     */
    @Test
    public void fromString_nullValues() throws BindingSetConversionException {
        // Setup the String that will be converted.
        final String bindingSetString =
                "http://value 1<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                BindingSetStringConverter.NULL_VALUE_STRING + ":::" +
                "http://value 2<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                BindingSetStringConverter.NULL_VALUE_STRING;

        // Convert it to a BindingSet
        final VariableOrder varOrder = new VariableOrder("x", "a", "y", "b");
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        final BindingSet bindingSet = converter.convert(bindingSetString, varOrder);

        // Ensure it converted to the expected reuslt.
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("x", new URIImpl("http://value 1"));
        expected.addBinding("y", new URIImpl("http://value 2"));

        assertEquals(expected, bindingSet);
    }

    @Test
    public void fromString_Decimal() throws BindingSetConversionException {
        // Setup the String that will be converted.
        final String bindingSetString = "2.5<<~>>http://www.w3.org/2001/XMLSchema#decimal";

        // Convert it to a BindingSet
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        final BindingSet bindingSet = converter.convert(bindingSetString, new VariableOrder("x"));

        // Ensure it converted to the expected result.
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("x", new DecimalLiteralImpl(new BigDecimal(2.5)));

        assertEquals(expected, bindingSet);
    }

    @Test
    public void fromString_Boolean() throws BindingSetConversionException {
        // Setup the String that will be converted.
        final String bindingSetString = "true<<~>>http://www.w3.org/2001/XMLSchema#boolean";

        // Convert it to a BindingSet
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        final BindingSet bindingSet = converter.convert(bindingSetString, new VariableOrder("x"));

        // Ensure it converted to the expected result.
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("x", new BooleanLiteralImpl(true));

        assertEquals(expected, bindingSet);
    }

    @Test
    public void fromString_Integer() throws BindingSetConversionException {
        // Setup the String that will be converted.
        final String bindingSetString = "5<<~>>http://www.w3.org/2001/XMLSchema#integer";

        // Convert it to a BindingSet
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        final BindingSet bindingSet = converter.convert(bindingSetString, new VariableOrder("x"));

        // Ensure it converted to the expected result.
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("x", new IntegerLiteralImpl(BigInteger.valueOf(5)));

        assertEquals(expected, bindingSet);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromString_varOrderTooShort() throws BindingSetConversionException {
        // Setup the String that will be converted.
        final String bindingSetString =
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        // This variable order is too short.
        final VariableOrder varOrder = new VariableOrder("x");

        // The conversion should throw an exception.
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        converter.convert(bindingSetString, varOrder);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromString_varOrderTooLong() throws BindingSetConversionException {
        // Setup the String that will be converted.
        final String bindingSetString =
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        // This variable order is too long.
        final VariableOrder varOrder = new VariableOrder("x", "y", "z");

        // The conversion should throw an exception.
        final BindingSetConverter<String> converter = new BindingSetStringConverter();
        converter.convert(bindingSetString, varOrder);
    }
}