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

import static org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetStringConverter.VISIBILITY_DELIM;
import static org.junit.Assert.assertEquals;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

/**
 * Tests the methods of {@link BindingSetStringConverter}.
 */
public class VisibilityBindingSetStringConverterTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Test
    public void toString_URIs() throws BindingSetConversionException {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", VF.createIRI("http://a"));
        originalBindingSet.addBinding("y", VF.createIRI("http://b"));
        originalBindingSet.addBinding("z", VF.createIRI("http://c"));

        final VisibilityBindingSet visiSet = new VisibilityBindingSet(originalBindingSet, "A&B&C");

        // Convert it to a String.
        final VariableOrder varOrder = new VariableOrder("y", "z", "x");
        final VisibilityBindingSetStringConverter converter = new VisibilityBindingSetStringConverter();
        final String bindingSetString = converter.convert(visiSet, varOrder);

        // Ensure it converted to the expected result.l
        final String expected =
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://c<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI" +
                VISIBILITY_DELIM + "A&B&C";

        assertEquals(expected, bindingSetString);
    }

    @Test
    public void fromString() throws BindingSetConversionException {
        // Setup the String that will be converted.
        final String bindingSetString =
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://c<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI" +
                VISIBILITY_DELIM + "A&B";

        // Convert it to a BindingSet
        final VariableOrder varOrder = new VariableOrder("y", "z", "x");
        final VisibilityBindingSetStringConverter converter = new VisibilityBindingSetStringConverter();
        final BindingSet bindingSet = converter.convert(bindingSetString, varOrder);

        // Ensure it converted to the expected result.
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("z", VF.createIRI("http://c"));
        expected.addBinding("y", VF.createIRI("http://b"));
        expected.addBinding("x", VF.createIRI("http://a"));
        final VisibilityBindingSet visiSet = new VisibilityBindingSet(expected, "A&B");

        assertEquals(visiSet, bindingSet);
    }

    @Test
    public void toString_URIs_noVisi() throws BindingSetConversionException {
        // Setup the binding set that will be converted.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", VF.createIRI("http://a"));
        originalBindingSet.addBinding("y", VF.createIRI("http://b"));
        originalBindingSet.addBinding("z", VF.createIRI("http://c"));

        final VisibilityBindingSet visiSet = new VisibilityBindingSet(originalBindingSet);

        // Convert it to a String.
        final VariableOrder varOrder = new VariableOrder("y", "z", "x");
        final VisibilityBindingSetStringConverter converter = new VisibilityBindingSetStringConverter();
        final String bindingSetString = converter.convert(visiSet, varOrder);

        // Ensure it converted to the expected result.l
        final String expected =
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://c<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        assertEquals(expected, bindingSetString);
    }

    @Test
    public void fromString_noVisi() throws BindingSetConversionException {
        // Setup the String that will be converted.
        final String bindingSetString =
                "http://b<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://c<<~>>http://www.w3.org/2001/XMLSchema#anyURI:::" +
                "http://a<<~>>http://www.w3.org/2001/XMLSchema#anyURI";

        // Convert it to a BindingSet
        final VariableOrder varOrder = new VariableOrder("y", "z", "x");
        final VisibilityBindingSetStringConverter converter = new VisibilityBindingSetStringConverter();
        final BindingSet bindingSet = converter.convert(bindingSetString, varOrder);

        // Ensure it converted to the expected result.
        final MapBindingSet expected = new MapBindingSet();
        expected.addBinding("z", VF.createIRI("http://c"));
        expected.addBinding("y", VF.createIRI("http://b"));
        expected.addBinding("x", VF.createIRI("http://a"));
        final VisibilityBindingSet visiSet = new VisibilityBindingSet(expected);

        assertEquals(visiSet, bindingSet);
    }
}