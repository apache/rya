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

import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

/**
 * Tests the methods of {@link AccumuloPcjSerialzer}.
 */
public class AccumuloPcjSerializerTest {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    /**
     * The BindingSet has fewer Bindings than there are variables in the variable
     * order, but they are all in the variable order. This is the case where
     * the missing bindings were optional.
     */
    @Test
    public void serialize_bindingsSubsetOfVarOrder() throws BindingSetConversionException {
        // Setup the Binding Set.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", VF.createIRI("http://a"));
        originalBindingSet.addBinding("y", VF.createIRI("http://b"));

        // Setup the variable order.
        final VariableOrder varOrder = new VariableOrder("x", "a", "y", "b");

        // Create the byte[] representation of the BindingSet.
        BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
        byte[] serialized = converter.convert(originalBindingSet, varOrder);

        // Deserialize the byte[] back into the binding set.
        BindingSet deserialized = converter.convert(serialized, varOrder);

        // Ensure the deserialized value matches the original.
        assertEquals(originalBindingSet, deserialized);
    }

    /**
     * The BindingSet has more Bindings than there are variables in the variable order.
     * This is the case where a Group By clause does not include all of the Bindings that
     * are in the Binding Set.
     */
    @Test
    public void serialize_bindingNotInVariableOrder() throws RyaTypeResolverException, BindingSetConversionException {
        // Setup the Binding Set.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", VF.createIRI("http://a"));
        originalBindingSet.addBinding("y", VF.createIRI("http://b"));
        originalBindingSet.addBinding("z", VF.createIRI("http://d"));

        // Setup the variable order.
        final VariableOrder varOrder = new VariableOrder("x", "y");

        // Serialize the Binding Set.
        BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
        byte[] serialized = converter.convert(originalBindingSet, varOrder);
        
        // Deserialize it again.
        BindingSet deserialized = converter.convert(serialized, varOrder);
        
        // Show that it only contains the bindings that were part of the Variable Order.
        MapBindingSet expected = new MapBindingSet();
        expected.addBinding("x", VF.createIRI("http://a"));
        expected.addBinding("y", VF.createIRI("http://b"));
        
        assertEquals(expected, deserialized);
    }

	@Test
	public void basicShortUriBsTest() throws BindingSetConversionException {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X", VF.createIRI("http://uri1"));
		bs.addBinding("Y", VF.createIRI("http://uri2"));
		final VariableOrder varOrder = new VariableOrder("X","Y");

		BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
		final byte[] byteVal = converter.convert(bs, varOrder);
		final BindingSet newBs = converter.convert(byteVal, varOrder);
		assertEquals(bs, newBs);
	}

	@Test
	public void basicLongUriBsTest() throws BindingSetConversionException {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X", VF.createIRI("http://uri1"));
		bs.addBinding("Y", VF.createIRI("http://uri2"));
		bs.addBinding("Z",VF.createIRI("http://uri3"));
		bs.addBinding("A", VF.createIRI("http://uri4"));
		bs.addBinding("B", VF.createIRI("http://uri5"));
		final VariableOrder varOrder = new VariableOrder("X","Y","Z","A","B");

		BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
		final byte[] byteVal = converter.convert(bs, varOrder);
		final BindingSet newBs = converter.convert(byteVal, varOrder);
		assertEquals(bs, newBs);
	}

	@Test
	public void basicShortStringLiteralBsTest() throws BindingSetConversionException {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X", VF.createLiteral("literal1"));
		bs.addBinding("Y", VF.createLiteral("literal2"));
		final VariableOrder varOrder = new VariableOrder("X","Y");

		BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
		final byte[] byteVal = converter.convert(bs, varOrder);
		final BindingSet newBs = converter.convert(byteVal, varOrder);
		assertEquals(bs, newBs);
	}

	@Test
	public void basicShortMixLiteralBsTest() throws BindingSetConversionException {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",VF.createLiteral("literal1"));
		bs.addBinding("Y",VF.createLiteral("5", VF.createIRI("http://www.w3.org/2001/XMLSchema#integer")));
		final VariableOrder varOrder = new VariableOrder("X","Y");

		BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
		final byte[] byteVal = converter.convert(bs, varOrder);
		final BindingSet newBs = converter.convert(byteVal, varOrder);
		assertEquals(bs, newBs);
	}

	@Test
	public void basicLongMixLiteralBsTest() throws BindingSetConversionException {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X", VF.createLiteral("literal1"));
		bs.addBinding("Y", VF.createLiteral("5", VF.createIRI("http://www.w3.org/2001/XMLSchema#integer")));
		bs.addBinding("Z", VF.createLiteral("5.0", VF.createIRI("http://www.w3.org/2001/XMLSchema#double")));
		bs.addBinding("W", VF.createLiteral("1000", VF.createIRI("http://www.w3.org/2001/XMLSchema#long")));
		final VariableOrder varOrder = new VariableOrder("W","X","Y","Z");

		BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
		final byte[] byteVal = converter.convert(bs, varOrder);
		final BindingSet newBs = converter.convert(byteVal, varOrder);
		assertEquals(bs, newBs);
	}

	@Test
	public void basicMixUriLiteralBsTest() throws BindingSetConversionException {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X", VF.createLiteral("literal1"));
		bs.addBinding("Y", VF.createLiteral("5", VF.createIRI("http://www.w3.org/2001/XMLSchema#integer")));
		bs.addBinding("Z", VF.createLiteral("5.0", VF.createIRI("http://www.w3.org/2001/XMLSchema#double")));
		bs.addBinding("W", VF.createLiteral("1000", VF.createIRI("http://www.w3.org/2001/XMLSchema#long")));
		bs.addBinding("A", VF.createIRI("http://uri1"));
		bs.addBinding("B", VF.createIRI("http://uri2"));
		bs.addBinding("C", VF.createIRI("http://uri3"));
		final VariableOrder varOrder = new VariableOrder("A","W","X","Y","Z","B","C");

		BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
		final byte[] byteVal = converter.convert(bs, varOrder);
		final BindingSet newBs = converter.convert(byteVal, varOrder);
		assertEquals(bs, newBs);
	}
}