package org.apache.rya.indexing.pcj.storage.accumulo;

import static org.junit.Assert.assertEquals;

import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.junit.Test;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.MapBindingSet;

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

import org.apache.rya.api.resolver.RyaTypeResolverException;

/**
 * Tests the methods of {@link AccumuloPcjSerialzer}.
 */
public class AccumuloPcjSerialzerTest {

    /**
     * The BindingSet has fewer Bindings than there are variables in the variable
     * order, but they are all in the variable order. This is the case where
     * the missing bindings were optional.
     */
    @Test
    public void serialize_bindingsSubsetOfVarOrder() throws BindingSetConversionException {
        // Setup the Binding Set.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new URIImpl("http://a"));
        originalBindingSet.addBinding("y", new URIImpl("http://b"));

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
     * The BindingSet has a Binding whose name is not in the variable order.
     * This is illegal.
     */
    @Test(expected = IllegalArgumentException.class)
    public void serialize_bindingNotInVariableOrder() throws RyaTypeResolverException, BindingSetConversionException {
        // Setup the Binding Set.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        originalBindingSet.addBinding("x", new URIImpl("http://a"));
        originalBindingSet.addBinding("y", new URIImpl("http://b"));
        originalBindingSet.addBinding("z", new URIImpl("http://d"));

        // Setup the variable order.
        final VariableOrder varOrder = new VariableOrder("x", "y");

        // Create the byte[] representation of the BindingSet. This will throw an exception.
        BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
        converter.convert(originalBindingSet, varOrder);
    }

	@Test
	public void basicShortUriBsTest() throws BindingSetConversionException {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",new URIImpl("http://uri1"));
		bs.addBinding("Y",new URIImpl("http://uri2"));
		final VariableOrder varOrder = new VariableOrder("X","Y");

		BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
		final byte[] byteVal = converter.convert(bs, varOrder);
		final BindingSet newBs = converter.convert(byteVal, varOrder);
		assertEquals(bs, newBs);
	}

	@Test
	public void basicLongUriBsTest() throws BindingSetConversionException {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",new URIImpl("http://uri1"));
		bs.addBinding("Y",new URIImpl("http://uri2"));
		bs.addBinding("Z",new URIImpl("http://uri3"));
		bs.addBinding("A",new URIImpl("http://uri4"));
		bs.addBinding("B",new URIImpl("http://uri5"));
		final VariableOrder varOrder = new VariableOrder("X","Y","Z","A","B");

		BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
		final byte[] byteVal = converter.convert(bs, varOrder);
		final BindingSet newBs = converter.convert(byteVal, varOrder);
		assertEquals(bs, newBs);
	}

	@Test
	public void basicShortStringLiteralBsTest() throws BindingSetConversionException {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",new LiteralImpl("literal1"));
		bs.addBinding("Y",new LiteralImpl("literal2"));
		final VariableOrder varOrder = new VariableOrder("X","Y");

		BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
		final byte[] byteVal = converter.convert(bs, varOrder);
		final BindingSet newBs = converter.convert(byteVal, varOrder);
		assertEquals(bs, newBs);
	}

	@Test
	public void basicShortMixLiteralBsTest() throws BindingSetConversionException {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",new LiteralImpl("literal1"));
		bs.addBinding("Y",new LiteralImpl("5", new URIImpl("http://www.w3.org/2001/XMLSchema#integer")));
		final VariableOrder varOrder = new VariableOrder("X","Y");

		BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
		final byte[] byteVal = converter.convert(bs, varOrder);
		final BindingSet newBs = converter.convert(byteVal, varOrder);
		assertEquals(bs, newBs);
	}

	@Test
	public void basicLongMixLiteralBsTest() throws BindingSetConversionException {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",new LiteralImpl("literal1"));
		bs.addBinding("Y",new LiteralImpl("5", new URIImpl("http://www.w3.org/2001/XMLSchema#integer")));
		bs.addBinding("Z",new LiteralImpl("5.0", new URIImpl("http://www.w3.org/2001/XMLSchema#double")));
		bs.addBinding("W",new LiteralImpl("1000", new URIImpl("http://www.w3.org/2001/XMLSchema#long")));
		final VariableOrder varOrder = new VariableOrder("W","X","Y","Z");

		BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
		final byte[] byteVal = converter.convert(bs, varOrder);
		final BindingSet newBs = converter.convert(byteVal, varOrder);
		assertEquals(bs, newBs);
	}

	@Test
	public void basicMixUriLiteralBsTest() throws BindingSetConversionException {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",new LiteralImpl("literal1"));
		bs.addBinding("Y",new LiteralImpl("5", new URIImpl("http://www.w3.org/2001/XMLSchema#integer")));
		bs.addBinding("Z",new LiteralImpl("5.0", new URIImpl("http://www.w3.org/2001/XMLSchema#double")));
		bs.addBinding("W",new LiteralImpl("1000", new URIImpl("http://www.w3.org/2001/XMLSchema#long")));
		bs.addBinding("A",new URIImpl("http://uri1"));
		bs.addBinding("B",new URIImpl("http://uri2"));
		bs.addBinding("C",new URIImpl("http://uri3"));
		final VariableOrder varOrder = new VariableOrder("A","W","X","Y","Z","B","C");

		BindingSetConverter<byte[]> converter = new AccumuloPcjSerializer();
		final byte[] byteVal = converter.convert(bs, varOrder);
		final BindingSet newBs = converter.convert(byteVal, varOrder);
		assertEquals(bs, newBs);
	}
}