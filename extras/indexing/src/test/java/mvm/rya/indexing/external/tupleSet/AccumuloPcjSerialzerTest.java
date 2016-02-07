package mvm.rya.indexing.external.tupleSet;

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

import mvm.rya.api.resolver.RyaTypeResolverException;

import org.junit.Test;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.vividsolutions.jts.util.Assert;

public class AccumuloPcjSerialzerTest {

	@Test
	public void basicShortUriBsTest() {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",new URIImpl("http://uri1"));
		bs.addBinding("Y",new URIImpl("http://uri2"));
		final String[] varOrder = new String[]{"X","Y"};
		try {
			final byte[] byteVal = AccumuloPcjSerializer.serialize(bs, varOrder);
			final BindingSet newBs = AccumuloPcjSerializer.deSerialize(byteVal, varOrder);
			Assert.equals(bs, newBs);
		} catch (final RyaTypeResolverException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void basicLongUriBsTest() {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",new URIImpl("http://uri1"));
		bs.addBinding("Y",new URIImpl("http://uri2"));
		bs.addBinding("Z",new URIImpl("http://uri3"));
		bs.addBinding("A",new URIImpl("http://uri4"));
		bs.addBinding("B",new URIImpl("http://uri5"));
		final String[] varOrder = new String[]{"X","Y","Z","A","B"};
		try {
			final byte[] byteVal = AccumuloPcjSerializer.serialize(bs, varOrder);
			final BindingSet newBs = AccumuloPcjSerializer.deSerialize(byteVal, varOrder);
			Assert.equals(bs, newBs);
		} catch (final RyaTypeResolverException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void basicShortStringLiteralBsTest() {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",new LiteralImpl("literal1"));
		bs.addBinding("Y",new LiteralImpl("literal2"));
		final String[] varOrder = new String[]{"X","Y"};
		try {
			final byte[] byteVal = AccumuloPcjSerializer.serialize(bs, varOrder);
			final BindingSet newBs = AccumuloPcjSerializer.deSerialize(byteVal, varOrder);
			Assert.equals(bs, newBs);
		} catch (final RyaTypeResolverException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void basicShortMixLiteralBsTest() {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",new LiteralImpl("literal1"));
		bs.addBinding("Y",new LiteralImpl("5", new URIImpl("http://www.w3.org/2001/XMLSchema#integer")));
		final String[] varOrder = new String[]{"X","Y"};
		try {
			final byte[] byteVal = AccumuloPcjSerializer.serialize(bs, varOrder);
			final BindingSet newBs = AccumuloPcjSerializer.deSerialize(byteVal, varOrder);
			Assert.equals(bs, newBs);
		} catch (final RyaTypeResolverException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void basicLongMixLiteralBsTest() {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",new LiteralImpl("literal1"));
		bs.addBinding("Y",new LiteralImpl("5", new URIImpl("http://www.w3.org/2001/XMLSchema#integer")));
		bs.addBinding("Z",new LiteralImpl("5.0", new URIImpl("http://www.w3.org/2001/XMLSchema#double")));
		bs.addBinding("W",new LiteralImpl("1000", new URIImpl("http://www.w3.org/2001/XMLSchema#long")));
		final String[] varOrder = new String[]{"W","X","Y","Z"};
		try {
			final byte[] byteVal = AccumuloPcjSerializer.serialize(bs, varOrder);
			final BindingSet newBs = AccumuloPcjSerializer.deSerialize(byteVal, varOrder);
			Assert.equals(bs, newBs);
		} catch (final RyaTypeResolverException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void basicMixUriLiteralBsTest() {
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("X",new LiteralImpl("literal1"));
		bs.addBinding("Y",new LiteralImpl("5", new URIImpl("http://www.w3.org/2001/XMLSchema#integer")));
		bs.addBinding("Z",new LiteralImpl("5.0", new URIImpl("http://www.w3.org/2001/XMLSchema#double")));
		bs.addBinding("W",new LiteralImpl("1000", new URIImpl("http://www.w3.org/2001/XMLSchema#long")));
		bs.addBinding("A",new URIImpl("http://uri1"));
		bs.addBinding("B",new URIImpl("http://uri2"));
		bs.addBinding("C",new URIImpl("http://uri3"));
		final String[] varOrder = new String[]{"A","W","X","Y","Z","B","C"};
		try {
			final byte[] byteVal = AccumuloPcjSerializer.serialize(bs, varOrder);
			final BindingSet newBs = AccumuloPcjSerializer.deSerialize(byteVal, varOrder);
			Assert.equals(bs, newBs);
		} catch (final RyaTypeResolverException e) {
			e.printStackTrace();
		}
	}
}
