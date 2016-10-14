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

//package org.apache.rya;

//
//import java.util.List;
//
//import junit.framework.TestCase;
//
//import org.openrdf.model.BNode;
//import org.openrdf.model.Resource;
//import org.openrdf.model.URI;
//import org.openrdf.model.Value;
//import org.openrdf.model.impl.ValueFactoryImpl;
//
//import com.google.common.io.ByteStreams;
//
//import static org.apache.rya.api.RdfCloudTripleStoreUtils.*;
//
//public class RdfCloudTripleStoreUtilsTest extends TestCase {
//
//	public void testWriteReadURI() throws Exception {
//		final ValueFactoryImpl vf = new ValueFactoryImpl();
//		URI uri = vf.createURI("http://www.example.org/test/rel");
//		byte[] value = writeValue(uri);
//
//		Value readValue = readValue(ByteStreams
//				.newDataInput(value), vf);
//		assertEquals(uri, readValue);
//	}
//
//	public void testWriteReadBNode() throws Exception {
//		final ValueFactoryImpl vf = new ValueFactoryImpl();
//		Value val = vf.createBNode("bnodeid");
//		byte[] value = writeValue(val);
//
//		Value readValue = readValue(ByteStreams
//				.newDataInput(value), vf);
//		assertEquals(val, readValue);
//	}
//
//	public void testWriteReadLiteral() throws Exception {
//		final ValueFactoryImpl vf = new ValueFactoryImpl();
//		Value val = vf.createLiteral("myliteral");
//		byte[] value = writeValue(val);
//
//		Value readValue = readValue(ByteStreams
//				.newDataInput(value), vf);
//		assertEquals(val, readValue);
//	}
//
//	public void testContexts() throws Exception {
//		final ValueFactoryImpl vf = new ValueFactoryImpl();
//		BNode cont1 = vf.createBNode("cont1");
//		BNode cont2 = vf.createBNode("cont2");
//		BNode cont3 = vf.createBNode("cont3");
//
//		byte[] cont_bytes = writeContexts(cont1, cont2,
//				cont3);
//		final String cont = new String(cont_bytes);
//		System.out.println(cont);
//
//		List<Resource> contexts = readContexts(cont_bytes,
//				vf);
//		for (Resource resource : contexts) {
//			System.out.println(resource);
//		}
//	}
//}
