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

//package org.apache.rya.api.query.strategy.wholerow;

//
//import junit.framework.TestCase;
//import org.apache.rya.api.RdfCloudTripleStoreConstants;
//import org.apache.rya.api.domain.*;
//import org.apache.rya.api.resolver.RyaContext;
//import org.apache.rya.api.resolver.triple.TripleRow;
//import org.apache.accumulo.core.data.Key;
//import org.apache.accumulo.core.data.Range;
//import org.eclipse.rdf4j.model.ValueFactory;
//import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
//import org.apache.hadoop.io.Text;
//
//import java.util.Map;
//
///**
// * Date: 7/14/12
// * Time: 11:46 AM
// */
//public class PoWholeRowTriplePatternStrategyTest extends TestCase {
//    private static final ValueFactory VF = SimpleValueFactory.getInstance();
//
//    RyaIRI uri = new RyaIRI("urn:test#1234");
//    RyaIRI uri2 = new RyaIRI("urn:test#1235");
//    RyaIRIRange rangeIRI = new RyaIRIRange(uri, uri2);
//    RyaIRIRange rangeIRI2 = new RyaIRIRange(new RyaIRI("urn:test#1235"), new RyaIRI("urn:test#1236"));
//    PoWholeRowTriplePatternStrategy strategy = new PoWholeRowTriplePatternStrategy();
//    RyaContext ryaContext = RyaContext.getInstance();
//
//    RyaType customType1 = new RyaType(VF.createIRI("urn:custom#type"), "1234");
//    RyaType customType2 = new RyaType(VF.createIRI("urn:custom#type"), "1235");
//    RyaType customType3 = new RyaType(VF.createIRI("urn:custom#type"), "1236");
//    RyaTypeRange customTypeRange1 = new RyaTypeRange(customType1, customType2);
//    RyaTypeRange customTypeRange2 = new RyaTypeRange(customType2, customType3);
//
//    public void testPoRange() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(null, uri, rangeIRI, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(null, uri, rangeIRI2, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testPoRangeCustomType() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, customType1, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(null, uri, customTypeRange1, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(null, uri, customTypeRange2, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testPo() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(null, uri, uri, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(null, uri, uri2, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testPoCustomType() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, customType1, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(null, uri, customType1, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(null, uri, customType2, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testPosRange() throws Exception {
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(rangeIRI, uri, uri, null, null);
//        assertTrue(entry.getValue().contains(key));
//
//        entry = strategy.defineRange(rangeIRI2, uri, uri, null, null);
//        assertFalse(entry.getValue().contains(key));
//    }
//
//    public void testPRange() throws Exception {
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(null, rangeIRI, null, null, null);
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//        assertTrue(entry.getValue().contains(key));
//    }
//
//    public void testP() throws Exception {
//        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Range> entry = strategy.defineRange(null, uri, null, null, null);
//        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaContext.serializeTriple(
//                new RyaStatement(uri, uri, uri, null));
//        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
//        Key key = new Key(new Text(tripleRow.getRow()));
//        assertTrue(entry.getValue().contains(key));
//    }
//
//    public void testHandles() throws Exception {
//        //po(ng)
//        assertTrue(strategy.handles(null, uri, uri, null));
//        assertTrue(strategy.handles(null, uri, uri, uri));
//        //po_r(s)(ng)
//        assertTrue(strategy.handles(rangeIRI, uri, uri, null));
//        assertTrue(strategy.handles(rangeIRI, uri, uri, uri));
//        //p(ng)
//        assertTrue(strategy.handles(null, uri, null, null));
//        assertTrue(strategy.handles(null, uri, null, uri));
//        //p_r(o)(ng)
//        assertTrue(strategy.handles(null, uri, rangeIRI, null));
//        assertTrue(strategy.handles(null, uri, rangeIRI, uri));
//        //r(p)(ng)
//        assertTrue(strategy.handles(null, rangeIRI, null, null));
//        assertTrue(strategy.handles(null, rangeIRI, null, uri));
//
//        //false cases
//        //sp..
//        assertFalse(strategy.handles(uri, uri, null, null));
//        //r(s)_p
//        assertFalse(strategy.handles(rangeIRI, uri, null, null));
//    }
//}
