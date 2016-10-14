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

//package org.apache.rya.accumulo;

//
//import junit.framework.TestCase;
//import org.apache.rya.accumulo.AccumuloRdfConfiguration;
//import org.apache.rya.accumulo.DefineTripleQueryRangeFactory;
//import org.apache.rya.accumulo.AccumuloRdfConfiguration;
//import org.apache.rya.accumulo.DefineTripleQueryRangeFactory;
//import org.apache.rya.api.domain.RangeValue;
//import org.apache.accumulo.core.data.Range;
//import org.openrdf.model.URI;
//import org.openrdf.model.Value;
//import org.openrdf.model.ValueFactory;
//import org.openrdf.model.impl.ValueFactoryImpl;
//
//import java.util.Map;
//
//import static org.apache.rya.api.RdfCloudTripleStoreConstants.*;
//
///**
// */
//public class DefineTripleQueryRangeFactoryTest extends TestCase {
//
//    public static final String DELIM_BYTES_STR = new String(DELIM_BYTES);
//    public static final String URI_MARKER_STR = "\u0007";
//    public static final String RANGE_ENDKEY_SUFFIX = "\u0000";
//    DefineTripleQueryRangeFactory factory = new DefineTripleQueryRangeFactory();
//    ValueFactory vf = ValueFactoryImpl.getInstance();
//    static String litdupsNS = "urn:test:litdups#";
//
//    private AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
//
//    public void testSPOCases() throws Exception {
//        URI cpu = vf.createURI(litdupsNS, "cpu");
//        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
//        URI obj = vf.createURI(litdupsNS, "uri1");
//
//        //spo
//        Map.Entry<TABLE_LAYOUT, Range> entry =
//                factory.defineRange(cpu, loadPerc, obj, conf);
//        assertEquals(TABLE_LAYOUT.SPO, entry.getKey());
//        String expected_start = URI_MARKER_STR + cpu.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + loadPerc.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + obj.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        assertEquals(expected_start + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//
//
//        //sp
//        entry = factory.defineRange(cpu, loadPerc, null, conf);
//        assertEquals(TABLE_LAYOUT.SPO, entry.getKey());
//        expected_start = URI_MARKER_STR + cpu.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + loadPerc.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        assertEquals(expected_start + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//
//        //s
//        entry = factory.defineRange(cpu, null, null, conf);
//        assertEquals(TABLE_LAYOUT.SPO, entry.getKey());
//        expected_start = URI_MARKER_STR + cpu.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        assertEquals(expected_start + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//
//        //all
//        entry = factory.defineRange(null, null, null, conf);
//        assertEquals(TABLE_LAYOUT.SPO, entry.getKey());
//        assertEquals("",
//                entry.getValue().getStartKey().getRow().toString());
//        assertEquals(new String(new byte[]{Byte.MAX_VALUE}) + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//    }
//
//    public void testSPOCasesWithRanges() throws Exception {
//        URI subj_start = vf.createURI(litdupsNS, "subj_start");
//        URI subj_end = vf.createURI(litdupsNS, "subj_stop");
//        URI pred_start = vf.createURI(litdupsNS, "pred_start");
//        URI pred_end = vf.createURI(litdupsNS, "pred_stop");
//        URI obj_start = vf.createURI(litdupsNS, "obj_start");
//        URI obj_end = vf.createURI(litdupsNS, "obj_stop");
//
//        Value subj = new RangeValue(subj_start, subj_end);
//        Value pred = new RangeValue(pred_start, pred_end);
//        Value obj = new RangeValue(obj_start, obj_end);
//
//        //spo - o has range
//        Map.Entry<TABLE_LAYOUT, Range> entry =
//                factory.defineRange(subj_start, pred_start, obj, conf);
//        assertEquals(TABLE_LAYOUT.SPO, entry.getKey());
//        String expected_start = URI_MARKER_STR + subj_start.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + pred_start.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + obj_start.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        String expected_end = URI_MARKER_STR + subj_start.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + pred_start.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + obj_end.stringValue();
//        assertEquals(expected_end + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//
//        //sp - p has range
//        entry = factory.defineRange(subj_start, pred, null, conf);
//        assertEquals(TABLE_LAYOUT.SPO, entry.getKey());
//        expected_start = URI_MARKER_STR + subj_start.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + pred_start.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        expected_end = URI_MARKER_STR + subj_start.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + pred_end.stringValue();
//        assertEquals(expected_end + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//
//        //s - s has range
//        entry = factory.defineRange(subj, null, null, conf);
//        assertEquals(TABLE_LAYOUT.SPO, entry.getKey());
//        expected_start = URI_MARKER_STR + subj_start.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        expected_end = URI_MARKER_STR + subj_end.stringValue();
//        assertEquals(expected_end + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//    }
//
//    public void testPOCases() throws Exception {
//        URI loadPerc = vf.createURI(litdupsNS, "loadPerc");
//        URI obj = vf.createURI(litdupsNS, "uri1");
//
//        //po
//        Map.Entry<TABLE_LAYOUT, Range> entry =
//                factory.defineRange(null, loadPerc, obj, conf);
//        assertEquals(TABLE_LAYOUT.PO, entry.getKey());
//        String expected_start = URI_MARKER_STR + loadPerc.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + obj.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        assertEquals(expected_start + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//
//        //p
//        entry = factory.defineRange(null, loadPerc, null, conf);
//        assertEquals(TABLE_LAYOUT.PO, entry.getKey());
//        expected_start = URI_MARKER_STR + loadPerc.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        assertEquals(expected_start + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//    }
//
//    public void testPOCasesWithRanges() throws Exception {
//        URI pred_start = vf.createURI(litdupsNS, "pred_start");
//        URI pred_end = vf.createURI(litdupsNS, "pred_stop");
//        URI obj_start = vf.createURI(litdupsNS, "obj_start");
//        URI obj_end = vf.createURI(litdupsNS, "obj_stop");
//
//        Value pred = new RangeValue(pred_start, pred_end);
//        Value obj = new RangeValue(obj_start, obj_end);
//
//        //po
//        Map.Entry<TABLE_LAYOUT, Range> entry =
//                factory.defineRange(null, pred_start, obj, conf);
//        assertEquals(TABLE_LAYOUT.PO, entry.getKey());
//        String expected_start = URI_MARKER_STR + pred_start.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + obj_start.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        String expected_end = URI_MARKER_STR + pred_start.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + obj_end.stringValue();
//        assertEquals(expected_end + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//
//        //p
//        entry = factory.defineRange(null, pred, null, conf);
//        assertEquals(TABLE_LAYOUT.PO, entry.getKey());
//        expected_start = URI_MARKER_STR + pred_start.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        expected_end = URI_MARKER_STR + pred_end.stringValue();
//        assertEquals(expected_end + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//    }
//
//    public void testOSPCases() throws Exception {
//        URI cpu = vf.createURI(litdupsNS, "cpu");
//        URI obj = vf.createURI(litdupsNS, "uri1");
//
//        //so
//        Map.Entry<TABLE_LAYOUT, Range> entry =
//                factory.defineRange(cpu, null, obj, conf);
//        assertEquals(TABLE_LAYOUT.OSP, entry.getKey());
//        String expected_start = URI_MARKER_STR + obj.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + cpu.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        assertEquals(expected_start + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//
//        //o
//        entry = factory.defineRange(null, null, obj, conf);
//        assertEquals(TABLE_LAYOUT.OSP, entry.getKey());
//        expected_start = URI_MARKER_STR + obj.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        assertEquals(expected_start + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//    }
//
//
//    public void testOSPCasesWithRanges() throws Exception {
//        URI subj_start = vf.createURI(litdupsNS, "subj_start");
//        URI subj_end = vf.createURI(litdupsNS, "subj_stop");
//        URI obj_start = vf.createURI(litdupsNS, "obj_start");
//        URI obj_end = vf.createURI(litdupsNS, "obj_stop");
//
//        Value subj = new RangeValue(subj_start, subj_end);
//        Value obj = new RangeValue(obj_start, obj_end);
//
//        //so - s should be the range
//        Map.Entry<TABLE_LAYOUT, Range> entry =
//                factory.defineRange(subj, null, obj_start, conf);
//        assertEquals(TABLE_LAYOUT.OSP, entry.getKey());
//        String expected_start = URI_MARKER_STR + obj_start.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + subj_start.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        String expected_end = URI_MARKER_STR + obj_start.stringValue() + DELIM_BYTES_STR +
//                URI_MARKER_STR + subj_end.stringValue();
//        assertEquals(expected_end + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//
//        //o - o is range
//        entry = factory.defineRange(null, null, obj, conf);
//        assertEquals(TABLE_LAYOUT.OSP, entry.getKey());
//        expected_start = URI_MARKER_STR + obj_start.stringValue();
//        assertEquals(expected_start,
//                entry.getValue().getStartKey().getRow().toString());
//        expected_end = URI_MARKER_STR + obj_end.stringValue();
//        assertEquals(expected_end + DELIM_STOP + RANGE_ENDKEY_SUFFIX,
//                entry.getValue().getEndKey().getRow().toString());
//    }
//
//}
