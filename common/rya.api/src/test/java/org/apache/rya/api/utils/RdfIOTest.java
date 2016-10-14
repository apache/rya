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

//package org.apache.rya.api.utils;

//
//import junit.framework.TestCase;
//import org.apache.rya.api.RdfCloudTripleStoreUtils;
//import org.openrdf.model.Statement;
//import org.openrdf.model.impl.StatementImpl;
//
//import static org.apache.rya.api.RdfCloudTripleStoreConstants.*;
//
///**
// * Class RdfIOTest
// * Date: Mar 8, 2012
// * Time: 10:12:00 PM
// */
//public class RdfIOTest extends TestCase {
//
//    Statement st = new StatementImpl(RTS_SUBJECT, RTS_VERSION_PREDICATE, VERSION);
//    int num = 100000;
//
//    public void testPerf() throws Exception {
//
//        long start = System.currentTimeMillis();
//        for(int i = 0; i < num; i++) {
//              byte[] bytes = RdfCloudTripleStoreUtils.writeValue(st.getSubject());
////            byte[] bytes = RdfIO.writeStatement(st);
////            Statement retSt = RdfIO.readStatement(ByteStreams.newDataInput(bytes), VALUE_FACTORY);
//        }
//        long dur = System.currentTimeMillis() - start;
//        System.out.println("RdfCloudTripleStoreUtils: " + dur);
//
//
//    }
//
//    public void testPerf2() throws Exception {
//        long start = System.currentTimeMillis();
//        for(int i = 0; i < num; i++) {
//            byte[] bytes = RdfIO.writeValue(st.getSubject());
//
////            byte[] bytes = RdfCloudTripleStoreUtils.buildRowWith(RdfCloudTripleStoreUtils.writeValue(st.getSubject()),
////                    RdfCloudTripleStoreUtils.writeValue(st.getPredicate()),
////                    RdfCloudTripleStoreUtils.writeValue(st.getObject()));
////            Statement retSt = RdfCloudTripleStoreUtils.translateStatementFromRow(ByteStreams.newDataInput(bytes), TABLE_LAYOUT.SPO, VALUE_FACTORY);
//        }
//        long dur = System.currentTimeMillis() - start;
//        System.out.println("RdfIO: " + dur);
//    }
//}
