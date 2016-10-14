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
//import com.google.common.io.ByteArrayDataOutput;
//import com.google.common.io.ByteStreams;
//import org.apache.rya.api.RdfCloudTripleStoreUtils;
//import org.apache.rya.api.domain.RangeValue;
//import org.apache.accumulo.core.data.Range;
//import org.apache.hadoop.io.Text;
//import org.openrdf.model.Value;
//import org.openrdf.model.ValueFactory;
//import org.openrdf.model.impl.ValueFactoryImpl;
//
//import java.io.IOException;
//import java.util.Map;
//
//import static org.apache.rya.api.RdfCloudTripleStoreConstants.*;
//import static org.apache.rya.api.RdfCloudTripleStoreUtils.CustomEntry;
//
///**
// * Class DefineTripleQueryRangeFactory
// * Date: Jun 2, 2011
// * Time: 10:35:43 AM
// */
//public class DefineTripleQueryRangeFactory {
//
//    ValueFactory vf = ValueFactoryImpl.getInstance();
//
//    protected void fillRange(ByteArrayDataOutput startRowOut, ByteArrayDataOutput endRowOut, Value val, boolean empty)
//            throws IOException {
//        if(!empty) {
//            startRowOut.write(DELIM_BYTES);
//            endRowOut.write(DELIM_BYTES);
//        }
//        //null check?
//        if(val instanceof RangeValue) {
//            RangeValue rangeValue = (RangeValue) val;
//            Value start = rangeValue.getStart();
//            Value end = rangeValue.getEnd();
//            byte[] start_val_bytes = RdfCloudTripleStoreUtils.writeValue(start);
//            byte[] end_val_bytes = RdfCloudTripleStoreUtils.writeValue(end);
//            startRowOut.write(start_val_bytes);
//            endRowOut.write(end_val_bytes);
//        } else {
//            byte[] val_bytes = RdfCloudTripleStoreUtils.writeValue(val);
//            startRowOut.write(val_bytes);
//            endRowOut.write(val_bytes);
//        }
//    }
//
//    public Map.Entry<TABLE_LAYOUT, Range> defineRange(Value subject, Value predicate, Value object, AccumuloRdfConfiguration conf)
//            throws IOException {
//
//        byte[] startrow, stoprow;
//        ByteArrayDataOutput startRowOut = ByteStreams.newDataOutput();
//        ByteArrayDataOutput stopRowOut = ByteStreams.newDataOutput();
//        Range range;
//        TABLE_LAYOUT tableLayout;
//
//        if (subject != null) {
//            /**
//             * Case: s
//             * Table: spo
//             * Want this to be the first if statement since it will be most likely the most asked for table
//             */
//            tableLayout = TABLE_LAYOUT.SPO;
//            fillRange(startRowOut, stopRowOut, subject, true);
//            if (predicate != null) {
//                /**
//                 * Case: sp
//                 * Table: spo
//                 */
//                fillRange(startRowOut, stopRowOut, predicate, false);
//                if (object != null) {
//                    /**
//                     * Case: spo
//                     * Table: spo
//                     */
//                    fillRange(startRowOut, stopRowOut, object, false);
//                }
//            } else if (object != null) {
//                /**
//                 * Case: so
//                 * Table: osp
//                 * Very rare case. Could have put this in the OSP if clause, but I wanted to reorder the if statement
//                 * for best performance. The SPO table probably gets the most scans, so I want it to be the first if
//                 * statement in the branch.
//                 */
//                tableLayout = TABLE_LAYOUT.OSP;
//                startRowOut = ByteStreams.newDataOutput();
//                stopRowOut = ByteStreams.newDataOutput();
//                fillRange(startRowOut, stopRowOut, object, true);
//                fillRange(startRowOut, stopRowOut, subject, false);
//            }
//        } else if (predicate != null) {
//            /**
//             * Case: p
//             * Table: po
//             * Wanted this to be the second if statement, since it will be the second most asked for table
//             */
//            tableLayout = TABLE_LAYOUT.PO;
//            fillRange(startRowOut, stopRowOut, predicate, true);
//            if (object != null) {
//                /**
//                 * Case: po
//                 * Table: po
//                 */
//                fillRange(startRowOut, stopRowOut, object, false);
//            }
//        } else if (object != null) {
//            /**
//             * Case: o
//             * Table: osp
//             * Probably a pretty rare scenario
//             */
//            tableLayout = TABLE_LAYOUT.OSP;
//            fillRange(startRowOut, stopRowOut, object, true);
//        } else {
//            tableLayout = TABLE_LAYOUT.SPO;
//            stopRowOut.write(Byte.MAX_VALUE);
//        }
//
//        startrow = startRowOut.toByteArray();
//        stopRowOut.write(DELIM_STOP_BYTES);
//        stoprow = stopRowOut.toByteArray();
//        Text startRowTxt = new Text(startrow);
//        Text stopRowTxt = new Text(stoprow);
//        range = new Range(startRowTxt, stopRowTxt);
//
//        return new CustomEntry<TABLE_LAYOUT, Range>(tableLayout, range);
//    }
//
//}
