package org.apache.rya.joinselect.mr;

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



import java.io.IOException;

import org.apache.rya.joinselect.mr.FullTableSize;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Test;

//TODO fix table names!

public class FullTableSizeTest {

    private static final String DELIM = "\u0000";

    @Test
    public void testFullTableSize() throws IOException {

        Value value = new Value(new byte[0]);

        Mutation m = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
        m.put(new Text("FullTableCardinality"), new Text("15"), new Value(new byte[0]));

        new MapReduceDriver<Key, Value, Text, IntWritable, Text, Mutation>()
                .withMapper(new FullTableSize.FullTableMapper()).withInput(new Key(new Text("entry1")), value)
                .withInput(new Key(new Text("entry2")), value).withInput(new Key(new Text("entry3")), value)
                .withInput(new Key(new Text("entry4")), value).withInput(new Key(new Text("entry5")), value)
                .withInput(new Key(new Text("entry6")), value).withInput(new Key(new Text("entry7")), value)
                .withInput(new Key(new Text("entry8")), value).withInput(new Key(new Text("entry9")), value)
                .withInput(new Key(new Text("entry10")), value).withInput(new Key(new Text("entry11")), value)
                .withInput(new Key(new Text("entry12")), value).withInput(new Key(new Text("entry13")), value)
                .withInput(new Key(new Text("entry14")), value).withInput(new Key(new Text("entry15")), value)
                .withCombiner(new FullTableSize.FullTableCombiner()).withReducer(new FullTableSize.FullTableReducer())
                .withOutput(new Text(""), m).runTest();

    }

}
