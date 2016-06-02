package mvm.rya.accumulo.mr;
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
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat;
import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.mr.utils.MRUtils;
import mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RyaTripleContext;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolverException;

public class RyaStatementInputFormat extends AbstractInputFormat<Text, RyaStatementWritable> {
    @Override
    public RecordReader<Text, RyaStatementWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RyaStatementRecordReader();
    }


    public static void setTableLayout(Job conf, TABLE_LAYOUT layout) {
        conf.getConfiguration().set(MRUtils.TABLE_LAYOUT_PROP, layout.name());
    }

    public class RyaStatementRecordReader extends AbstractRecordReader<Text, RyaStatementWritable> {

        private RyaTripleContext ryaContext;
        private TABLE_LAYOUT tableLayout;

        @Override
        protected void setupIterators(TaskAttemptContext context, Scanner scanner, String tableName, RangeInputSplit split) {

        }

        @Override
        public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
            super.initialize(inSplit, attempt);
            this.tableLayout = TABLE_LAYOUT.valueOf(attempt.getConfiguration().get(MRUtils.TABLE_LAYOUT_PROP, TABLE_LAYOUT.OSP.toString()));
            //TODO verify that this is correct
            this.ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(attempt.getConfiguration()));
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (!scannerIterator.hasNext())
                return false;

            Entry<Key, Value> entry = scannerIterator.next();
            ++numKeysRead;
            currentKey = entry.getKey();

            try {
                currentK = currentKey.getRow();
                RyaStatement stmt = this.ryaContext.deserializeTriple(this.tableLayout, new TripleRow(entry.getKey().getRow().getBytes(), entry.getKey().getColumnFamily().getBytes(), entry.getKey().getColumnQualifier().getBytes(), entry.getKey().getTimestamp(), entry.getKey().getColumnVisibility().getBytes(), entry.getValue().get()));
                RyaStatementWritable writable = new RyaStatementWritable();
                writable.setRyaStatement(stmt);
                currentV = writable;
            } catch(TripleRowResolverException e) {
                throw new IOException(e);
            }
            return true;
        }

    }

}