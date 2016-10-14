package org.apache.rya.accumulo.mr;

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

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;

/**
 * Subclass of {@link AbstractInputFormat} for reading
 * {@link RyaStatementWritable}s directly from a running Rya instance.
 */
public class RyaInputFormat extends AbstractInputFormat<Text, RyaStatementWritable> {
    /**
     * Instantiates a RecordReader for this InputFormat and a given task and
     * input split.
     * @param   split   Defines the portion of the input this RecordReader is
     *                  responsible for.
     * @param   context     The context of the task.
     * @return A RecordReader that can be used to fetch RyaStatementWritables.
     */
    @Override
    public RecordReader<Text, RyaStatementWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new RyaStatementRecordReader();
    }

    /**
     * Sets the table layout to use.
     * @param conf  Configuration to set the layout in.
     * @param layout    Statements will be read from the Rya table associated
     *                  with this layout.
     */
    public static void setTableLayout(Job conf, TABLE_LAYOUT layout) {
        conf.getConfiguration().set(MRUtils.TABLE_LAYOUT_PROP, layout.name());
    }

    /**
     * Retrieves RyaStatementWritable objects from Accumulo tables.
     */
    public class RyaStatementRecordReader extends AbstractRecordReader<Text, RyaStatementWritable> {
        private RyaTripleContext ryaContext;
        private TABLE_LAYOUT tableLayout;

        @Override
        protected void setupIterators(TaskAttemptContext context, Scanner scanner, String tableName,
                RangeInputSplit split) {
        }

        /**
         * Initializes the RecordReader.
         * @param   inSplit Defines the portion of data to read.
         * @param   attempt Context for this task attempt.
         * @throws IOException if thrown by the superclass's initialize method.
         */
        @Override
        public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
            super.initialize(inSplit, attempt);
            this.tableLayout = MRUtils.getTableLayout(attempt.getConfiguration(), TABLE_LAYOUT.OSP);
            //TODO verify that this is correct
            this.ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(attempt.getConfiguration()));
        }

        /**
         * Load the next statement by converting the next Accumulo row to a
         * statement, and make the new (key,value) pair available for retrieval.
         * @return true if another (key,value) pair was fetched and is ready to
         *          be retrieved, false if there was none.
         * @throws  IOException if a row was loaded but could not be converted
         *          to a statement.
         */
        @Override
        public boolean nextKeyValue() throws IOException {
            if (!scannerIterator.hasNext())
                return false;
            Entry<Key, Value> entry = scannerIterator.next();
            ++numKeysRead;
            currentKey = entry.getKey();
            try {
                currentK = currentKey.getRow();
                RyaStatement stmt = this.ryaContext.deserializeTriple(this.tableLayout,
                        new TripleRow(entry.getKey().getRow().getBytes(),
                                entry.getKey().getColumnFamily().getBytes(),
                                entry.getKey().getColumnQualifier().getBytes(),
                                entry.getKey().getTimestamp(),
                                entry.getKey().getColumnVisibility().getBytes(),
                                entry.getValue().get()));
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
