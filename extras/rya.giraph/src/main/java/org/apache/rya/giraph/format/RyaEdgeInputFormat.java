package org.apache.rya.giraph.format;
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
import java.util.List;

import org.apache.giraph.io.EdgeInputFormat;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.rya.accumulo.mr.RyaInputFormat;
import org.apache.rya.accumulo.mr.RyaInputFormat.RyaStatementRecordReader;
import org.apache.rya.accumulo.mr.RyaStatementWritable;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class RyaEdgeInputFormat extends EdgeInputFormat<Text, RyaStatementWritable> {

    private RyaInputFormat ryaInputFormat = new RyaInputFormat();
    private TABLE_LAYOUT rdfTableLayout;
    private RyaTripleContext tripleContext;
    
    @Override
    public EdgeReader<Text, RyaStatementWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new RyaEdgeReader((RyaStatementRecordReader) ryaInputFormat.createRecordReader(split, context),
                rdfTableLayout, tripleContext,
                context.getConfiguration());
    }

    @Override
    public void checkInputSpecs(Configuration arg0) {
        // nothing to do
        
    }

    @Override
    public List<InputSplit> getSplits(JobContext context, int arg1) throws IOException, InterruptedException {
       return ryaInputFormat.getSplits(context);
    }

}
