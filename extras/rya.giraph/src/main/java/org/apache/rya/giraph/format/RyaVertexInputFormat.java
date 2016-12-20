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

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.accumulo.AccumuloVertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.accumulo.mr.RyaInputFormat;
import org.apache.rya.accumulo.mr.RyaInputFormat.RyaStatementRecordReader;
import org.apache.rya.accumulo.mr.RyaStatementWritable;
import org.apache.rya.accumulo.mr.RyaTypeWritable;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.resolver.RyaTripleContext;

public class RyaVertexInputFormat extends AccumuloVertexInputFormat<Text, RyaTypeWritable, RyaStatementWritable> {


    private RyaInputFormat ryaInputFormat = new RyaInputFormat();
    private TABLE_LAYOUT rdfTableLayout;
    private RyaTripleContext tripleContext;
    
    @Override
    public VertexReader<Text, RyaTypeWritable, RyaStatementWritable> createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
         return new RyaVertexReader((RyaStatementRecordReader) ryaInputFormat.createRecordReader(split, context),
                rdfTableLayout, tripleContext,
                context.getConfiguration());
    }

    @Override
    public void checkInputSpecs(Configuration conf) {
        // don't need to do anything here
    }

    @Override
    public List<InputSplit> getSplits(JobContext context, int minSplitCountHint) throws IOException, InterruptedException {
        return ryaInputFormat.getSplits(context);
    }

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration<Text, RyaTypeWritable, RyaStatementWritable> conf) {
        super.setConf(conf);
        RyaGiraphUtils.initializeAccumuloInputFormat(conf);
        rdfTableLayout = MRUtils.getTableLayout(conf, TABLE_LAYOUT.SPO);
        tripleContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(conf));

    }


}
