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

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.EdgeReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.rya.accumulo.mr.RyaInputFormat.RyaStatementRecordReader;
import org.apache.rya.accumulo.mr.RyaStatementWritable;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.RyaTripleContext;

public class RyaEdgeReader extends EdgeReader<Text, RyaStatementWritable>{
    
    private RyaStatementRecordReader reader;
    private RyaTripleContext tripleContext;
    private TABLE_LAYOUT tableLayout;
    
    public RyaEdgeReader(RyaStatementRecordReader recordReader,
            TABLE_LAYOUT rdfTableLayout, RyaTripleContext tripleContext, Configuration conf){
        this.reader = recordReader;
        this.tableLayout = rdfTableLayout;
        this.tripleContext = tripleContext;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
       reader.initialize(inputSplit, context, tripleContext, tableLayout);       
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
       return reader.getProgress();
    }

    @Override
    public boolean nextEdge() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }

    @Override
    public Text getCurrentSourceId() throws IOException, InterruptedException {
        RyaStatementWritable currentStatement = reader.getCurrentValue();
        return new Text(currentStatement.getRyaStatement().getSubject().getData());
    }

    @Override
    public Edge<Text, RyaStatementWritable> getCurrentEdge() throws IOException, InterruptedException {
        RyaStatementWritable currentStatement = reader.getCurrentValue();
        RyaStatement ryaStatement = currentStatement.getRyaStatement();
        Edge<Text, RyaStatementWritable> edge = EdgeFactory.create(new Text(ryaStatement.toString()),
               currentStatement);
        return edge;
    }

}
