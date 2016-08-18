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
import java.util.ArrayList;
import java.util.List;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.spark.graphx.Edge;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GraphXEdgeInputFormatTest {

    static String username = "root", table = "rya_spo";
    static PasswordToken password = new PasswordToken("");

    static Instance instance;
    static AccumuloRyaDAO apiImpl;

    @Before
    public void init() throws Exception {
        instance = new MockInstance(GraphXEdgeInputFormatTest.class.getName() + ".mock_instance");
        Connector connector = instance.getConnector(username, password);
        connector.tableOperations().create(table);

        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix("rya_");
        conf.setDisplayQueryPlan(false);

        apiImpl = new AccumuloRyaDAO();
        apiImpl.setConf(conf);
        apiImpl.setConnector(connector);
        apiImpl.init();
    }

    @After
    public void after() throws Exception {
        apiImpl.dropAndDestroy();
    }

    @SuppressWarnings("rawtypes")
	@Test
    public void testInputFormat() throws Exception {
    	RyaStatement input = RyaStatement.builder()
            .setSubject(new RyaURI("http://www.google.com"))
            .setPredicate(new RyaURI("http://some_other_uri"))
            .setObject(new RyaURI("http://www.yahoo.com"))
            .setColumnVisibility(new byte[0])
            .setValue(new byte[0])
            .build();

        apiImpl.add(input);

        Job jobConf = Job.getInstance();

        GraphXEdgeInputFormat.setMockInstance(jobConf, instance.getInstanceName());
        GraphXEdgeInputFormat.setConnectorInfo(jobConf, username, password);
        GraphXEdgeInputFormat.setTableLayout(jobConf, TABLE_LAYOUT.SPO);
        GraphXEdgeInputFormat.setInputTableName(jobConf, table);
        GraphXEdgeInputFormat.setInputTableName(jobConf, table);

        GraphXEdgeInputFormat.setScanIsolation(jobConf, false);
        GraphXEdgeInputFormat.setLocalIterators(jobConf, false);
        GraphXEdgeInputFormat.setOfflineTableScan(jobConf, false);

        GraphXEdgeInputFormat inputFormat = new GraphXEdgeInputFormat();

        JobContext context = new JobContextImpl(jobConf.getConfiguration(), jobConf.getJobID());

        List<InputSplit> splits = inputFormat.getSplits(context);

        Assert.assertEquals(1, splits.size());

        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(context.getConfiguration(), new TaskAttemptID(new TaskID(), 1));

        RecordReader reader = inputFormat.createRecordReader(splits.get(0), taskAttemptContext);

        RecordReader ryaStatementRecordReader = (RecordReader) reader;
        ryaStatementRecordReader.initialize(splits.get(0), taskAttemptContext);

        List<Edge> results = new ArrayList<Edge>();
        while(ryaStatementRecordReader.nextKeyValue()) {
            Edge writable = (Edge) ryaStatementRecordReader.getCurrentValue();
            long srcId = writable.srcId();
            long destId = writable.dstId();
			RyaTypeWritable rtw = null;
            Object text = ryaStatementRecordReader.getCurrentKey();
            Edge<RyaTypeWritable> edge = new Edge<RyaTypeWritable>(srcId, destId, rtw);
            results.add(edge);

            System.out.println(text);
        }

        System.out.println(results.size());
        System.out.println(results);
        Assert.assertTrue(results.size() == 2);
    }
}
