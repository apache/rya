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
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.AccumuloRyaDAO;
import mvm.rya.accumulo.RyaTableMutationsFactory;
import mvm.rya.accumulo.mr.RyaStatementInputFormat.RyaStatementRecordReader;
import mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RyaTripleContext;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class RyaInputFormatTest {

    static String username = "root", table = "rya_spo";
    static PasswordToken password = new PasswordToken("");

    static Instance instance;
    static AccumuloRyaDAO apiImpl;

    @BeforeClass
    public static void init() throws Exception {
        instance = new MockInstance("mock_instance");
        Connector connector = instance.getConnector(username, password);
        connector.tableOperations().create(table);

        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix("rya_");
        conf.setDisplayQueryPlan(false);

        apiImpl = new AccumuloRyaDAO();
        apiImpl.setConf(conf);
        apiImpl.setConnector(connector);
    }

    @Before
    public void before() throws Exception {
        apiImpl.init();
    }

    @After
    public void after() throws Exception {
        apiImpl.dropAndDestroy();
    }

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

        RyaStatementInputFormat.setMockInstance(jobConf, instance.getInstanceName());
        RyaStatementInputFormat.setConnectorInfo(jobConf, username, password);
        RyaStatementInputFormat.setTableLayout(jobConf, TABLE_LAYOUT.SPO);

        AccumuloInputFormat.setInputTableName(jobConf, table);
        AccumuloInputFormat.setInputTableName(jobConf, table);
        AccumuloInputFormat.setScanIsolation(jobConf, false);
        AccumuloInputFormat.setLocalIterators(jobConf, false);
        AccumuloInputFormat.setOfflineTableScan(jobConf, false);

        RyaStatementInputFormat inputFormat = new RyaStatementInputFormat();

        JobContext context = new JobContextImpl(jobConf.getConfiguration(), jobConf.getJobID());

        List<InputSplit> splits = inputFormat.getSplits(context);

        Assert.assertEquals(1, splits.size());

        TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(context.getConfiguration(), new TaskAttemptID(new TaskID(), 1));

        RecordReader reader = inputFormat.createRecordReader(splits.get(0), taskAttemptContext);

        RyaStatementRecordReader ryaStatementRecordReader = (RyaStatementRecordReader)reader;
        ryaStatementRecordReader.initialize(splits.get(0), taskAttemptContext);

        List<RyaStatement> results = new ArrayList<RyaStatement>();
        while(ryaStatementRecordReader.nextKeyValue()) {
            RyaStatementWritable writable = ryaStatementRecordReader.getCurrentValue();
            RyaStatement value = writable.getRyaStatement();
            Text text = ryaStatementRecordReader.getCurrentKey();
            RyaStatement stmt = RyaStatement.builder()
                .setSubject(value.getSubject())
                .setPredicate(value.getPredicate())
                .setObject(value.getObject())
                .setContext(value.getContext())
                .setQualifier(value.getQualifer())
                .setColumnVisibility(value.getColumnVisibility())
                .setValue(value.getValue())
                .build();
            results.add(stmt);

            System.out.println(text);
            System.out.println(value);
        }

        Assert.assertTrue(results.size() == 2);
        Assert.assertTrue(results.contains(input));
    }

    @Test
    public void mapperTest() throws Exception {

        RyaStatement input = RyaStatement.builder()
            .setSubject(new RyaURI("http://www.google.com"))
            .setPredicate(new RyaURI("http://some_other_uri"))
            .setObject(new RyaURI("http://www.yahoo.com"))
            .setValue(new byte[0])
            .setTimestamp(0L)
            .build();

        RyaStatementWritable writable = new RyaStatementWritable();
        writable.setRyaStatement(input);

        RyaStatementMapper mapper = new RyaStatementMapper();
        MapDriver<Text, RyaStatementWritable, Text, Mutation> mapDriver = MapDriver.newMapDriver(mapper);

        RyaTripleContext context = RyaTripleContext.getInstance(new AccumuloRdfConfiguration());
        RyaTableMutationsFactory mutationsFactory = new RyaTableMutationsFactory(context);

        Map<TABLE_LAYOUT, Collection<Mutation>> mutations = mutationsFactory.serialize(input);

        mapDriver.withInput(new Text("sometext"), writable);

        for(TABLE_LAYOUT key : mutations.keySet()) {
            Collection<Mutation> mutationCollection = mutations.get(key);
            for(Mutation m : mutationCollection) {
                mapDriver.withOutput(new Text("rya_" + key.name().toLowerCase()), m);
            }
        }

        mapDriver.runTest(false);

    }

    @Test
    public void reducerTest() throws Exception {
        RyaStatement input = RyaStatement.builder()
                .setSubject(new RyaURI("http://www.google.com"))
                .setPredicate(new RyaURI("http://some_other_uri"))
                .setObject(new RyaURI("http://www.yahoo.com"))
                .setValue(new byte[0])
                .setTimestamp(0L)
                .build();

        RyaStatementWritable writable = new RyaStatementWritable();
        writable.setRyaStatement(input);

        RyaStatementReducer reducer = new RyaStatementReducer();
        ReduceDriver<WritableComparable, RyaStatementWritable, Text, Mutation> reduceDriver = ReduceDriver.newReduceDriver(reducer);

        RyaTripleContext context = RyaTripleContext.getInstance(new AccumuloRdfConfiguration());
        RyaTableMutationsFactory mutationsFactory = new RyaTableMutationsFactory(context);

        Map<TABLE_LAYOUT, Collection<Mutation>> mutations = mutationsFactory.serialize(input);

        reduceDriver.withInput(new Text("sometext"), Arrays.asList(writable));

        for(TABLE_LAYOUT key : mutations.keySet()) {
            Collection<Mutation> mutationCollection = mutations.get(key);
            for(Mutation m : mutationCollection) {
                reduceDriver.withOutput(new Text("rya_" + key.name().toLowerCase()), m);
            }
        }

        reduceDriver.runTest(false);
    }

}