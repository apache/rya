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
import java.util.ArrayList;
import java.util.List;

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
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.mr.GraphXInputFormat.RyaStatementRecordReader;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.utils.LiteralLanguageUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GraphXInputFormatTest {

    private final String username = "root", table = "rya_eci";
    private final PasswordToken password = new PasswordToken("");

    private Instance instance;
    private AccumuloRyaDAO apiImpl;

    @Before
    public void init() throws Exception {
        instance = new MockInstance(GraphXInputFormatTest.class.getName() + ".mock_instance");
        final Connector connector = instance.getConnector(username, password);
        connector.tableOperations().create(table);

        final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix("rya_");
        conf.setDisplayQueryPlan(false);
        conf.setBoolean("sc.use_entity", true);

        apiImpl = new AccumuloRyaDAO();
        apiImpl.setConf(conf);
        apiImpl.setConnector(connector);
        apiImpl.init();
    }

    @After
    public void after() throws Exception {
        apiImpl.dropAndDestroy();
    }

    @Test
    public void testInputFormat() throws Exception {
        final RyaStatement input = RyaStatement.builder()
            .setSubject(new RyaIRI("http://www.google.com"))
            .setPredicate(new RyaIRI("http://some_other_uri"))
            .setObject(new RyaIRI("http://www.yahoo.com"))
            .setColumnVisibility(new byte[0])
            .setValue(new byte[0])
            .build();

        apiImpl.add(input);

        final Job jobConf = Job.getInstance();

        GraphXInputFormat.setMockInstance(jobConf, instance.getInstanceName());
        GraphXInputFormat.setConnectorInfo(jobConf, username, password);
        GraphXInputFormat.setInputTableName(jobConf, table);
        GraphXInputFormat.setInputTableName(jobConf, table);

        GraphXInputFormat.setScanIsolation(jobConf, false);
        GraphXInputFormat.setLocalIterators(jobConf, false);
        GraphXInputFormat.setOfflineTableScan(jobConf, false);

        final GraphXInputFormat inputFormat = new GraphXInputFormat();

        final JobContext context = new JobContextImpl(jobConf.getConfiguration(), jobConf.getJobID());

        final List<InputSplit> splits = inputFormat.getSplits(context);

        Assert.assertEquals(1, splits.size());

        final TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(context.getConfiguration(), new TaskAttemptID(new TaskID(), 1));

        final RecordReader<Object, RyaTypeWritable> reader = inputFormat.createRecordReader(splits.get(0), taskAttemptContext);

        final RyaStatementRecordReader ryaStatementRecordReader = (RyaStatementRecordReader)reader;
        ryaStatementRecordReader.initialize(splits.get(0), taskAttemptContext);

        final List<RyaType> results = new ArrayList<RyaType>();
        System.out.println("before while");
        while(ryaStatementRecordReader.nextKeyValue()) {
            System.out.println("in while");
            final RyaTypeWritable writable = ryaStatementRecordReader.getCurrentValue();
            final RyaType value = writable.getRyaType();
            final Object text = ryaStatementRecordReader.getCurrentKey();
            final RyaType type = new RyaType();
            final String validatedLanguage = LiteralLanguageUtils.validateLanguage(value.getLanguage(), value.getDataType());
            type.setData(value.getData());
            type.setDataType(value.getDataType());
            type.setLanguage(validatedLanguage);
            results.add(type);

            System.out.println(value.getData());
            System.out.println(value.getDataType());
            System.out.println(results);
            System.out.println(type);
            System.out.println(text);
            System.out.println(value);
        }
        System.out.println("after while");

        System.out.println(results.size());
        System.out.println(results);
//        Assert.assertTrue(results.size() == 2);
//        Assert.assertTrue(results.contains(input));
    }
}
