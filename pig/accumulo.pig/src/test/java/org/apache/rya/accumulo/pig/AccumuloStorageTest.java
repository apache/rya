package org.apache.rya.accumulo.pig;

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
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.pig.data.Tuple;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/20/12
 * Time: 10:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class AccumuloStorageTest extends TestCase {

    private String user = "user";
    private String pwd = "pwd";
    private String instance = "myinstance";
    private String table = "testTable";
    private Authorizations auths = Constants.NO_AUTHS;
    private Connector connector;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        connector = new MockInstance(instance).getConnector(user, new PasswordToken(pwd.getBytes()));
        connector.tableOperations().create(table);
        SecurityOperations secOps = connector.securityOperations();
        secOps.createLocalUser(user, new PasswordToken(pwd.getBytes()));
        secOps.grantTablePermission(user, table, TablePermission.READ);
        secOps.grantTablePermission(user, table, TablePermission.WRITE);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        connector.tableOperations().delete(table);
    }

    public void testSimpleOutput() throws Exception {
        BatchWriter batchWriter = connector.createBatchWriter(table, 10l, 10l, 2);
        Mutation row = new Mutation("row");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        batchWriter.flush();
        batchWriter.close();

        String location = "accumulo://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a|z&mock=true";
        AccumuloStorage storage = createAccumuloStorage(location);
        int count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(1, count);
    }

    public void testRange() throws Exception {
        BatchWriter batchWriter = connector.createBatchWriter(table, 10l, 10l, 2);
        Mutation row = new Mutation("a");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        row = new Mutation("b");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        row = new Mutation("d");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        batchWriter.flush();
        batchWriter.close();

        String location = "accumulo://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a|c&mock=true";
        AccumuloStorage storage = createAccumuloStorage(location);
        int count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(2, count);
    }

    public void testMultipleRanges() throws Exception {
        BatchWriter batchWriter = connector.createBatchWriter(table, 10l, 10l, 2);
        Mutation row = new Mutation("a");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        row = new Mutation("b");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        row = new Mutation("d");
        row.put("cf", "cq", new Value(new byte[0]));
        batchWriter.addMutation(row);
        batchWriter.flush();
        batchWriter.close();

        String location = "accumulo://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a|c&range=d|e&mock=true";
        List<AccumuloStorage> storages = createAccumuloStorages(location);
        assertEquals(2, storages.size());
        AccumuloStorage storage = storages.get(0);
        int count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(2, count);
        storage = storages.get(1);
        count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(1, count);
    }

    public void testColumns() throws Exception {
        BatchWriter batchWriter = connector.createBatchWriter(table, 10l, 10l, 2);
        Mutation row = new Mutation("a");
        row.put("cf1", "cq", new Value(new byte[0]));
        row.put("cf2", "cq", new Value(new byte[0]));
        row.put("cf3", "cq1", new Value(new byte[0]));
        row.put("cf3", "cq2", new Value(new byte[0]));
        batchWriter.addMutation(row);
        batchWriter.flush();
        batchWriter.close();

        String location = "accumulo://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a|c&columns=cf1,cf3|cq1&mock=true";
        AccumuloStorage storage = createAccumuloStorage(location);
        int count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(2, count);
    }

    public void testWholeRowRange() throws Exception {
        BatchWriter batchWriter = connector.createBatchWriter(table, 10l, 10l, 2);
        Mutation row = new Mutation("a");
        row.put("cf1", "cq", new Value(new byte[0]));
        row.put("cf2", "cq", new Value(new byte[0]));
        row.put("cf3", "cq1", new Value(new byte[0]));
        row.put("cf3", "cq2", new Value(new byte[0]));
        batchWriter.addMutation(row);
        batchWriter.flush();
        batchWriter.close();

        String location = "accumulo://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a&mock=true";
        AccumuloStorage storage = createAccumuloStorage(location);
        int count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(4, count);
    }

    public void testAuths() throws Exception {
        BatchWriter batchWriter = connector.createBatchWriter(table, 10l, 10l, 2);
        Mutation row = new Mutation("a");
        row.put("cf1", "cq1", new ColumnVisibility("A"), new Value(new byte[0]));
        row.put("cf2", "cq2", new Value(new byte[0]));
        batchWriter.addMutation(row);
        batchWriter.flush();
        batchWriter.close();

        String location = "accumulo://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a|c&mock=true";
        AccumuloStorage storage = createAccumuloStorage(location);
        int count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(1, count);

        location = "accumulo://" + table + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&range=a|c&auths=A&mock=true";
        storage = createAccumuloStorage(location);
        count = 0;
        while (true) {
            Tuple next = storage.getNext();
            if (next == null)
                break;
            assertEquals(6, next.size());
            count++;
        }
        assertEquals(2, count);
    }

    protected AccumuloStorage createAccumuloStorage(String location) throws IOException, InterruptedException {
        List<AccumuloStorage> accumuloStorages = createAccumuloStorages(location);
        if (accumuloStorages.size() > 0) {
            return accumuloStorages.get(0);
        }
        return null;
    }

    protected List<AccumuloStorage> createAccumuloStorages(String location) throws IOException, InterruptedException {
        List<AccumuloStorage> accumuloStorages = new ArrayList<AccumuloStorage>();
        AccumuloStorage storage = new AccumuloStorage();
        InputFormat inputFormat = storage.getInputFormat();
        Job job = new Job(new Configuration());
        storage.setLocation(location, job);
        List<InputSplit> splits = inputFormat.getSplits(job);
        assertNotNull(splits);

        for (InputSplit inputSplit : splits) {
            storage = new AccumuloStorage();
            job = new Job(new Configuration());
            storage.setLocation(location, job);
            TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(job.getConfiguration(),
                    new TaskAttemptID("jtid", 0, false, 0, 0));
            RecordReader recordReader = inputFormat.createRecordReader(inputSplit,
                    taskAttemptContext);
            recordReader.initialize(inputSplit, taskAttemptContext);

            storage.prepareToRead(recordReader, null);
            accumuloStorages.add(storage);
        }
        return accumuloStorages;
    }
}
