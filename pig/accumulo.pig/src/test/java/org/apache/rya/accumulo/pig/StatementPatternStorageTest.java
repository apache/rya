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
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
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
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/20/12
 * Time: 5:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class StatementPatternStorageTest extends TestCase {

    private String user = "user";
    private String pwd = "pwd";
    private String instance = "myinstance";
    private String tablePrefix = "t_";
    private Authorizations auths = Constants.NO_AUTHS;
    private Connector connector;
    private AccumuloRyaDAO ryaDAO;
    private ValueFactory vf = new ValueFactoryImpl();
    private String namespace = "urn:test#";
    private AccumuloRdfConfiguration conf;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        connector = new MockInstance(instance).getConnector(user, pwd.getBytes());
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
        SecurityOperations secOps = connector.securityOperations();
        secOps.createUser(user, pwd.getBytes(), auths);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX, TablePermission.READ);

        conf = new AccumuloRdfConfiguration();
        ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        conf.setTablePrefix(tablePrefix);
        ryaDAO.setConf(conf);
        ryaDAO.init();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
    }

    public void testSimplePredicateRange() throws Exception {
        ryaDAO.add(new RyaStatement(new RyaURI(namespace, "a"),new RyaURI(namespace,"p"), new RyaType("l")));
        ryaDAO.add(new RyaStatement(new RyaURI(namespace, "b"), new RyaURI(namespace, "p"), new RyaType("l")));
        ryaDAO.add(new RyaStatement(new RyaURI(namespace, "c"), new RyaURI(namespace, "n"), new RyaType("l")));
        

        int count = 0;
        List<StatementPatternStorage> storages = createStorages("accumulo://" + tablePrefix + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&predicate=<" + namespace + "p>&mock=true");
        for (StatementPatternStorage storage : storages) {
            while (true) {
                Tuple next = storage.getNext();
                if (next == null) {
                    break;
                }
                count++;
            }
        }
        assertEquals(2, count);
        ryaDAO.destroy();
    }

    public void testContext() throws Exception {
        ryaDAO.add(new RyaStatement(new RyaURI(namespace, "a"), new RyaURI(namespace, "p"), new RyaType("l1")));
        ryaDAO.add(new RyaStatement(new RyaURI(namespace, "a"), new RyaURI(namespace, "p"), new RyaType("l2"), new RyaURI(namespace, "g1")));
        

        int count = 0;
        List<StatementPatternStorage> storages = createStorages("accumulo://" + tablePrefix + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&predicate=<" + namespace + "p>&mock=true");
        for (StatementPatternStorage storage : storages) {
            while (true) {
                Tuple next = storage.getNext();
                if (next == null) {
                    break;
                }
                count++;
            }
        }
        assertEquals(2, count);

        count = 0;
        storages = createStorages("accumulo://" + tablePrefix + "?instance=" + instance + "&user=" + user + "&password=" + pwd + "&predicate=<" + namespace + "p>&context=<"+namespace+"g1>&mock=true");
        for (StatementPatternStorage storage : storages) {
            while (true) {
                Tuple next = storage.getNext();
                if (next == null) {
                    break;
                }
                count++;
            }
        }
        assertEquals(1, count);

        ryaDAO.destroy();
    }

    protected List<StatementPatternStorage> createStorages(String location) throws IOException, InterruptedException {
        List<StatementPatternStorage> storages = new ArrayList<StatementPatternStorage>();
        StatementPatternStorage storage = new StatementPatternStorage();
        InputFormat inputFormat = storage.getInputFormat();
        Job job = new Job(new Configuration());
        storage.setLocation(location, job);
        List<InputSplit> splits = inputFormat.getSplits(job);
        assertNotNull(splits);

        for (InputSplit inputSplit : splits) {
            storage = new StatementPatternStorage();
            job = new Job(new Configuration());
            storage.setLocation(location, job);
            TaskAttemptContext taskAttemptContext = new TaskAttemptContextImpl(job.getConfiguration(),
                    new TaskAttemptID("jtid", 0, false, 0, 0));
            RecordReader recordReader = inputFormat.createRecordReader(inputSplit,
                    taskAttemptContext);
            recordReader.initialize(inputSplit, taskAttemptContext);

            storage.prepareToRead(recordReader, null);
            storages.add(storage);
        }
        return storages;
    }

}
