package mvm.rya.accumulo.mr.fileinput;

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



import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.RyaTripleContext;
import mvm.rya.api.resolver.triple.TripleRow;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.io.Text;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/25/12
 * Time: 10:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class RdfFileInputToolTest extends TestCase {

    private String user = "user";
    private String pwd = "pwd";
    private String instance = "myinstance";
    private String tablePrefix = "t_";
    private Authorizations auths = Constants.NO_AUTHS;
    private Connector connector;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        connector = new MockInstance(instance).getConnector(user, pwd.getBytes());
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
        SecurityOperations secOps = connector.securityOperations();
        secOps.createUser(user, pwd.getBytes(), auths);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX, TablePermission.WRITE);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
    }

    public void testNTriplesInput() throws Exception {
        RdfFileInputTool.main(new String[]{
                "-Dac.mock=true",
                "-Dac.instance=" + instance,
                "-Dac.username=" + user,
                "-Dac.pwd=" + pwd,
                "-Drdf.tablePrefix=" + tablePrefix,
                "-Drdf.format=" + RDFFormat.NTRIPLES.getName(),
                "src/test/resources/test.ntriples",
        });

        Scanner scanner = connector.createScanner(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, auths);
        scanner.setRange(new Range());
        Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
        ValueFactory vf = new ValueFactoryImpl();
        assertTrue(iterator.hasNext());
        RyaStatement rs = new RyaStatement(new RyaURI("urn:lubm:rdfts#GraduateStudent01"),
                new RyaURI("urn:lubm:rdfts#hasFriend"),
                new RyaURI("urn:lubm:rdfts#GraduateStudent02"));
        assertEquals(new Text(RyaTripleContext.getInstance(new AccumuloRdfConfiguration()).serializeTriple(rs).get(TABLE_LAYOUT.SPO).getRow()), iterator.next().getKey().getRow());
    }

    public void testInputContext() throws Exception {
        RdfFileInputTool.main(new String[]{
                "-Dac.mock=true",
                "-Dac.instance=" + instance,
                "-Dac.username=" + user,
                "-Dac.pwd=" + pwd,
                "-Drdf.tablePrefix=" + tablePrefix,
                "-Drdf.format=" + RDFFormat.TRIG.getName(),
                "src/test/resources/namedgraphs.trig",
        });

        Scanner scanner = connector.createScanner(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, auths);
        scanner.setRange(new Range());
        Iterator<Map.Entry<Key, Value>> iterator = scanner.iterator();
        ValueFactory vf = new ValueFactoryImpl();
        assertTrue(iterator.hasNext());
        RyaStatement rs = new RyaStatement(new RyaURI("http://www.example.org/exampleDocument#Monica"),
                new RyaURI("http://www.example.org/vocabulary#name"),
                new RyaType("Monica Murphy"),
                new RyaURI("http://www.example.org/exampleDocument#G1"));
        Key key = iterator.next().getKey();

        TripleRow tripleRow = RyaTripleContext.getInstance(new AccumuloRdfConfiguration()).serializeTriple(rs).get(TABLE_LAYOUT.SPO);
        assertEquals(new Text(tripleRow.getRow()), key.getRow());
        assertEquals(new Text(tripleRow.getColumnFamily()), key.getColumnFamily());
    }

}
