package org.apache.rya.accumulo.mr.tools;

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

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.mr.TestUtils;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/25/12
 * Time: 10:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class RdfFileInputToolTest {

    private String user = "user";
    private String pwd = "pwd";
    private String instance = RdfFileInputToolTest.class.getSimpleName() + ".myinstance";
    private String tablePrefix = "t_";
    private Authorizations auths = new Authorizations("test_auths");
    private Connector connector;

    @Before
    public void setUp() throws Exception {
        connector = new MockInstance(instance).getConnector(user, new PasswordToken(pwd));
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
        SecurityOperations secOps = connector.securityOperations();
        secOps.createLocalUser(user, new PasswordToken(pwd));
        secOps.changeUserAuthorizations(user, auths);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX, TablePermission.WRITE);
    }

    @After
    public void tearDown() throws Exception {
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
    }

    @Test
    public void testNTriplesInput() throws Exception {
        RdfFileInputTool.main(new String[]{
                "-Dac.mock=true",
                "-Dac.instance=" + instance,
                "-Dac.username=" + user,
                "-Dac.pwd=" + pwd,
                "-Dac.auth=" + auths.toString(),
                "-Dac.cv=" + auths.toString(),
                "-Drdf.tablePrefix=" + tablePrefix,
                "-Drdf.format=" + RDFFormat.NTRIPLES.getName(),
                "src/test/resources/test.ntriples",
        });
        RyaStatement rs = new RyaStatement(new RyaIRI("urn:lubm:rdfts#GraduateStudent01"),
                new RyaIRI("urn:lubm:rdfts#hasFriend"),
                new RyaIRI("urn:lubm:rdfts#GraduateStudent02"));
        rs.setColumnVisibility(auths.toString().getBytes());
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(tablePrefix);
        conf.setAuths(auths.toString());
        TestUtils.verify(connector, conf, rs);
    }

    @Test
    public void testMultipleNTriplesInputs() throws Exception {
        RdfFileInputTool.main(new String[]{
                "-Dac.mock=true",
                "-Dac.instance=" + instance,
                "-Dac.username=" + user,
                "-Dac.pwd=" + pwd,
                "-Dac.auth=" + auths.toString(),
                "-Dac.cv=" + auths.toString(),
                "-Drdf.tablePrefix=" + tablePrefix,
                "-Drdf.format=" + RDFFormat.NTRIPLES.getName(),
                "src/test/resources/test.ntriples,src/test/resources/test2.ntriples",
        });
        RyaStatement rs1 = new RyaStatement(new RyaIRI("urn:lubm:rdfts#GraduateStudent01"),
                new RyaIRI("urn:lubm:rdfts#hasFriend"),
                new RyaIRI("urn:lubm:rdfts#GraduateStudent02"));
        RyaStatement rs2 = new RyaStatement(new RyaIRI("urn:lubm:rdfts#GraduateStudent05"),
                new RyaIRI("urn:lubm:rdfts#hasFriend"),
                new RyaIRI("urn:lubm:rdfts#GraduateStudent07"));
        rs1.setColumnVisibility(auths.toString().getBytes());
        rs2.setColumnVisibility(auths.toString().getBytes());
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(tablePrefix);
        conf.setAuths(auths.toString());
        TestUtils.verify(connector, conf, rs1, rs2);
    }

    @Test
    public void testInputContext() throws Exception {
        RdfFileInputTool.main(new String[]{
                "-Dac.mock=true",
                "-Dac.instance=" + instance,
                "-Dac.username=" + user,
                "-Dac.pwd=" + pwd,
                "-Dac.auth=" + auths.toString(),
                "-Dac.cv=" + auths.toString(),
                "-Drdf.tablePrefix=" + tablePrefix,
                "-Drdf.format=" + RDFFormat.TRIG.getName(),
                "src/test/resources/namedgraphs.trig",
        });
        RyaStatement rs = new RyaStatement(new RyaIRI("http://www.example.org/exampleDocument#Monica"),
                new RyaIRI("http://www.example.org/vocabulary#name"),
                new RyaType("Monica Murphy"),
                new RyaIRI("http://www.example.org/exampleDocument#G1"));
        rs.setColumnVisibility(auths.toString().getBytes());
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(tablePrefix);
        conf.setAuths(auths.toString());
        TestUtils.verify(connector, conf, rs);
    }
}