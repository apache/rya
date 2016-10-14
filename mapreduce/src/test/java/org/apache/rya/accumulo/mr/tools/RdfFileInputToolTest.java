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
import org.junit.Test;
import org.openrdf.rio.RDFFormat;

import junit.framework.TestCase;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.mr.TestUtils;
import org.apache.rya.accumulo.mr.tools.RdfFileInputTool;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;

/**
 * Created by IntelliJ IDEA.
 * Date: 4/25/12
 * Time: 10:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class RdfFileInputToolTest extends TestCase {

    private String user = "user";
    private String pwd = "pwd";
    private String instance = RdfFileInputToolTest.class.getSimpleName() + ".myinstance";
    private String tablePrefix = "t_";
    private Authorizations auths = new Authorizations("test_auths");
    private Connector connector;

    @Override
    public void setUp() throws Exception {
        super.setUp();
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

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
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
        RyaStatement rs = new RyaStatement(new RyaURI("urn:lubm:rdfts#GraduateStudent01"),
                new RyaURI("urn:lubm:rdfts#hasFriend"),
                new RyaURI("urn:lubm:rdfts#GraduateStudent02"));
        rs.setColumnVisibility(auths.toString().getBytes());
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(tablePrefix);
        conf.setAuths(auths.toString());
        TestUtils.verify(connector, conf, rs);
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
        RyaStatement rs = new RyaStatement(new RyaURI("http://www.example.org/exampleDocument#Monica"),
                new RyaURI("http://www.example.org/vocabulary#name"),
                new RyaType("Monica Murphy"),
                new RyaURI("http://www.example.org/exampleDocument#G1"));
        rs.setColumnVisibility(auths.toString().getBytes());
        AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
        conf.setTablePrefix(tablePrefix);
        conf.setAuths(auths.toString());
        TestUtils.verify(connector, conf, rs);
    }
}