package org.apache.rya.accumulo.mr.tools;

import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.openrdf.model.vocabulary.XMLSchema;

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



import junit.framework.TestCase;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.mr.TestUtils;
import org.apache.rya.accumulo.mr.tools.Upgrade322Tool;
import org.apache.rya.accumulo.query.AccumuloRyaQueryEngine;
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
public class Upgrade322ToolTest extends TestCase {

    private String user = "user";
    private String pwd = "pwd";
    private String instance = Upgrade322ToolTest.class.getSimpleName() + ".myinstance";
    private String tablePrefix = "t_";
    private Authorizations auths = Constants.NO_AUTHS;
    private Connector connector;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        final String spoTable = tablePrefix +
                                RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX;
        final String poTable = tablePrefix +
                               RdfCloudTripleStoreConstants.TBL_PO_SUFFIX;
        final String ospTable = tablePrefix +
                                RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX;

        connector = new MockInstance(instance).getConnector(user, pwd.getBytes());

        connector.tableOperations().create(spoTable);
        connector.tableOperations().create(poTable);
        connector.tableOperations().create(ospTable);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
        connector.tableOperations().create(tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
        SecurityOperations secOps = connector.securityOperations();
        secOps.createUser(user, pwd.getBytes(), auths);
        secOps.grantTablePermission(user, spoTable, TablePermission.READ);
        secOps.grantTablePermission(user, poTable, TablePermission.READ);
        secOps.grantTablePermission(user, ospTable, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX, TablePermission.READ);
        secOps.grantTablePermission(user, tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX, TablePermission.WRITE);

        //load data
        final BatchWriter ospWriter = connector
          .createBatchWriter(ospTable, new BatchWriterConfig());
        ospWriter.addMutation(getMutation("00000000000000000010\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0000http://here/2010/tracked-data-provenance/ns#longLit\u0001\u0004"));
        ospWriter.addMutation(getMutation("00000000010\u0000http://here/2010/tracked-data-provenance/ns#uuid10" +
        "\u0000http://here/2010/tracked-data-provenance/ns#intLit\u0001\u0005"));
        ospWriter.addMutation(getMutation("00000010\u0000http://here/2010/tracked-data-provenance/ns#uuid10" +
        "\u0000http://here/2010/tracked-data-provenance/ns#byteLit\u0001\t"));
        ospWriter.addMutation(getMutation("00001 1.0\u0000http://here/2010/tracked-data-provenance/ns#uuid10" +
        "\u0000http://here/2010/tracked-data-provenance/ns#doubleLit\u0001\u0006"));
        ospWriter.addMutation(getMutation("10\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0000http" +
        "://here/2010/tracked-data-provenance/ns#shortLit\u0001http://www.w3" +
        ".org/2001/XMLSchema#short\u0001\b"));
        ospWriter.addMutation(getMutation("10.0\u0000http://here/2010/tracked-data-provenance/ns#uuid10" +
        "\u0000http://here/2010/tracked-data-provenance/ns#floatLit\u0001http" +
        "://www.w3.org/2001/XMLSchema#float\u0001\b"));
        ospWriter.addMutation(getMutation("3.0.0\u0000urn:org.apache.rya/2012/05#rts\u0000urn:org.apache" +
        ".rya/2012/05#version\u0001\u0003"));
        ospWriter.addMutation(getMutation("9223370726404375807\u0000http://here/2010/tracked-data-provenance/ns" +
        "#uuid10\u0000http://here/2010/tracked-data-provenance/ns#dateLit" +
        "\u0001\u0007"));
        ospWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#Created\u0000http://here" +
        "/2010/tracked-data-provenance/ns#uuid10\u0000http://www.w3" +
        ".org/1999/02/22-rdf-syntax-ns#type\u0001\u0002"));
        ospWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#objectuuid1\u0000http" +
        "://here/2010/tracked-data-provenance/ns#uuid10\u0000http://here/2010" +
        "/tracked-data-provenance/ns#uriLit\u0001\u0002"));
        ospWriter.addMutation(getMutation("stringLit\u0000http://here/2010/tracked-data-provenance/ns#uuid10" +
        "\u0000http://here/2010/tracked-data-provenance/ns#stringLit\u0001" +
        "\u0003"));
        ospWriter.addMutation(getMutation("true\u0000http://here/2010/tracked-data-provenance/ns#uuid10" +
        "\u0000http://here/2010/tracked-data-provenance/ns#booleanLit\u0001\n"));
        ospWriter.flush();
        ospWriter.close();

        final BatchWriter spoWriter = connector
          .createBatchWriter(spoTable, new BatchWriterConfig());
        spoWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#uuid10\u0000http://here/2010/tracked-data-provenance/ns#longLit\u000000000000000000000010\u0001\u0004"));
        spoWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#uuid10" +
                                          "\u0000http://here/2010/tracked-data-provenance/ns#intLit\u000000000000010\u0001\u0005"));
        spoWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#uuid10" +
                                          "\u0000http://here/2010/tracked-data-provenance/ns#byteLit\u000000000010\u0001\t"));
        spoWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#uuid10" +
                                          "\u0000http://here/2010/tracked-data-provenance/ns#doubleLit\u000000001 1.0\u0001\u0006"));
        spoWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#uuid10\u0000http" +
                                          "://here/2010/tracked-data-provenance/ns#shortLit\u000010\u0001http://www.w3" +
                                          ".org/2001/XMLSchema#short\u0001\b"));
        spoWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#uuid10" +
                                          "\u0000http://here/2010/tracked-data-provenance/ns#floatLit\u0001http" +
                                          "://www.w3.org/2001/XMLSchema#float\u000010.0\u0001\b"));
        spoWriter.addMutation(getMutation("urn:org.apache.rya/2012/05#rts\u0000urn:org.apache" +
                                          ".rya/2012/05#version\u00003.0.0\u0001\u0003"));
        spoWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns" +
                                          "#uuid10\u0000http://here/2010/tracked-data-provenance/ns#dateLit" +
                                          "\u00009223370726404375807\u0001\u0007"));
        spoWriter.addMutation(getMutation("http://here" +
                                          "/2010/tracked-data-provenance/ns#uuid10\u0000http://www.w3" +
                                          ".org/1999/02/22-rdf-syntax-ns#type\u0000http://here/2010/tracked-data-provenance/ns#Created\u0001\u0002"));
        spoWriter.addMutation(getMutation("http" +
                                          "://here/2010/tracked-data-provenance/ns#uuid10\u0000http://here/2010" +
                                          "/tracked-data-provenance/ns#uriLit\u0000http://here/2010/tracked-data-provenance/ns#objectuuid1\u0001\u0002"));
        spoWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#uuid10" +
                                          "\u0000http://here/2010/tracked-data-provenance/ns#stringLit\u0000stringLit\u0001" +
                                          "\u0003"));
        spoWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#uuid10" +
                                          "\u0000http://here/2010/tracked-data-provenance/ns#booleanLit\u0000true\u0001\n"));
        spoWriter.flush();
        spoWriter.close();

        final BatchWriter poWriter = connector
          .createBatchWriter(poTable, new BatchWriterConfig());
        poWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#longLit\u000000000000000000000010\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0001\u0004"));
        poWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#intLit\u000000000000010\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0001\u0005"));
        poWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#byteLit\u000000000010\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0001\t"));
        poWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#doubleLit\u000000001 1.0\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0001\u0006"));
        poWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#shortLit\u000010\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0001http://www.w3" +
                                          ".org/2001/XMLSchema#short\u0001\b"));
        poWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#floatLit\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0001http" +
                                          "://www.w3.org/2001/XMLSchema#float\u000010.0\u0001\b"));
        poWriter.addMutation(getMutation("urn:org.apache" +
                                          ".rya/2012/05#version\u00003.0.0\u0000urn:org.apache.rya/2012/05#rts\u0001\u0003"));
        poWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#dateLit" +
                                          "\u00009223370726404375807\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0001\u0007"));
        poWriter.addMutation(getMutation("http://www.w3" +
                                          ".org/1999/02/22-rdf-syntax-ns#type\u0000http://here/2010/tracked-data-provenance/ns#Created\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0001\u0002"));
        poWriter.addMutation(getMutation("http://here/2010" +
                                          "/tracked-data-provenance/ns#uriLit\u0000http://here/2010/tracked-data-provenance/ns#objectuuid1\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0001\u0002"));
        poWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#stringLit\u0000stringLit\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0001" +
                                          "\u0003"));
        poWriter.addMutation(getMutation("http://here/2010/tracked-data-provenance/ns#booleanLit\u0000true\u0000http://here/2010/tracked-data-provenance/ns#uuid10\u0001\n"));
        poWriter.flush();
        poWriter.close();
    }

    public Mutation getMutation(String row) {
        final Mutation mutation = new Mutation(row);
        mutation.put("", "", "");
        return mutation;
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        connector.tableOperations().delete(
          tablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);
        connector.tableOperations().delete(tablePrefix + RdfCloudTripleStoreConstants.TBL_NS_SUFFIX);
        connector.tableOperations().delete(
          tablePrefix + RdfCloudTripleStoreConstants.TBL_EVAL_SUFFIX);
    }

    public void testUpgrade() throws Exception {
        Upgrade322Tool.main(new String[]{
                "-Dac.mock=true",
                "-Dac.instance=" + instance,
                "-Dac.username=" + user,
                "-Dac.pwd=" + pwd,
                "-Drdf.tablePrefix=" + tablePrefix,
        });

        final AccumuloRdfConfiguration configuration = new AccumuloRdfConfiguration();
        configuration.setTablePrefix(tablePrefix);
        final AccumuloRyaDAO ryaDAO = new AccumuloRyaDAO();
        ryaDAO.setConnector(connector);
        ryaDAO.setConf(configuration);
        ryaDAO.init();

        final AccumuloRyaQueryEngine queryEngine = ryaDAO.getQueryEngine();

        TestUtils.verify(new RyaStatement(
          new RyaURI("http://here/2010/tracked-data-provenance/ns#uuid10"),
          new RyaURI("http://here/2010/tracked-data-provenance/ns#booleanLit"),
          new RyaType(XMLSchema.BOOLEAN, "true")), queryEngine);
        TestUtils.verify(new RyaStatement(
          new RyaURI("http://here/2010/tracked-data-provenance/ns#uuid10"),
          new RyaURI("http://here/2010/tracked-data-provenance/ns#longLit"),
          new RyaType(XMLSchema.LONG, "10")), queryEngine);
        TestUtils.verify(new RyaStatement(
          new RyaURI("http://here/2010/tracked-data-provenance/ns#uuid10"),
          new RyaURI("http://here/2010/tracked-data-provenance/ns#intLit"),
          new RyaType(XMLSchema.INTEGER, "10")), queryEngine);
        TestUtils.verify(new RyaStatement(
          new RyaURI("http://here/2010/tracked-data-provenance/ns#uuid10"),
          new RyaURI("http://here/2010/tracked-data-provenance/ns#byteLit"),
          new RyaType(XMLSchema.BYTE, "10")), queryEngine);
        TestUtils.verify(new RyaStatement(
          new RyaURI("http://here/2010/tracked-data-provenance/ns#uuid10"),
          new RyaURI("http://here/2010/tracked-data-provenance/ns#doubleLit"),
          new RyaType(XMLSchema.DOUBLE, "10.0")), queryEngine);
        TestUtils.verify(new RyaStatement(
          new RyaURI("http://here/2010/tracked-data-provenance/ns#uuid10"),
          new RyaURI("http://here/2010/tracked-data-provenance/ns#dateLit"),
          new RyaType(XMLSchema.DATETIME, "2011-07-12T06:00:00.000Z")), queryEngine);
        TestUtils.verify(new RyaStatement(
          new RyaURI("http://here/2010/tracked-data-provenance/ns#uuid10"),
          new RyaURI("http://here/2010/tracked-data-provenance/ns#stringLit"),
          new RyaType("stringLit")), queryEngine);
        TestUtils.verify(new RyaStatement(
          new RyaURI("http://here/2010/tracked-data-provenance/ns#uuid10"),
          new RyaURI("http://here/2010/tracked-data-provenance/ns#uriLit"),
          new RyaURI("http://here/2010/tracked-data-provenance/ns" +
                     "#objectuuid1")), queryEngine);
        TestUtils.verify(new RyaStatement(
          new RyaURI("urn:org.apache.rya/2012/05#rts"),
          new RyaURI("urn:org.apache.rya/2012/05#version"),
          new RyaType("3.0.0")), queryEngine);
    }

    public void printTableData(String tableName)
      throws TableNotFoundException{
        Scanner scanner = connector.createScanner(tableName, auths);
        scanner.setRange(new Range());
        for(Map.Entry<Key, Value> entry : scanner) {
            final Key key = entry.getKey();
            final Value value = entry.getValue();
            System.out.println(key.getRow() + " " + key.getColumnFamily() + " " + key.getColumnQualifier() + " " + key.getTimestamp() + " " + value.toString());
        }
    }

}
