package org.apache.rya.rdftriplestore.evaluation;

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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.layout.TablePrefixLayoutStrategy;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.apache.rya.joinselect.AccumuloSelectivityEvalDAO;
import org.apache.rya.prospector.service.ProspectorServiceEvalStatsDAO;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class RdfCloudTripleStoreSelectivityEvaluationStatisticsTest {

    // TODO fix table names!!!

    private static final String DELIM = "\u0000";
    private final byte[] EMPTY_BYTE = new byte[0];
    private final Value EMPTY_VAL = new Value(EMPTY_BYTE);

    private String q1 = ""//
            + "SELECT ?h  " //
            + "{" //
            + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ."//
            + "  ?h <uri:barksAt> <uri:cat> ."//
            + "  ?h <uri:peesOn> <uri:hydrant> . "//
            + "}";//

    private Connector conn;
    AccumuloRdfConfiguration arc;
    BatchWriterConfig config;
    Instance mock;

    @Before
    public void init() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {

        mock = new MockInstance("accumulo");
        PasswordToken pToken = new PasswordToken("pass".getBytes());
        conn = mock.getConnector("user", pToken);

        config = new BatchWriterConfig();
        config.setMaxMemory(1000);
        config.setMaxLatency(1000, TimeUnit.SECONDS);
        config.setMaxWriteThreads(10);

        if (conn.tableOperations().exists("rya_prospects")) {
            conn.tableOperations().delete("rya_prospects");
        }
        if (conn.tableOperations().exists("rya_selectivity")) {
            conn.tableOperations().delete("rya_selectivity");
        }

        arc = new AccumuloRdfConfiguration();
        arc.setTableLayoutStrategy(new TablePrefixLayoutStrategy());
        arc.setMaxRangesForScanner(300);

    }

    @Test
    public void testOptimizeQ1() throws Exception {

        RdfEvalStatsDAO<RdfCloudTripleStoreConfiguration> res = new ProspectorServiceEvalStatsDAO(conn, arc);
        AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
        accc.setConf(arc);
        accc.setRdfEvalDAO(res);
        accc.setConnector(conn);
        accc.init();

        BatchWriter bw1 = conn.createBatchWriter("rya_prospects", config);
        BatchWriter bw2 = conn.createBatchWriter("rya_selectivity", config);

        String s1 = "predicateobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:dog";
        String s2 = "predicateobject" + DELIM + "uri:barksAt" + DELIM + "uri:cat";
        String s3 = "predicateobject" + DELIM + "uri:peesOn" + DELIM + "uri:hydrant";
        List<Mutation> mList = new ArrayList<Mutation>();
        List<Mutation> mList2 = new ArrayList<Mutation>();
        List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "predicateobject", "predicatepredicate",
                "predicatesubject");
        Mutation m1, m2, m3, m4;

        m1 = new Mutation(s1 + DELIM + "1");
        m1.put(new Text("count"), new Text(""), new Value("1".getBytes()));
        m2 = new Mutation(s2 + DELIM + "2");
        m2.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        m3 = new Mutation(s3 + DELIM + "3");
        m3.put(new Text("count"), new Text(""), new Value("3".getBytes()));
        mList.add(m1);
        mList.add(m2);
        mList.add(m3);

        bw1.addMutations(mList);
        bw1.close();

//        Scanner scan = conn.createScanner("rya_prospects", new Authorizations());
//        scan.setRange(new Range());

//        for (Map.Entry<Key, Value> entry : scan) {
//            System.out.println("Key row string is " + entry.getKey().getRow().toString());
//            System.out.println("Key is " + entry.getKey());
//            System.out.println("Value is " + (new String(entry.getValue().get())));
//        }

        m1 = new Mutation(s1);
        m2 = new Mutation(s2);
        m3 = new Mutation(s3);
        m4 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
        m4.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
        int i = 2;
        int j = 3;
        int k = 4;
        Long count1;
        Long count2;
        Long count3;

        for (String s : sList) {
            count1 = (long) i;
            count2 = (long) j;
            count3 = (long) k;
            m1.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
            m2.put(new Text(s), new Text(count2.toString()), EMPTY_VAL);
            m3.put(new Text(s), new Text(count3.toString()), EMPTY_VAL);
            i = 2 * i;
            j = 2 * j;
            k = 2 * k;
        }
        mList2.add(m1);
        mList2.add(m2);
        mList2.add(m3);
        mList2.add(m4);
        bw2.addMutations(mList2);
        bw2.close();

//        scan = conn.createScanner("rya_selectivity", new Authorizations());
//        scan.setRange(new Range());

//        for (Map.Entry<Key, Value> entry : scan) {
//            System.out.println("Key row string is " + entry.getKey().getRow().toString());
//            System.out.println("Key is " + entry.getKey());
//            System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));
//
//        }

        TupleExpr te = getTupleExpr(q1);
        System.out.println(te);

        RdfCloudTripleStoreSelectivityEvaluationStatistics ars = new RdfCloudTripleStoreSelectivityEvaluationStatistics(arc, res, accc);
        double card = ars.getCardinality(te);

        Assert.assertEquals(6.3136, card, .0001);

    }

    @Test
    public void testOptimizeQ1ZeroCard() throws Exception {

        RdfEvalStatsDAO<RdfCloudTripleStoreConfiguration> res = new ProspectorServiceEvalStatsDAO(conn, arc);
        AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
        accc.setConf(arc);
        accc.setConnector(conn);
        accc.setRdfEvalDAO(res);
        accc.init();

        BatchWriter bw1 = conn.createBatchWriter("rya_prospects", config);
        BatchWriter bw2 = conn.createBatchWriter("rya_selectivity", config);

        String s1 = "predicateobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:dog";
        String s2 = "predicateobject" + DELIM + "uri:barksAt" + DELIM + "uri:cat";
        String s3 = "predicateobject" + DELIM + "uri:peesOn" + DELIM + "uri:hydrant";
        List<Mutation> mList = new ArrayList<Mutation>();
        List<Mutation> mList2 = new ArrayList<Mutation>();
        List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "predicateobject", "predicatepredicate",
                "predicatesubject");
        Mutation m1, m2, m3, m4;

        m1 = new Mutation(s1 + DELIM + "1");
        m1.put(new Text("count"), new Text(""), new Value("1".getBytes()));
        m2 = new Mutation(s2 + DELIM + "2");
        m2.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        // m3 = new Mutation(s3 + DELIM + "3");
        // m3.put(new Text("count"), new Text(""), new Value("3".getBytes()));
        mList.add(m1);
        mList.add(m2);
        // mList.add(m3);

        bw1.addMutations(mList);
        bw1.close();

//        Scanner scan = conn.createScanner("rya_prospects", new Authorizations());
//        scan.setRange(new Range());

//        for (Map.Entry<Key, Value> entry : scan) {
//            System.out.println("Key row string is " + entry.getKey().getRow().toString());
//            System.out.println("Key is " + entry.getKey());
//            System.out.println("Value is " + (new String(entry.getValue().get())));
//        }

        m1 = new Mutation(s1);
        m2 = new Mutation(s2);
        m3 = new Mutation(s3);
        m4 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
        m4.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
        int i = 2;
        int j = 3;
        int k = 4;
        Long count1;
        Long count2;
        Long count3;

        for (String s : sList) {
            count1 = (long) i;
            count2 = (long) j;
            count3 = (long) k;
            m1.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
            m2.put(new Text(s), new Text(count2.toString()), EMPTY_VAL);
            m3.put(new Text(s), new Text(count3.toString()), EMPTY_VAL);
            i = 2 * i;
            j = 2 * j;
            k = 2 * k;
        }
        mList2.add(m1);
        mList2.add(m2);
        mList2.add(m3);
        mList2.add(m4);
        bw2.addMutations(mList2);
        bw2.close();

//        scan = conn.createScanner("rya_selectivity", new Authorizations());
//        scan.setRange(new Range());

//        for (Map.Entry<Key, Value> entry : scan) {
//            System.out.println("Key row string is " + entry.getKey().getRow().toString());
//            System.out.println("Key is " + entry.getKey());
//            System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));
//
//        }

        TupleExpr te = getTupleExpr(q1);
        System.out.println(te);

        RdfCloudTripleStoreSelectivityEvaluationStatistics ars = new RdfCloudTripleStoreSelectivityEvaluationStatistics(arc, res, accc);
        double card = ars.getCardinality(te);

        Assert.assertEquals(4.04, card, .0001);

    }

    private TupleExpr getTupleExpr(String query) throws MalformedQueryException {

        SPARQLParser sp = new SPARQLParser();
        ParsedQuery pq = sp.parseQuery(query, null);

        return pq.getTupleExpr();
    }

}
