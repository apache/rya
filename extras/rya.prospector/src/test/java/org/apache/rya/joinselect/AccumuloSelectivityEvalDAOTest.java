package org.apache.rya.joinselect;

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



import java.math.BigDecimal;
import java.math.MathContext;
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
import org.apache.accumulo.core.client.admin.TableOperations;
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
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Lists;

public class AccumuloSelectivityEvalDAOTest {

    private static final String DELIM = "\u0000";
    private final byte[] EMPTY_BYTE = new byte[0];
    private final Value EMPTY_VAL = new Value(EMPTY_BYTE);

    private String q1 = ""//
            + "SELECT ?h  " //
            + "{" //
            + "  ?h <uri:howlsAt>  <uri:moon>. "//
            + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ."//
            + "  ?h <uri:barksAt> <uri:cat> ."//
            + "  ?h <uri:peesOn> <uri:hydrant> . "//
            + "}";//

    private String q2 = ""//
            + "SELECT ?h  " //
            + "{" //
            + "   <uri:howlsAt> ?h  <uri:moon>. "//
            + "   <http://www.w3.org/2000/01/rdf-schema#label> ?h <uri:dog> ."//
            + "   <uri:barksAt> ?h <uri:cat> ."//
            + "   <uri:peesOn> ?h <uri:hydrant> . "//
            + "}";//

    private String q3 = ""//
            + "SELECT ?h  " //
            + "{" //
            + "   <uri:howlsAt> <uri:moon>  ?h. "//
            + "   <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ?h  ."//
            + "   <uri:barksAt> ?h <uri:cat> ."//
            + "   ?h <uri:peesOn> <uri:hydrant> . "//
            + "}";//

    private Connector conn;
    AccumuloRdfConfiguration arc;
    RdfEvalStatsDAO<RdfCloudTripleStoreConfiguration> res;
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
        res = new ProspectorServiceEvalStatsDAO(conn, arc);
        arc.setTableLayoutStrategy(new TablePrefixLayoutStrategy());
        arc.setMaxRangesForScanner(300);

    }

    @Test
    public void testInitialize() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {

        AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
        accc.setConf(arc);
        accc.setConnector(conn);
        accc.setRdfEvalDAO(res);
        accc.init();

        TableOperations tos = conn.tableOperations();
        Assert.assertTrue(tos.exists("rya_prospects") && tos.exists("rya_selectivity"));
        Assert.assertTrue(accc.isInitialized());
        Assert.assertTrue(accc.getConf().equals(arc));
        Assert.assertTrue(accc.getConnector().equals(conn));
        Assert.assertTrue(accc.getRdfEvalDAO().equals(res));

    }

    @Test
    public void testCardinalityQuery1() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException,
            MalformedQueryException {

        AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
        accc.setConf(arc);
        accc.setRdfEvalDAO(res);
        accc.setConnector(conn);
        accc.init();

        BatchWriter bw = conn.createBatchWriter("rya_prospects", config);
        
        BatchWriter bw1 = conn.createBatchWriter("rya_selectivity", config);
        Mutation m = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
        m.put(new Text("FullTableCardinality"), new Text("600"), EMPTY_VAL);
        List<Mutation> list = Lists.newArrayList();
        list.add(m);
        bw1.addMutations(list);
        bw1.close();

        String s1 = "predicateobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:dog";
        String s2 = "predicateobject" + DELIM + "uri:barksAt" + DELIM + "uri:cat";
        String s3 = "predicateobject" + DELIM + "uri:peesOn" + DELIM + "uri:hydrant";
        List<Mutation> mList = new ArrayList<Mutation>();
        Mutation m1, m2, m3;

        Integer tempInt;
        Integer tempInt2;

        for (int i = 1; i < 7; i++) {
            tempInt = 5 * i;
            tempInt2 = 10 - i;
            m1 = new Mutation(s1 + DELIM + i);
            m1.put(new Text("count"), new Text(""), new Value((tempInt.toString()).getBytes()));
            m2 = new Mutation(s2 + DELIM + (7 - i));
            m2.put(new Text("count"), new Text(""), new Value((tempInt.toString()).getBytes()));
            m3 = new Mutation(s3 + DELIM + (10 + i));
            m3.put(new Text("count"), new Text(""), new Value((tempInt2.toString()).getBytes()));
            mList.add(m1);
            mList.add(m2);
            mList.add(m3);
        }

        bw.addMutations(mList);
        bw.close();

        List<StatementPattern> spList = getSpList(q1);
        long c1 = accc.getCardinality(arc, spList.get(0));
        long c2 = accc.getCardinality(arc, spList.get(1));
        long c3 = accc.getCardinality(arc, spList.get(2));
        long c4 = accc.getCardinality(arc, spList.get(3));

        Assert.assertTrue(c1 == (long) 0);
        Assert.assertTrue(c2 == (long) 5);
        Assert.assertTrue(c3 == (long) 30);
        Assert.assertTrue(c4 == (long) 9);

    }

    @Test
    public void testCardinalityQuery2() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException,
            MalformedQueryException {

        AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
        accc.setConf(arc);
        accc.setConnector(conn);
        accc.setRdfEvalDAO(res);
        accc.init();

        BatchWriter bw = conn.createBatchWriter("rya_prospects", config);

        BatchWriter bw1 = conn.createBatchWriter("rya_selectivity", config);
        Mutation m = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
        m.put(new Text("FullTableCardinality"), new Text("600"), EMPTY_VAL);
        List<Mutation> list = Lists.newArrayList();
        list.add(m);
        bw1.addMutations(list);
        bw1.close();

        String s1 = "subjectobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:dog";
        String s2 = "subjectobject" + DELIM + "uri:barksAt" + DELIM + "uri:cat";
        String s3 = "subjectobject" + DELIM + "uri:peesOn" + DELIM + "uri:hydrant";
        List<Mutation> mList = new ArrayList<Mutation>();
        Mutation m1, m2, m3;

        Integer tempInt;
        Integer tempInt2;

        for (int i = 1; i < 7; i++) {
            tempInt = 5 * i;
            tempInt2 = 10 - i;
            m1 = new Mutation(s1 + DELIM + i);
            m1.put(new Text("count"), new Text(""), new Value((tempInt.toString()).getBytes()));
            m2 = new Mutation(s2 + DELIM + (7 - i));
            m2.put(new Text("count"), new Text(""), new Value((tempInt.toString()).getBytes()));
            m3 = new Mutation(s3 + DELIM + (10 + i));
            m3.put(new Text("count"), new Text(""), new Value((tempInt2.toString()).getBytes()));
            mList.add(m1);
            mList.add(m2);
            mList.add(m3);
        }
        bw.addMutations(mList);
        bw.close();

        List<StatementPattern> spList = getSpList(q2);
        long c1 = accc.getCardinality(arc, spList.get(0));
        long c2 = accc.getCardinality(arc, spList.get(1));
        long c3 = accc.getCardinality(arc, spList.get(2));
        long c4 = accc.getCardinality(arc, spList.get(3));

        Assert.assertTrue(c1 == (long) 0);
        Assert.assertTrue(c2 == (long) 5);
        Assert.assertTrue(c3 == (long) 30);
        Assert.assertTrue(c4 == (long) 9);

    }

    @Test
    public void testJoinCardinalityQuery1() throws AccumuloException, AccumuloSecurityException, TableExistsException,
            TableNotFoundException, MalformedQueryException {

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
        m1.put(new Text("count"), new Text(""), new Value("20".getBytes()));
        m2 = new Mutation(s2 + DELIM + "2");
        m2.put(new Text("count"), new Text(""), new Value("15".getBytes()));
        m3 = new Mutation(s3 + DELIM + "3");
        m3.put(new Text("count"), new Text(""), new Value("10".getBytes()));
        mList.add(m1);
        mList.add(m2);
        mList.add(m3);

        bw1.addMutations(mList);
        bw1.close();

        m1 = new Mutation(s1);
        m2 = new Mutation(s2);
        m3 = new Mutation(s3);
        int i = 30;
        int j = 60;
        int k = 90;
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
        m4 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
        m4.put(new Text("FullTableCardinality"), new Text("600"), EMPTY_VAL);
        mList2.add(m1);
        mList2.add(m2);
        mList2.add(m3);
        mList2.add(m4);
        bw2.addMutations(mList2);
        bw2.close();

        Scanner scan = conn.createScanner("rya_selectivity", new Authorizations());
        scan.setRange(new Range());

        for (Map.Entry<Key, Value> entry : scan) {
            System.out.println("Key row string is " + entry.getKey().getRow().toString());
            System.out.println("Key is " + entry.getKey());
            System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));

        }

        List<StatementPattern> spList = getSpList(q1);
        System.out.println(spList);
        List<Double> jCardList = new ArrayList<Double>();

        for (StatementPattern sp1 : spList) {
            for (StatementPattern sp2 : spList) {
                jCardList.add(accc.getJoinSelect(arc, sp1, sp2));
            }
        }

        System.out.println("Join cardinalities are " + jCardList);

        Assert.assertEquals(0, jCardList.get(0), .001);
        Assert.assertEquals(0, jCardList.get(3), .001);
        Assert.assertEquals(6.0 / 600, jCardList.get(5), .001);
        Assert.assertEquals(6.0 / 600, jCardList.get(6), .001);
        Assert.assertEquals(0 / 600, jCardList.get(8), .001);
        Assert.assertEquals(6.0 / 600, jCardList.get(7), .001);
        Assert.assertEquals(15.0 / 600, jCardList.get(11), .001);
        Assert.assertEquals(6.0 / 600, jCardList.get(13), .001);
        Assert.assertEquals(10.0 / 600, jCardList.get(15), .001);

        Assert.assertTrue(jCardList.get(0) == 0);
        Assert.assertTrue(jCardList.get(3) == 0);
        Assert.assertTrue(jCardList.get(5) == .01);
        Assert.assertTrue(jCardList.get(6) == .01);
        Assert.assertTrue(jCardList.get(8) == 0);
        Assert.assertTrue(jCardList.get(7) == (6.0 / 600));
        Assert.assertTrue(jCardList.get(11) == (1.0 / 40));
        Assert.assertTrue(jCardList.get(13) == .01);
        Assert.assertTrue(jCardList.get(15) == (10.0 / 600));

    }

    @Test
    public void testJoinCardinalityQuery2() throws AccumuloException, AccumuloSecurityException, TableExistsException,
            TableNotFoundException, MalformedQueryException {

        AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
        accc.setConf(arc);
        accc.setConnector(conn);
        accc.setRdfEvalDAO(res);
        accc.init();

        BatchWriter bw1 = conn.createBatchWriter("rya_prospects", config);
        BatchWriter bw2 = conn.createBatchWriter("rya_selectivity", config);

        String s1 = "subjectobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:dog";
        String s2 = "subjectobject" + DELIM + "uri:barksAt" + DELIM + "uri:cat";
        String s3 = "subjectobject" + DELIM + "uri:peesOn" + DELIM + "uri:hydrant";
        String s4 = "objectsubject" + DELIM + "uri:dog" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label";
        String s5 = "objectsubject" + DELIM + "uri:cat" + DELIM + "uri:barksAt";
        String s6 = "objectsubject" + DELIM + "uri:hydrant" + DELIM + "uri:peesOn";
        List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "predicateobject", "predicatepredicate",
                "predicatesubject");
        List<Mutation> mList = new ArrayList<Mutation>();
        List<Mutation> mList2 = new ArrayList<Mutation>();
        Mutation m1, m2, m3, m4;

        m1 = new Mutation(s1 + DELIM + "1");
        m1.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        m2 = new Mutation(s2 + DELIM + "2");
        m2.put(new Text("count"), new Text(""), new Value("4".getBytes()));
        m3 = new Mutation(s3 + DELIM + "3");
        m3.put(new Text("count"), new Text(""), new Value("6".getBytes()));
        mList.add(m1);
        mList.add(m2);
        mList.add(m3);

        bw1.addMutations(mList);
        bw1.close();

        m1 = new Mutation(s4);
        m2 = new Mutation(s5);
        m3 = new Mutation(s6);
        int i = 5;
        int j = 6;
        int k = 7;
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
        m4 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
        m4.put(new Text("FullTableCardinality"), new Text("600"), EMPTY_VAL);
        mList2.add(m1);
        mList2.add(m2);
        mList2.add(m3);
        mList2.add(m4);
        bw2.addMutations(mList2);
        bw2.close();

        List<StatementPattern> spList = getSpList(q2);
        // System.out.println(spList);
        List<Double> jCardList = new ArrayList<Double>();

        for (StatementPattern sp1 : spList) {
            for (StatementPattern sp2 : spList) {
                jCardList.add(accc.getJoinSelect(arc, sp1, sp2));
            }
        }

        System.out.println("Join cardinalities are " + jCardList);

        Assert.assertEquals(0, jCardList.get(0), .001);
        Assert.assertEquals(0, jCardList.get(3), .001);
        Assert.assertEquals(2.0 / 600, jCardList.get(5), .001);
        Assert.assertEquals(4.0 / 600, jCardList.get(6), .001);
        Assert.assertEquals(.0 / 600, jCardList.get(8), .001);
        Assert.assertEquals(6. / 600, jCardList.get(7), .001);
        Assert.assertEquals(6. / 600, jCardList.get(11), .001);

        Assert.assertTrue(jCardList.get(0) == 0);
        Assert.assertTrue(jCardList.get(3) == 0);
        Assert.assertTrue(jCardList.get(5) == (1.0 / 300));
        Assert.assertTrue(jCardList.get(6) == (4.0 / 600));
        Assert.assertTrue(jCardList.get(8) == 0);
        Assert.assertTrue(jCardList.get(7) == .01);
        Assert.assertTrue(jCardList.get(11) == .01);

    }

    @Test
    public void testJoinCardinalityQuery3() throws AccumuloException, AccumuloSecurityException, TableExistsException,
            TableNotFoundException, MalformedQueryException {

        AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
        accc.setConf(arc);
        accc.setConnector(conn);
        accc.setRdfEvalDAO(res);
        accc.init();

        BatchWriter bw1 = conn.createBatchWriter("rya_prospects", config);
        BatchWriter bw2 = conn.createBatchWriter("rya_selectivity", config);

        String s1 = "subjectpredicate" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:dog";
        String s2 = "subjectobject" + DELIM + "uri:barksAt" + DELIM + "uri:cat";
        String s3 = "predicateobject" + DELIM + "uri:peesOn" + DELIM + "uri:hydrant";
        String s4 = "subjectpredicate" + DELIM + "uri:howlsAt" + DELIM + "uri:moon";
        String s5 = "objectsubject" + DELIM + "uri:cat" + DELIM + "uri:barksAt";

        List<String> sList = Arrays.asList("subjectobject", "objectsubject", "objectobject", "objectpredicate", "subjectpredicate",
                "subjectsubject", "predicateobject", "predicatepredicate", "predicatesubject");
        List<Mutation> mList = new ArrayList<Mutation>();
        List<Mutation> mList2 = new ArrayList<Mutation>();
        Mutation m1, m2, m3, m4, m5, m6;

        m1 = new Mutation(s1 + DELIM + "1");
        m1.put(new Text("count"), new Text(""), new Value("15".getBytes()));
        m2 = new Mutation(s2 + DELIM + "2");
        m2.put(new Text("count"), new Text(""), new Value("11".getBytes()));
        m3 = new Mutation(s3 + DELIM + "3");
        m3.put(new Text("count"), new Text(""), new Value("13".getBytes()));
        m4 = new Mutation(s4 + DELIM + "8");
        m4.put(new Text("count"), new Text(""), new Value("20".getBytes()));
        m5 = new Mutation(s4 + DELIM + "2");
        m5.put(new Text("count"), new Text(""), new Value("10".getBytes()));

        mList.add(m1);
        mList.add(m2);
        mList.add(m3);
        mList.add(m4);
        mList.add(m5);

        bw1.addMutations(mList);
        bw1.close();

        m1 = new Mutation(s1);
        m2 = new Mutation(s5);
        m3 = new Mutation(s3);
        m4 = new Mutation(s4);
        int i = 5;
        int j = 6;
        int k = 7;
        int l = 8;
        Long count1;
        Long count2;
        Long count3;
        Long count4;

        for (String s : sList) {
            count1 = (long) i;
            count2 = (long) j;
            count3 = (long) k;
            count4 = (long) l;
            m1.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
            m2.put(new Text(s), new Text(count2.toString()), EMPTY_VAL);
            m3.put(new Text(s), new Text(count3.toString()), EMPTY_VAL);
            m4.put(new Text(s), new Text(count4.toString()), EMPTY_VAL);
            i = 2 * i;
            j = 2 * j;
            k = 2 * k;
            l = 2 * l;
        }
        m5 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
        m5.put(new Text("FullTableCardinality"), new Text("600"), EMPTY_VAL);
        mList2.add(m1);
        mList2.add(m2);
        mList2.add(m3);
        mList2.add(m4);
        mList2.add(m5);
        bw2.addMutations(mList2);
        bw2.close();

        List<StatementPattern> spList = getSpList(q3);
        System.out.println(spList);
        List<Double> jCardList = new ArrayList<Double>();

        for (StatementPattern sp1 : spList) {
            for (StatementPattern sp2 : spList) {
                jCardList.add(accc.getJoinSelect(arc, sp1, sp2));
            }
        }

        MathContext mc = new MathContext(3);

        Assert.assertEquals(3.2 / 600, jCardList.get(0), .001);
        Assert.assertEquals(0.5384615384615384 / 600, jCardList.get(3), .001);
        Assert.assertEquals(1.3333333333333333 / 600, jCardList.get(5), .001);
        Assert.assertEquals(2.6666666666666665 / 600, jCardList.get(6), .001);
        Assert.assertEquals(6.4 / 600, jCardList.get(8), .001);
        Assert.assertEquals(13. / 600, jCardList.get(15), .001);

        Assert.assertTrue(new BigDecimal(jCardList.get(2)).round(mc).equals(new BigDecimal(64.0 / 6000).round(mc)));
        Assert.assertTrue(new BigDecimal(jCardList.get(7)).round(mc).equals(new BigDecimal(7.0 / 7800).round(mc)));
        Assert.assertTrue(new BigDecimal(jCardList.get(14)).round(mc).equals(new BigDecimal(112.0 / 7800).round(mc)));

    }

    private List<StatementPattern> getSpList(String query) throws MalformedQueryException {

        SPARQLParser sp = new SPARQLParser();
        ParsedQuery pq = sp.parseQuery(query, null);
        TupleExpr te = pq.getTupleExpr();

        return StatementPatternCollector.process(te);
    }

}
