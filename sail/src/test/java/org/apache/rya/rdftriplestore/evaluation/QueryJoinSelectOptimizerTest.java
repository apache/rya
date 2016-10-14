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
import org.apache.rya.rdftriplestore.evaluation.QueryJoinSelectOptimizer;
import org.apache.rya.rdftriplestore.evaluation.RdfCloudTripleStoreSelectivityEvaluationStatistics;

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
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class QueryJoinSelectOptimizerTest {

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

  private String Q1 = ""//
      + "SELECT ?h  " //
      + "{" //
      + "  ?h <uri:peesOn> <uri:hydrant> . "//
      + "  ?h <uri:barksAt> <uri:cat> ."//
      + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ."//
      + "}";//

  private String q2 = ""//
      + "SELECT ?h ?l ?m" //
      + "{" //
      + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ."//
      + "  ?h <uri:barksAt> <uri:cat> ."//
      + "  ?h <uri:peesOn> <uri:hydrant> . "//
      + "  ?m <uri:eats>  <uri:chickens>. " //
      + "  ?m <uri:scratches> <uri:ears>. " //
      + "}";//

  private String Q2 = ""//
      + "SELECT ?h ?l ?m" //
      + "{" //
      + "  ?h <uri:peesOn> <uri:hydrant> . "//
      + "  ?h <uri:barksAt> <uri:cat> ."//
      + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ."//
      + "  ?m <uri:eats>  <uri:chickens>. " //
      + "  ?m <uri:scratches> <uri:ears>. " //
      + "}";//

  private String q3 = ""//
      + "SELECT ?h ?l ?m" //
      + "{" //
      + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ."//
      + "  ?h <uri:barksAt> <uri:cat> ."//
      + "  ?h <uri:peesOn> <uri:hydrant> . "//
      + "  {?m <uri:eats>  <uri:chickens>} OPTIONAL {?m <uri:scratches> <uri:ears>}. " //
      + "  {?m <uri:eats>  <uri:kibble>. ?m <uri:watches> <uri:television>.} UNION {?m <uri:rollsIn> <uri:mud>}. " //
      + "  ?l <uri:runsIn> <uri:field> ."//
      + "  ?l <uri:smells> <uri:butt> ."//
      + "  ?l <uri:eats> <uri:sticks> ."//
      + "}";//

  private String Q4 = ""//
      + "SELECT ?h ?l ?m" //
      + "{" //
      + "  ?h <uri:barksAt> <uri:cat> ."//
      + "  ?m <uri:scratches> <uri:ears>. " //
      + "  ?m <uri:eats>  <uri:chickens>. " //
      + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ."//
      + "  ?h <uri:peesOn> <uri:hydrant> . "//
      + "}";//

  private String q5 = ""//
      + "SELECT ?h ?l ?m" //
      + "{" //
      + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ."//
      + "  ?h <uri:barksAt> <uri:cat> ."//
      + "  ?h <uri:peesOn> <uri:hydrant> . "//
      + "  {?m <uri:eats>  <uri:kibble>. ?m <uri:watches> <uri:television>.?m <uri:eats>  <uri:chickens>} " + "  UNION {?m <uri:rollsIn> <uri:mud>}. " //
      + "  ?l <uri:runsIn> <uri:field> ."//
      + "  ?l <uri:smells> <uri:butt> ."//
      + "  ?l <uri:eats> <uri:sticks> ."//
      + "}";//
  
  
  private String q6 = ""//
          + "SELECT ?h ?l ?m" //
          + "{" //
          + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ."//
          + "  ?h <uri:barksAt> <uri:cat> ."//
          + "  ?h <uri:peesOn> <uri:hydrant> . "//
           + "  FILTER(?l = <uri:grover>) ." //
          + "  {?m <uri:eats>  <uri:kibble>. ?m <uri:watches> <uri:television>.?m <uri:eats>  <uri:chickens>} " + "  UNION {?m <uri:rollsIn> <uri:mud>}. " //
          + "  ?l <uri:runsIn> <uri:field> ."//
          + "  ?l <uri:smells> <uri:butt> ."//
          + "  ?l <uri:eats> <uri:sticks> ."//
          + "}";//

  private Connector conn;
  AccumuloRdfConfiguration arc;
  BatchWriterConfig config;
  RdfEvalStatsDAO<RdfCloudTripleStoreConfiguration> res;
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
    res = new ProspectorServiceEvalStatsDAO(conn, arc);

  }

  @Test
  public void testOptimizeQ1() throws Exception {

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
    List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "predicateobject", "predicatepredicate", "predicatesubject");
    Mutation m1, m2, m3, m4;

    m1 = new Mutation(s1 + DELIM + "3");
    m1.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m2 = new Mutation(s2 + DELIM + "2");
    m2.put(new Text("count"), new Text(""), new Value("2".getBytes()));
    m3 = new Mutation(s3 + DELIM + "1");
    m3.put(new Text("count"), new Text(""), new Value("1".getBytes()));
    mList.add(m1);
    mList.add(m2);
    mList.add(m3);

    bw1.addMutations(mList);
    bw1.close();

    Scanner scan = conn.createScanner("rya_prospects", new Authorizations());
    scan.setRange(new Range());

    for (Map.Entry<Key,Value> entry : scan) {
      System.out.println("Key row string is " + entry.getKey().getRow().toString());
      System.out.println("Key is " + entry.getKey());
      System.out.println("Value is " + (new String(entry.getValue().get())));
    }

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

    scan = conn.createScanner("rya_selectivity", new Authorizations());
    scan.setRange(new Range());

    for (Map.Entry<Key,Value> entry : scan) {
      System.out.println("Key row string is " + entry.getKey().getRow().toString());
      System.out.println("Key is " + entry.getKey());
      System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));

    }

    TupleExpr te = getTupleExpr(q1);

    RdfCloudTripleStoreSelectivityEvaluationStatistics ars = new RdfCloudTripleStoreSelectivityEvaluationStatistics(arc, res, accc);
    QueryJoinSelectOptimizer qjs = new QueryJoinSelectOptimizer(ars, accc);
    System.out.println("Originial query is " + te);
    qjs.optimize(te, null, null);
    Assert.assertTrue(te.equals(getTupleExpr(Q1)));

  }

  @Test
  public void testOptimizeQ2() throws Exception {

    System.out.println("*********************QUERY2********************");

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
    String s5 = "predicateobject" + DELIM + "uri:scratches" + DELIM + "uri:ears";
    String s4 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:chickens";
    List<Mutation> mList = new ArrayList<Mutation>();
    List<Mutation> mList2 = new ArrayList<Mutation>();
    List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "predicateobject", "predicatepredicate", "predicatesubject");
    Mutation m1, m2, m3, m4, m5, m6;

    m1 = new Mutation(s1 + DELIM + "3");
    m1.put(new Text("count"), new Text(""), new Value("4".getBytes()));
    m2 = new Mutation(s2 + DELIM + "2");
    m2.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m3 = new Mutation(s3 + DELIM + "1");
    m3.put(new Text("count"), new Text(""), new Value("2".getBytes()));
    m4 = new Mutation(s4 + DELIM + "1");
    m4.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m5 = new Mutation(s5 + DELIM + "1");
    m5.put(new Text("count"), new Text(""), new Value("5".getBytes()));
    mList.add(m1);
    mList.add(m2);
    mList.add(m3);
    mList.add(m4);
    mList.add(m5);

    bw1.addMutations(mList);
    bw1.close();

    Scanner scan = conn.createScanner("rya_prospects", new Authorizations());
    scan.setRange(new Range());

    for (Map.Entry<Key,Value> entry : scan) {
      System.out.println("Key row string is " + entry.getKey().getRow().toString());
      System.out.println("Key is " + entry.getKey());
      System.out.println("Value is " + (new String(entry.getValue().get())));
    }

    m1 = new Mutation(s1);
    m2 = new Mutation(s2);
    m3 = new Mutation(s3);
    m4 = new Mutation(s4);
    m5 = new Mutation(s5);
    m6 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
    m6.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
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
      m3.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
      m4.put(new Text(s), new Text(count3.toString()), EMPTY_VAL);
      m5.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);

      i = 2 * i;
      j = 2 * j;
      k = 2 * k;
    }
    mList2.add(m1);
    mList2.add(m2);
    mList2.add(m3);
    mList2.add(m5);
    mList2.add(m4);
    mList2.add(m6);
    bw2.addMutations(mList2);
    bw2.close();

    // scan = conn.createScanner("rya_selectivity" , new Authorizations());
    // scan.setRange(new Range());
    //
    // for (Map.Entry<Key, Value> entry : scan) {
    // System.out.println("Key row string is " + entry.getKey().getRow().toString());
    // System.out.println("Key is " + entry.getKey());
    // System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));
    //
    // }

    TupleExpr te = getTupleExpr(q2);
    System.out.println("Bindings are " + te.getBindingNames());
    RdfCloudTripleStoreSelectivityEvaluationStatistics ars = new RdfCloudTripleStoreSelectivityEvaluationStatistics(arc, res, accc);
    QueryJoinSelectOptimizer qjs = new QueryJoinSelectOptimizer(ars, accc);
    System.out.println("Originial query is " + te);
    qjs.optimize(te, null, null);
    System.out.println("Optimized query is " + te);
    // System.out.println("Bindings are " + te.getBindingNames());
    Assert.assertTrue(te.equals(getTupleExpr(Q2)));

  }

  @Test
  public void testOptimizeQ3() throws Exception {

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
    String s5 = "predicateobject" + DELIM + "uri:scratches" + DELIM + "uri:ears";
    String s4 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:chickens";
    String s6 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:kibble";
    String s7 = "predicateobject" + DELIM + "uri:rollsIn" + DELIM + "uri:mud";
    String s8 = "predicateobject" + DELIM + "uri:runsIn" + DELIM + "uri:field";
    String s9 = "predicateobject" + DELIM + "uri:smells" + DELIM + "uri:butt";
    String s10 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:sticks";

    List<Mutation> mList = new ArrayList<Mutation>();
    List<Mutation> mList2 = new ArrayList<Mutation>();
    List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "predicateobject", "predicatepredicate", "predicatesubject");
    Mutation m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11;

    m1 = new Mutation(s1 + DELIM + "3");
    m1.put(new Text("count"), new Text(""), new Value("5".getBytes()));
    m2 = new Mutation(s2 + DELIM + "2");
    m2.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m3 = new Mutation(s3 + DELIM + "1");
    m3.put(new Text("count"), new Text(""), new Value("2".getBytes()));
    m4 = new Mutation(s4 + DELIM + "1");
    m4.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m5 = new Mutation(s5 + DELIM + "1");
    m5.put(new Text("count"), new Text(""), new Value("5".getBytes()));
    m6 = new Mutation(s6 + DELIM + "1");
    m6.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m7 = new Mutation(s7 + DELIM + "1");
    m7.put(new Text("count"), new Text(""), new Value("2".getBytes()));
    m8 = new Mutation(s8 + DELIM + "1");
    m8.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m9 = new Mutation(s9 + DELIM + "1");
    m9.put(new Text("count"), new Text(""), new Value("1".getBytes()));
    m10 = new Mutation(s10 + DELIM + "1");
    m10.put(new Text("count"), new Text(""), new Value("1".getBytes()));

    mList.add(m1);
    mList.add(m2);
    mList.add(m3);
    mList.add(m4);
    mList.add(m5);
    mList.add(m6);
    mList.add(m7);
    mList.add(m8);
    mList.add(m9);
    mList.add(m10);

    bw1.addMutations(mList);
    bw1.close();

    Scanner scan = conn.createScanner("rya_prospects", new Authorizations());
    scan.setRange(new Range());

    for (Map.Entry<Key,Value> entry : scan) {
      System.out.println("Key row string is " + entry.getKey().getRow().toString());
      System.out.println("Key is " + entry.getKey());
      System.out.println("Value is " + (new String(entry.getValue().get())));
    }

    m1 = new Mutation(s1);
    m2 = new Mutation(s2);
    m3 = new Mutation(s3);
    m4 = new Mutation(s4);
    m5 = new Mutation(s5);
    m6 = new Mutation(s6);
    m7 = new Mutation(s7);
    m8 = new Mutation(s8);
    m9 = new Mutation(s9);
    m10 = new Mutation(s10);
    m11 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
    m11.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
    int i = 2;
    int j = 3;
    int k = 4;
    int l = 5;
    Long count1;
    Long count2;
    Long count3;
    Long count4;

    for (String s : sList) {
      count1 = (long) i;
      count2 = (long) j;
      count3 = (long) k;
      count4 = (long) l;
      m1.put(new Text(s), new Text(count4.toString()), EMPTY_VAL);
      m2.put(new Text(s), new Text(count2.toString()), EMPTY_VAL);
      m3.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
      m4.put(new Text(s), new Text(count3.toString()), EMPTY_VAL);
      m5.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
      m6.put(new Text(s), new Text(count2.toString()), EMPTY_VAL);
      m7.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
      m8.put(new Text(s), new Text(count4.toString()), EMPTY_VAL);
      m9.put(new Text(s), new Text(count3.toString()), EMPTY_VAL);
      m10.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);

      i = 2 * i;
      j = 2 * j;
      k = 2 * k;
      l = 2 * l;
    }
    mList2.add(m1);
    mList2.add(m2);
    mList2.add(m3);
    mList2.add(m5);
    mList2.add(m4);
    mList2.add(m6);
    mList2.add(m7);
    mList2.add(m8);
    mList2.add(m9);
    mList2.add(m10);
    mList2.add(m11);
    bw2.addMutations(mList2);
    bw2.close();

    scan = conn.createScanner("rya_selectivity", new Authorizations());
    scan.setRange(new Range());

    for (Map.Entry<Key,Value> entry : scan) {
      System.out.println("Key row string is " + entry.getKey().getRow().toString());
      System.out.println("Key is " + entry.getKey());
      System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));

    }

    TupleExpr te = getTupleExpr(q3);
    RdfCloudTripleStoreSelectivityEvaluationStatistics ars = new RdfCloudTripleStoreSelectivityEvaluationStatistics(arc, res, accc);
    QueryJoinSelectOptimizer qjs = new QueryJoinSelectOptimizer(ars, accc);
    System.out.println("Originial query is " + te);
    qjs.optimize(te, null, null);

    System.out.print("Optimized query is " + te);

  }

  @Test
  public void testOptimizeQ4() throws Exception {

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
    String s5 = "predicateobject" + DELIM + "uri:scratches" + DELIM + "uri:ears";
    String s4 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:chickens";
    List<Mutation> mList = new ArrayList<Mutation>();
    List<Mutation> mList2 = new ArrayList<Mutation>();
    List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "predicateobject", "predicatepredicate", "predicatesubject");
    Mutation m1, m2, m3, m4, m5, m6;

    m1 = new Mutation(s1 + DELIM + "3");
    m1.put(new Text("count"), new Text(""), new Value("4".getBytes()));
    m2 = new Mutation(s2 + DELIM + "2");
    m2.put(new Text("count"), new Text(""), new Value("0".getBytes()));
    m3 = new Mutation(s3 + DELIM + "1");
    m3.put(new Text("count"), new Text(""), new Value("8".getBytes()));
    m4 = new Mutation(s4 + DELIM + "1");
    m4.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m5 = new Mutation(s5 + DELIM + "1");
    m5.put(new Text("count"), new Text(""), new Value("0".getBytes()));
    mList.add(m1);
    mList.add(m2);
    mList.add(m3);
    mList.add(m4);
    mList.add(m5);

    bw1.addMutations(mList);
    bw1.close();

    Scanner scan = conn.createScanner("rya_prospects", new Authorizations());
    scan.setRange(new Range());

    for (Map.Entry<Key,Value> entry : scan) {
      System.out.println("Key row string is " + entry.getKey().getRow().toString());
      System.out.println("Key is " + entry.getKey());
      System.out.println("Value is " + (new String(entry.getValue().get())));
    }

    m1 = new Mutation(s1);
    m2 = new Mutation(s2);
    m3 = new Mutation(s3);
    m4 = new Mutation(s4);
    m5 = new Mutation(s5);
    m6 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
    m6.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
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
      m3.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
      m4.put(new Text(s), new Text(count3.toString()), EMPTY_VAL);
      m5.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);

      i = 2 * i;
      j = 2 * j;
      k = 2 * k;
    }
    mList2.add(m1);
    mList2.add(m2);
    mList2.add(m3);
    mList2.add(m5);
    mList2.add(m4);
    mList2.add(m6);
    bw2.addMutations(mList2);
    bw2.close();

    scan = conn.createScanner("rya_selectivity", new Authorizations());
    scan.setRange(new Range());

    for (Map.Entry<Key,Value> entry : scan) {
      System.out.println("Key row string is " + entry.getKey().getRow().toString());
      System.out.println("Key is " + entry.getKey());
      System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));

    }

    TupleExpr te = getTupleExpr(q2);
    RdfCloudTripleStoreSelectivityEvaluationStatistics ars = new RdfCloudTripleStoreSelectivityEvaluationStatistics(arc, res, accc);
    QueryJoinSelectOptimizer qjs = new QueryJoinSelectOptimizer(ars, accc);
    System.out.println("Originial query is " + te);
    qjs.optimize(te, null, null);
    Assert.assertTrue(te.equals(getTupleExpr(Q4)));

    System.out.print("Optimized query is " + te);

  }

  @Test
  public void testOptimizeQ5() throws Exception {

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
    String s5 = "predicateobject" + DELIM + "uri:watches" + DELIM + "uri:television";
    String s4 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:chickens";
    String s6 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:kibble";
    String s7 = "predicateobject" + DELIM + "uri:rollsIn" + DELIM + "uri:mud";
    String s8 = "predicateobject" + DELIM + "uri:runsIn" + DELIM + "uri:field";
    String s9 = "predicateobject" + DELIM + "uri:smells" + DELIM + "uri:butt";
    String s10 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:sticks";

    List<Mutation> mList = new ArrayList<Mutation>();
    List<Mutation> mList2 = new ArrayList<Mutation>();
    List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "predicateobject", "predicatepredicate", "predicatesubject");
    Mutation m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11;

    m1 = new Mutation(s1 + DELIM + "3");
    m1.put(new Text("count"), new Text(""), new Value("5".getBytes()));
    m2 = new Mutation(s2 + DELIM + "2");
    m2.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m3 = new Mutation(s3 + DELIM + "1");
    m3.put(new Text("count"), new Text(""), new Value("2".getBytes()));
    m4 = new Mutation(s4 + DELIM + "1");
    m4.put(new Text("count"), new Text(""), new Value("0".getBytes()));
    m5 = new Mutation(s5 + DELIM + "1");
    m5.put(new Text("count"), new Text(""), new Value("1".getBytes()));
    m6 = new Mutation(s6 + DELIM + "1");
    m6.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m7 = new Mutation(s7 + DELIM + "1");
    m7.put(new Text("count"), new Text(""), new Value("2".getBytes()));
    m8 = new Mutation(s8 + DELIM + "1");
    m8.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m9 = new Mutation(s9 + DELIM + "1");
    m9.put(new Text("count"), new Text(""), new Value("1".getBytes()));
    m10 = new Mutation(s10 + DELIM + "1");
    m10.put(new Text("count"), new Text(""), new Value("1".getBytes()));

    mList.add(m1);
    mList.add(m2);
    mList.add(m3);
    mList.add(m4);
    mList.add(m5);
    mList.add(m6);
    mList.add(m7);
    mList.add(m8);
    mList.add(m9);
    mList.add(m10);

    bw1.addMutations(mList);
    bw1.close();

    Scanner scan = conn.createScanner("rya_prospects", new Authorizations());
    scan.setRange(new Range());

    for (Map.Entry<Key,Value> entry : scan) {
      System.out.println("Key row string is " + entry.getKey().getRow().toString());
      System.out.println("Key is " + entry.getKey());
      System.out.println("Value is " + (new String(entry.getValue().get())));
    }

    m1 = new Mutation(s1);
    m2 = new Mutation(s2);
    m3 = new Mutation(s3);
    m4 = new Mutation(s4);
    m5 = new Mutation(s5);
    m6 = new Mutation(s6);
    m7 = new Mutation(s7);
    m8 = new Mutation(s8);
    m9 = new Mutation(s9);
    m10 = new Mutation(s10);
    m11 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
    m11.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
    int i = 2;
    int j = 3;
    int k = 4;
    int l = 5;
    Long count1;
    Long count2;
    Long count3;
    Long count4;

    for (String s : sList) {
      count1 = (long) i;
      count2 = (long) j;
      count3 = (long) k;
      count4 = (long) l;
      m1.put(new Text(s), new Text(count4.toString()), EMPTY_VAL);
      m2.put(new Text(s), new Text(count2.toString()), EMPTY_VAL);
      m3.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
      m4.put(new Text(s), new Text(count3.toString()), EMPTY_VAL);
      m5.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
      m6.put(new Text(s), new Text(count2.toString()), EMPTY_VAL);
      m7.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
      m8.put(new Text(s), new Text(count4.toString()), EMPTY_VAL);
      m9.put(new Text(s), new Text(count3.toString()), EMPTY_VAL);
      m10.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);

      i = 2 * i;
      j = 2 * j;
      k = 2 * k;
      l = 2 * l;
    }
    mList2.add(m1);
    mList2.add(m2);
    mList2.add(m3);
    mList2.add(m5);
    mList2.add(m4);
    mList2.add(m6);
    mList2.add(m7);
    mList2.add(m8);
    mList2.add(m9);
    mList2.add(m10);
    mList2.add(m11);
    bw2.addMutations(mList2);
    bw2.close();

    scan = conn.createScanner("rya_selectivity", new Authorizations());
    scan.setRange(new Range());

    for (Map.Entry<Key,Value> entry : scan) {
      System.out.println("Key row string is " + entry.getKey().getRow().toString());
      System.out.println("Key is " + entry.getKey());
      System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));

    }

    TupleExpr te = getTupleExpr(q5);
    System.out.println("Bindings are " + te.getBindingNames());
    RdfCloudTripleStoreSelectivityEvaluationStatistics ars = new RdfCloudTripleStoreSelectivityEvaluationStatistics(arc, res, accc);
    QueryJoinSelectOptimizer qjs = new QueryJoinSelectOptimizer(ars, accc);
    System.out.println("Originial query is " + te);
    qjs.optimize(te, null, null);
    System.out.println("Bindings are " + te.getBindingNames());

    System.out.print("Optimized query is " + te);

  }
  
  
  
  
  
  
  
  
  @Test
  public void testOptimizeQ6() throws Exception {

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
    String s5 = "predicateobject" + DELIM + "uri:watches" + DELIM + "uri:television";
    String s4 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:chickens";
    String s6 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:kibble";
    String s7 = "predicateobject" + DELIM + "uri:rollsIn" + DELIM + "uri:mud";
    String s8 = "predicateobject" + DELIM + "uri:runsIn" + DELIM + "uri:field";
    String s9 = "predicateobject" + DELIM + "uri:smells" + DELIM + "uri:butt";
    String s10 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:sticks";

    List<Mutation> mList = new ArrayList<Mutation>();
    List<Mutation> mList2 = new ArrayList<Mutation>();
    List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "predicateobject", "predicatepredicate", "predicatesubject");
    Mutation m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11;

    m1 = new Mutation(s1 + DELIM + "3");
    m1.put(new Text("count"), new Text(""), new Value("5".getBytes()));
    m2 = new Mutation(s2 + DELIM + "2");
    m2.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m3 = new Mutation(s3 + DELIM + "1");
    m3.put(new Text("count"), new Text(""), new Value("2".getBytes()));
    m4 = new Mutation(s4 + DELIM + "1");
    m4.put(new Text("count"), new Text(""), new Value("0".getBytes()));
    m5 = new Mutation(s5 + DELIM + "1");
    m5.put(new Text("count"), new Text(""), new Value("1".getBytes()));
    m6 = new Mutation(s6 + DELIM + "1");
    m6.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m7 = new Mutation(s7 + DELIM + "1");
    m7.put(new Text("count"), new Text(""), new Value("2".getBytes()));
    m8 = new Mutation(s8 + DELIM + "1");
    m8.put(new Text("count"), new Text(""), new Value("3".getBytes()));
    m9 = new Mutation(s9 + DELIM + "1");
    m9.put(new Text("count"), new Text(""), new Value("1".getBytes()));
    m10 = new Mutation(s10 + DELIM + "1");
    m10.put(new Text("count"), new Text(""), new Value("1".getBytes()));

    mList.add(m1);
    mList.add(m2);
    mList.add(m3);
    mList.add(m4);
    mList.add(m5);
    mList.add(m6);
    mList.add(m7);
    mList.add(m8);
    mList.add(m9);
    mList.add(m10);

    bw1.addMutations(mList);
    bw1.close();

    Scanner scan = conn.createScanner("rya_prospects", new Authorizations());
    scan.setRange(new Range());

    for (Map.Entry<Key,Value> entry : scan) {
      System.out.println("Key row string is " + entry.getKey().getRow().toString());
      System.out.println("Key is " + entry.getKey());
      System.out.println("Value is " + (new String(entry.getValue().get())));
    }

    m1 = new Mutation(s1);
    m2 = new Mutation(s2);
    m3 = new Mutation(s3);
    m4 = new Mutation(s4);
    m5 = new Mutation(s5);
    m6 = new Mutation(s6);
    m7 = new Mutation(s7);
    m8 = new Mutation(s8);
    m9 = new Mutation(s9);
    m10 = new Mutation(s10);
    m11 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
    m11.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
    int i = 2;
    int j = 3;
    int k = 4;
    int l = 5;
    Long count1;
    Long count2;
    Long count3;
    Long count4;

    for (String s : sList) {
      count1 = (long) i;
      count2 = (long) j;
      count3 = (long) k;
      count4 = (long) l;
      m1.put(new Text(s), new Text(count4.toString()), EMPTY_VAL);
      m2.put(new Text(s), new Text(count2.toString()), EMPTY_VAL);
      m3.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
      m4.put(new Text(s), new Text(count3.toString()), EMPTY_VAL);
      m5.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
      m6.put(new Text(s), new Text(count2.toString()), EMPTY_VAL);
      m7.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);
      m8.put(new Text(s), new Text(count4.toString()), EMPTY_VAL);
      m9.put(new Text(s), new Text(count3.toString()), EMPTY_VAL);
      m10.put(new Text(s), new Text(count1.toString()), EMPTY_VAL);

      i = 2 * i;
      j = 2 * j;
      k = 2 * k;
      l = 2 * l;
    }
    mList2.add(m1);
    mList2.add(m2);
    mList2.add(m3);
    mList2.add(m5);
    mList2.add(m4);
    mList2.add(m6);
    mList2.add(m7);
    mList2.add(m8);
    mList2.add(m9);
    mList2.add(m10);
    mList2.add(m11);
    bw2.addMutations(mList2);
    bw2.close();

    scan = conn.createScanner("rya_selectivity", new Authorizations());
    scan.setRange(new Range());

    for (Map.Entry<Key,Value> entry : scan) {
      System.out.println("Key row string is " + entry.getKey().getRow().toString());
      System.out.println("Key is " + entry.getKey());
      System.out.println("Value is " + (new String(entry.getKey().getColumnQualifier().toString())));

    }

    TupleExpr te = getTupleExpr(q6);
    TupleExpr te2 = (TupleExpr) te.clone();
    System.out.println("Bindings are " + te.getBindingNames());
    RdfCloudTripleStoreSelectivityEvaluationStatistics ars = new RdfCloudTripleStoreSelectivityEvaluationStatistics(arc, res, accc);
    QueryJoinSelectOptimizer qjs = new QueryJoinSelectOptimizer(ars, accc);
    System.out.println("Originial query is " + te);
    qjs.optimize(te, null, null);
    
    
    
    FilterOptimizer fo = new FilterOptimizer();
    fo.optimize(te2, null, null);
    System.out.print("filter optimized query before js opt is " + te2);
    qjs.optimize(te2, null, null);

    System.out.println("join selectivity opt query before filter opt is " + te);
    fo.optimize(te, null, null);
    
    System.out.println("join selectivity opt query is " + te);
    System.out.print("filter optimized query is " + te2);

  }
  
  
  
  
  
  
  
  
  
  
  
  
  

  private TupleExpr getTupleExpr(String query) throws MalformedQueryException {

    SPARQLParser sp = new SPARQLParser();
    ParsedQuery pq = sp.parseQuery(query, null);

    return pq.getTupleExpr();
  }

}
