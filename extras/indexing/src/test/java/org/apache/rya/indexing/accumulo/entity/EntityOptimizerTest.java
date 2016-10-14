package org.apache.rya.indexing.accumulo.entity;

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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.layout.TablePrefixLayoutStrategy;
import org.apache.rya.api.persist.RdfEvalStatsDAO;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.entity.EntityOptimizer;
import org.apache.rya.indexing.accumulo.entity.EntityTupleSet;
import org.apache.rya.joinselect.AccumuloSelectivityEvalDAO;
import org.apache.rya.prospector.service.ProspectorServiceEvalStatsDAO;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.impl.FilterOptimizer;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class EntityOptimizerTest {

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


    private String q2 = ""//
        + "SELECT ?h ?m" //
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
            + "SELECT ?h ?m" //
            + "{" //
            + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ."//
            + "  ?h <uri:barksAt> <uri:cat> ."//
            + "  ?h <uri:peesOn> ?m . "//
            + "  ?m <uri:eats>  <uri:chickens>. " //
            + "  ?m <uri:scratches> <uri:ears>. " //
            + "}";//
    
    
    
    private String q6 = ""//
            + "SELECT ?h ?i ?l ?m" //
            + "{" //
            + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ."//
            + "  ?h <uri:barksAt> <uri:cat> ."//
            + "  ?h <uri:peesOn> <uri:hydrant> . "//
            + "  {?m <uri:eats>  <uri:chickens>} OPTIONAL {?m <uri:scratches> <uri:ears>}. " //
            + "  {?m <uri:eats> <uri:kibble>. ?m <uri:watches> ?i. ?i <uri:runsIn> <uri:field> .} " //
            + "  UNION {?m <uri:rollsIn> <uri:mud>. ?l <uri:smells> ?m . ?l <uri:eats> <uri:sticks> . }. " //
            + "}";//
    
    
    private String q7 = ""//
            + "SELECT ?h ?m" //
            + "{" //
            + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:chickens> ."//
            + "  ?h <uri:barksAt> <uri:chickens> ."//
            + "  ?h <uri:peesOn> ?m . "//
            + "  ?m <uri:eats>  <uri:chickens>. " //
            + "  ?m <uri:scratches> <uri:ears>. " //
            + "}";//
    
    
    private String q8 = ""//
            + "SELECT ?h ?m" //
            + "{" //
            + "  Filter(?h = \"Diego\") " //
            + "  Filter(?m = \"Rosie\") " //
            + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:chickens> ."//
            + "  ?h <uri:barksAt> <uri:chickens> ."//
            + "  ?h <uri:peesOn> ?m . "//
            + "  ?m <uri:eats>  <uri:chickens>. " //
            + "  ?m <uri:scratches> <uri:ears>. " //
            + "}";//
    
    
    
    
    private String q9 = ""//
            + "SELECT ?h ?i ?l ?m" //
            + "{" //
            + "  Filter(?h = \"Diego\") " //
            + "  Filter(?m = \"Rosie\") " //
            + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> <uri:dog> ."//
            + "  ?h <uri:barksAt> <uri:cat> ."//
            + "  ?h <uri:peesOn> <uri:hydrant> . "//
            + "  {?m <uri:eats>  <uri:chickens>} OPTIONAL {?m <uri:scratches> <uri:ears>}. " //
            + "  {   Filter(?i = \"Bobo\").  ?m <uri:eats> <uri:kibble>. ?m <uri:watches> ?i. ?i <uri:runsIn> <uri:field> .} " //
            + "  UNION {?m <uri:rollsIn> <uri:mud>. ?l <uri:smells> ?m . ?l <uri:eats> <uri:sticks> . }. " //
            + "}";//
    
    

    private Connector accCon;
    AccumuloRdfConfiguration conf;
    BatchWriterConfig config;
    RdfEvalStatsDAO<RdfCloudTripleStoreConfiguration> res;
    
    
    
    @Before
    public void init() throws RepositoryException, TupleQueryResultHandlerException, QueryEvaluationException,
            MalformedQueryException, AccumuloException, AccumuloSecurityException, TableExistsException {

        
       
        accCon = new MockInstance("instance").getConnector("root", "".getBytes());
        
        config = new BatchWriterConfig();
        config.setMaxMemory(1000);
        config.setMaxLatency(1000, TimeUnit.SECONDS);
        config.setMaxWriteThreads(10);
        
        if (accCon.tableOperations().exists("rya_prospects")) {
            try {
                accCon.tableOperations().delete("rya_prospects");
            } catch (TableNotFoundException e) {
                e.printStackTrace();
            }
        }
        if (accCon.tableOperations().exists("rya_selectivity")) {
            try {
                accCon.tableOperations().delete("rya_selectivity");
            } catch (TableNotFoundException e) {
                e.printStackTrace();
            }
        }
          
        accCon.tableOperations().create("rya_prospects");
        accCon.tableOperations().create("rya_selectivity");
        
        Configuration con = new Configuration();
        con.set(ConfigUtils.CLOUDBASE_AUTHS, "U");
        con.set(ConfigUtils.CLOUDBASE_INSTANCE, "instance");
        con.set(ConfigUtils.CLOUDBASE_USER, "root");
        con.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
        conf = new AccumuloRdfConfiguration(con);
        TablePrefixLayoutStrategy tps = new TablePrefixLayoutStrategy("rya_");
        conf.setTableLayoutStrategy(tps);
        conf.set(ConfigUtils.USE_MOCK_INSTANCE, "true");
        
        
        res = new ProspectorServiceEvalStatsDAO(accCon, conf);

    }
    

 @Test
    public void testOptimizeQ1SamePriority() throws Exception {

      AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
      accc.setConf(conf);
      accc.setConnector(accCon);
      accc.setRdfEvalDAO(res);
      accc.init();

      BatchWriter bw1 = accCon.createBatchWriter("rya_prospects", config);
      BatchWriter bw2 = accCon.createBatchWriter("rya_selectivity", config);

      String s1 = "predicateobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:dog";
      String s2 = "predicateobject" + DELIM + "uri:barksAt" + DELIM + "uri:cat";
      String s3 = "predicateobject" + DELIM + "uri:peesOn" + DELIM + "uri:hydrant";
      List<Mutation> mList = new ArrayList<Mutation>();
      List<Mutation> mList2 = new ArrayList<Mutation>();
      List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject");
      Mutation m1, m2, m3, m4;

      m1 = new Mutation(s1 + DELIM + "1");
      m1.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m2 = new Mutation(s2 + DELIM + "1");
      m2.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m3 = new Mutation(s3 + DELIM + "1");
      m3.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      mList.add(m1);
      mList.add(m2);
      mList.add(m3);

      bw1.addMutations(mList);
      bw1.close();


      m1 = new Mutation(s1);
      m2 = new Mutation(s2);
      m3 = new Mutation(s3);
      m4 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
      m4.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
     

      for (String s : sList) {
       
        m1.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m2.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m3.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
       
      }
      mList2.add(m1);
      mList2.add(m2);
      mList2.add(m3);
      mList2.add(m4);
      bw2.addMutations(mList2);
      bw2.close();


      TupleExpr te = getTupleExpr(q1);

      EntityOptimizer cco = new EntityOptimizer(accc);
      System.out.println("Originial query is " + te);
      cco.optimize(te, null, null);

      
      EntityCentricVisitor ccv = new EntityCentricVisitor();
      te.visit(ccv);
      
      Assert.assertEquals(1, ccv.getCcNodes().size());
      
      System.out.println(te);

    }
    
    
    
    
    
 @Test
    public void testOptimizeQ2SamePriority() throws Exception {

      AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
      accc.setConf(conf);
      accc.setConnector(accCon);
      accc.setRdfEvalDAO(res);
      accc.init();

      BatchWriter bw1 = accCon.createBatchWriter("rya_prospects", config);
      BatchWriter bw2 = accCon.createBatchWriter("rya_selectivity", config);

      String s1 = "predicateobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:dog";
      String s2 = "predicateobject" + DELIM + "uri:barksAt" + DELIM + "uri:cat";
      String s3 = "predicateobject" + DELIM + "uri:peesOn" + DELIM + "uri:hydrant";
      String s5 = "predicateobject" + DELIM + "uri:scratches" + DELIM + "uri:ears";
      String s4 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:chickens";
      List<Mutation> mList = new ArrayList<Mutation>();
      List<Mutation> mList2 = new ArrayList<Mutation>();
      List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject");
      Mutation m1, m2, m3, m4, m5, m6;

      m1 = new Mutation(s1 + DELIM + "1");
      m1.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m2 = new Mutation(s2 + DELIM + "1");
      m2.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m3 = new Mutation(s3 + DELIM + "1");
      m3.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m4 = new Mutation(s4 + DELIM + "1");
      m4.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m5 = new Mutation(s5 + DELIM + "1");
      m5.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      mList.add(m1);
      mList.add(m2);
      mList.add(m3);
      mList.add(m4);
      mList.add(m5);

      bw1.addMutations(mList);
      bw1.close();

//      Scanner scan = conn.createScanner("rya_prospects", new Authorizations());
//      scan.setRange(new Range());
//
//      for (Map.Entry<Key,Value> entry : scan) {
//        System.out.println("Key row string is " + entry.getKey().getRow().toString());
//        System.out.println("Key is " + entry.getKey());
//        System.out.println("Value is " + (new String(entry.getValue().get())));
//      }

      m1 = new Mutation(s1);
      m2 = new Mutation(s2);
      m3 = new Mutation(s3);
      m4 = new Mutation(s4);
      m5 = new Mutation(s5);
      m6 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
      m6.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
     

      for (String s : sList) {
       
        m1.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m2.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m3.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m4.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m5.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
       
      }
      mList2.add(m1);
      mList2.add(m2);
      mList2.add(m3);
      mList2.add(m4);
      mList2.add(m5);
      mList2.add(m6);
      bw2.addMutations(mList2);
      bw2.close();


      TupleExpr te = getTupleExpr(q2);

      EntityOptimizer cco = new EntityOptimizer(accc);
      System.out.println("Originial query is " + te);
      cco.optimize(te, null, null);

      EntityCentricVisitor ccv = new EntityCentricVisitor();
      te.visit(ccv);
      
      Assert.assertEquals(2, ccv.getCcNodes().size());
      
      
      System.out.println(te);

    }
    
    
    
    
    
    
 @Test
    public void testOptimizeQ3SamePriority() throws Exception {

      AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
      accc.setConf(conf);
      accc.setConnector(accCon);
      accc.setRdfEvalDAO(res);
      accc.init();

      BatchWriter bw1 = accCon.createBatchWriter("rya_prospects", config);
      BatchWriter bw2 = accCon.createBatchWriter("rya_selectivity", config);

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
      String s11 = "predicateobject" + DELIM + "uri:watches" + DELIM + "uri:television";
      

      
      
      List<Mutation> mList = new ArrayList<Mutation>();
      List<Mutation> mList2 = new ArrayList<Mutation>();
      List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject");
      Mutation m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12;

      m1 = new Mutation(s1 + DELIM + "1");
      m1.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m2 = new Mutation(s2 + DELIM + "1");
      m2.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m3 = new Mutation(s3 + DELIM + "1");
      m3.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m4 = new Mutation(s4 + DELIM + "1");
      m4.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m5 = new Mutation(s5 + DELIM + "1");
      m5.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m6 = new Mutation(s6 + DELIM + "1");
      m6.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m7 = new Mutation(s7 + DELIM + "1");
      m7.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m8 = new Mutation(s8 + DELIM + "1");
      m8.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m9 = new Mutation(s9 + DELIM + "1");
      m9.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m10 = new Mutation(s10 + DELIM + "1");
      m10.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m11 = new Mutation(s11 + DELIM + "1");
      m11.put(new Text("count"), new Text(""), new Value("1".getBytes()));

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
      mList.add(m11);

      bw1.addMutations(mList);
      bw1.close();


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
      m11 = new Mutation(s11);
      m12 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
      m12.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
     

      for (String s : sList) {
       
        m1.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m2.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m3.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m4.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m5.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m6.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m7.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m8.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m9.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m10.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m11.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);

       
      }
      mList2.add(m1);
      mList2.add(m2);
      mList2.add(m3);
      mList2.add(m4);
      mList2.add(m5);
      mList2.add(m6);
      mList2.add(m7);
      mList2.add(m8);
      mList2.add(m9);
      mList2.add(m10);
      mList2.add(m11);
      mList2.add(m12);
      bw2.addMutations(mList2);
      bw2.close();



      TupleExpr te = getTupleExpr(q3);

      EntityOptimizer cco = new EntityOptimizer(accc);
      System.out.println("Originial query is " + te);
      cco.optimize(te, null, null);

      
      EntityCentricVisitor ccv = new EntityCentricVisitor();
      te.visit(ccv);
      
      Assert.assertEquals(3, ccv.getCcNodes().size());
      
      System.out.println(te);

    }
    
    
    
    
   
    
 @Test
    public void testOptimizeQ2DiffPriority() throws Exception {

      AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
      accc.setConf(conf);
      accc.setConnector(accCon);
      accc.setRdfEvalDAO(res);
      accc.init();

      BatchWriter bw1 = accCon.createBatchWriter("rya_prospects", config);
      BatchWriter bw2 = accCon.createBatchWriter("rya_selectivity", config);

      String s1 = "predicateobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:dog";
      String s2 = "predicateobject" + DELIM + "uri:barksAt" + DELIM + "uri:cat";
      String s3 = "predicate" + DELIM + "uri:peesOn";
      String s5 = "predicateobject" + DELIM + "uri:scratches" + DELIM + "uri:ears";
      String s4 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:chickens";
      List<Mutation> mList = new ArrayList<Mutation>();
      List<Mutation> mList2 = new ArrayList<Mutation>();
      List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "objectsubject", "objectpredicate", "objectobject");
      Mutation m1, m2, m3, m4, m5, m6;

      m1 = new Mutation(s1 + DELIM + "1");
      m1.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m2 = new Mutation(s2 + DELIM + "1");
      m2.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m3 = new Mutation(s3 + DELIM + "1");
      m3.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m4 = new Mutation(s4 + DELIM + "1");
      m4.put(new Text("count"), new Text(""), new Value("3".getBytes()));
      m5 = new Mutation(s5 + DELIM + "1");
      m5.put(new Text("count"), new Text(""), new Value("3".getBytes()));
      mList.add(m1);
      mList.add(m2);
      mList.add(m3);
      mList.add(m4);
      mList.add(m5);

      bw1.addMutations(mList);
      bw1.close();


      m1 = new Mutation(s1);
      m2 = new Mutation(s2);
      m3 = new Mutation(s3);
      m4 = new Mutation(s4);
      m5 = new Mutation(s5);
      m6 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
      m6.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
     

      for (String s : sList) {
       
        m1.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);
        m2.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);
        m3.put(new Text(s), new Text(Integer.toString(3)), EMPTY_VAL);
        m4.put(new Text(s), new Text(Integer.toString(3)), EMPTY_VAL);
        m5.put(new Text(s), new Text(Integer.toString(3)), EMPTY_VAL);
       
      }
      mList2.add(m1);
      mList2.add(m2);
      mList2.add(m3);
      mList2.add(m4);
      mList2.add(m5);
      mList2.add(m6);
      bw2.addMutations(mList2);
      bw2.close();


      TupleExpr te = getTupleExpr(q5);

      EntityOptimizer cco = new EntityOptimizer(accc);
      System.out.println("Originial query is " + te);
      cco.optimize(te, null, null);

      EntityCentricVisitor ccv = new EntityCentricVisitor();
      te.visit(ccv);
      
      List<QueryModelNode> nodes = Lists.newArrayList(ccv.getCcNodes());
      
      Assert.assertEquals(2, nodes.size());
      
      for(QueryModelNode q: nodes) {
          
          if(((EntityTupleSet)q).getStarQuery().getNodes().size() == 2) {
              Assert.assertEquals("h", ((EntityTupleSet)q).getStarQuery().getCommonVarName());
          } else if(((EntityTupleSet)q).getStarQuery().getNodes().size() == 3) {
              Assert.assertEquals("m", ((EntityTupleSet)q).getStarQuery().getCommonVarName());
          } else {
              Assert.assertTrue(false);
          }
      }
      
     
      
     
      
      System.out.println(te);

    }
    
    
    
    
    
    
 @Test
    public void testOptimizeQ2DiffPriority2() throws Exception {

      AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
      accc.setConf(conf);
      accc.setConnector(accCon);
      accc.setRdfEvalDAO(res);
      accc.init();

      BatchWriter bw1 = accCon.createBatchWriter("rya_prospects", config);
      BatchWriter bw2 = accCon.createBatchWriter("rya_selectivity", config);

      String s1 = "predicateobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:dog";
      String s2 = "predicateobject" + DELIM + "uri:barksAt" + DELIM + "uri:cat";
      String s3 = "predicate" + DELIM + "uri:peesOn";
      String s5 = "predicateobject" + DELIM + "uri:scratches" + DELIM + "uri:ears";
      String s4 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:chickens";
      List<Mutation> mList = new ArrayList<Mutation>();
      List<Mutation> mList2 = new ArrayList<Mutation>();
      List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "objectsubject", "objectpredicate", "objectobject");
      Mutation m1, m2, m3, m4, m5, m6;

      m1 = new Mutation(s1 + DELIM + "1");
      m1.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m2 = new Mutation(s2 + DELIM + "1");
      m2.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m3 = new Mutation(s3 + DELIM + "1");
      m3.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m4 = new Mutation(s4 + DELIM + "1");
      m4.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m5 = new Mutation(s5 + DELIM + "1");
      m5.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      mList.add(m1);
      mList.add(m2);
      mList.add(m3);
      mList.add(m4);
      mList.add(m5);

      bw1.addMutations(mList);
      bw1.close();


      m1 = new Mutation(s1);
      m2 = new Mutation(s2);
      m3 = new Mutation(s3);
      m4 = new Mutation(s4);
      m5 = new Mutation(s5);
      m6 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
      m6.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
     

      for (String s : sList) {
       
        m1.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);
        m2.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);
        m3.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);
        m4.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);
        m5.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
       
      }
      mList2.add(m1);
      mList2.add(m2);
      mList2.add(m3);
      mList2.add(m4);
      mList2.add(m5);
      mList2.add(m6);
      bw2.addMutations(mList2);
      bw2.close();


      TupleExpr te = getTupleExpr(q5);

      EntityOptimizer cco = new EntityOptimizer(accc);
      System.out.println("Originial query is " + te);
      cco.optimize(te, null, null);

      EntityCentricVisitor ccv = new EntityCentricVisitor();
      te.visit(ccv);
      
      List<QueryModelNode> nodes = Lists.newArrayList(ccv.getCcNodes());
      
      Assert.assertEquals(2, nodes.size());
      
      
      for(QueryModelNode q: nodes) {
          
          if(((EntityTupleSet)q).getStarQuery().getNodes().size() == 2) {
              Assert.assertEquals("m", ((EntityTupleSet)q).getStarQuery().getCommonVarName());
          } else if(((EntityTupleSet)q).getStarQuery().getNodes().size() == 3) {
              Assert.assertEquals("h", ((EntityTupleSet)q).getStarQuery().getCommonVarName());
          } else {
              Assert.assertTrue(false);
          }
      }
      
      
      System.out.println(te);

    }
    
   
    

    
    
 @Test
    public void testOptimizeQ6DiffPriority() throws Exception {

      AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
      accc.setConf(conf);
      accc.setConnector(accCon);
      accc.setRdfEvalDAO(res);
      accc.init();

      BatchWriter bw1 = accCon.createBatchWriter("rya_prospects", config);
      BatchWriter bw2 = accCon.createBatchWriter("rya_selectivity", config);

      String s1 = "predicateobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:dog";
      String s2 = "predicateobject" + DELIM + "uri:barksAt" + DELIM + "uri:cat";
      String s3 = "predicateobject" + DELIM + "uri:peesOn" + DELIM + "uri:hydrant";
      String s5 = "predicateobject" + DELIM + "uri:scratches" + DELIM + "uri:ears";
      String s4 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:chickens";
      String s6 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:kibble";
      String s7 = "predicateobject" + DELIM + "uri:rollsIn" + DELIM + "uri:mud";
      String s8 = "predicateobject" + DELIM + "uri:runsIn" + DELIM + "uri:field";
      String s9 = "predicate" + DELIM + "uri:smells" ;
      String s10 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:sticks";
      String s11 = "predicate" + DELIM + "uri:watches";
      

      
      
      List<Mutation> mList = new ArrayList<Mutation>();
      List<Mutation> mList2 = new ArrayList<Mutation>();
      List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject","objectsubject", "objectpredicate", "objectobject");
      Mutation m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12;

      m1 = new Mutation(s1 + DELIM + "1");
      m1.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m2 = new Mutation(s2 + DELIM + "1");
      m2.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m3 = new Mutation(s3 + DELIM + "1");
      m3.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m4 = new Mutation(s4 + DELIM + "1");
      m4.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m5 = new Mutation(s5 + DELIM + "1");
      m5.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m6 = new Mutation(s6 + DELIM + "1");
      m6.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m7 = new Mutation(s7 + DELIM + "1");
      m7.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m8 = new Mutation(s8 + DELIM + "1");
      m8.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m9 = new Mutation(s9 + DELIM + "1");
      m9.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m10 = new Mutation(s10 + DELIM + "1");
      m10.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m11 = new Mutation(s11 + DELIM + "1");
      m11.put(new Text("count"), new Text(""), new Value("2".getBytes()));

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
      mList.add(m11);

      bw1.addMutations(mList);
      bw1.close();


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
      m11 = new Mutation(s11);
      m12 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
      m12.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
     

      for (String s : sList) {
       
        m1.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m2.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m3.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m4.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m5.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m6.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m7.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);
        m8.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);
        m9.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);
        m10.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m11.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);

       
      }
      mList2.add(m1);
      mList2.add(m2);
      mList2.add(m3);
      mList2.add(m4);
      mList2.add(m5);
      mList2.add(m6);
      mList2.add(m7);
      mList2.add(m8);
      mList2.add(m9);
      mList2.add(m10);
      mList2.add(m11);
      mList2.add(m12);
      bw2.addMutations(mList2);
      bw2.close();



      TupleExpr te = getTupleExpr(q6);

      EntityOptimizer cco = new EntityOptimizer(accc);
      System.out.println("Originial query is " + te);
      cco.optimize(te, null, null);

      EntityCentricVisitor ccv = new EntityCentricVisitor();
      te.visit(ccv);
      
      List<QueryModelNode> nodes = Lists.newArrayList(ccv.getCcNodes());
      
      Assert.assertEquals(3, nodes.size());
      List<String> cVarList = Lists.newArrayList();
      cVarList.add("i");
      cVarList.add("m");
      
      for(QueryModelNode q: nodes) {
          
          if(((EntityTupleSet)q).getStarQuery().getNodes().size() == 2) {
              String s = ((EntityTupleSet)q).getStarQuery().getCommonVarName();
              System.out.println("node is " + q  + " and common var is " + s);
              System.out.println("star query is " + ((EntityTupleSet)q).getStarQuery());
              Assert.assertTrue(cVarList.contains(s));
              cVarList.remove(s);
          } else if(((EntityTupleSet)q).getStarQuery().getNodes().size() == 3) {
              Assert.assertEquals("h", ((EntityTupleSet)q).getStarQuery().getCommonVarName());
          } else {
              Assert.assertTrue(false);
          }
      }
      
      
      System.out.println(te);

    }
    
    
    
    
 
 
 
 
 
 
 
    @Test
    public void testOptimizeConstantPriority() throws Exception {

        AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
        accc.setConf(conf);
        accc.setConnector(accCon);
        accc.setRdfEvalDAO(res);
        accc.init();

        BatchWriter bw1 = accCon.createBatchWriter("rya_prospects", config);
        BatchWriter bw2 = accCon.createBatchWriter("rya_selectivity", config);

        String s1 = "predicateobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:chickens";
        String s2 = "predicateobject" + DELIM + "uri:barksAt" + DELIM + "uri:chickens";
        String s3 = "predicate" + DELIM + "uri:peesOn";
        String s5 = "predicateobject" + DELIM + "uri:scratches" + DELIM + "uri:ears";
        String s4 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:chickens";
        List<Mutation> mList = new ArrayList<Mutation>();
        List<Mutation> mList2 = new ArrayList<Mutation>();
        List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "objectsubject",
                "objectpredicate", "objectobject");
        Mutation m1, m2, m3, m4, m5, m6;

        m1 = new Mutation(s1 + DELIM + "1");
        m1.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        m2 = new Mutation(s2 + DELIM + "1");
        m2.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        m3 = new Mutation(s3 + DELIM + "1");
        m3.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        m4 = new Mutation(s4 + DELIM + "1");
        m4.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        m5 = new Mutation(s5 + DELIM + "1");
        m5.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        mList.add(m1);
        mList.add(m2);
        mList.add(m3);
        mList.add(m4);
        mList.add(m5);

        bw1.addMutations(mList);
        bw1.close();

        m1 = new Mutation(s1);
        m2 = new Mutation(s2);
        m3 = new Mutation(s3);
        m4 = new Mutation(s4);
        m5 = new Mutation(s5);
        m6 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
        m6.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);

        for (String s : sList) {

            m1.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
            m2.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
            m3.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
            m4.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
            m5.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);

        }
        mList2.add(m1);
        mList2.add(m2);
        mList2.add(m3);
        mList2.add(m4);
        mList2.add(m5);
        mList2.add(m6);
        bw2.addMutations(mList2);
        bw2.close();

        TupleExpr te = getTupleExpr(q7);

        EntityOptimizer cco = new EntityOptimizer(accc);
        System.out.println("Originial query is " + te);
        cco.optimize(te, null, null);

        EntityCentricVisitor ccv = new EntityCentricVisitor();
        te.visit(ccv);

        List<QueryModelNode> nodes = Lists.newArrayList(ccv.getCcNodes());
        System.out.println("Test 7 nodes are :" + nodes);
        
        Assert.assertEquals(2, nodes.size());

        for (QueryModelNode q : nodes) {

            if (((EntityTupleSet) q).getStarQuery().getNodes().size() == 2) {
                Assert.assertEquals("m", ((EntityTupleSet) q).getStarQuery().getCommonVarName());
            } else if (((EntityTupleSet) q).getStarQuery().getNodes().size() == 3) {
                Assert.assertEquals("uri:chickens", ((EntityTupleSet) q).getStarQuery().getCommonVarName());
            } else {
                Assert.assertTrue(false);
            }
        }

        System.out.println(te);

    }
 
 
 
 
    
    
    @Test
    public void testOptimizeFilters() throws Exception {

        AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
        accc.setConf(conf);
        accc.setConnector(accCon);
        accc.setRdfEvalDAO(res);
        accc.init();

        BatchWriter bw1 = accCon.createBatchWriter("rya_prospects", config);
        BatchWriter bw2 = accCon.createBatchWriter("rya_selectivity", config);

        String s1 = "predicateobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:chickens";
        String s2 = "predicateobject" + DELIM + "uri:barksAt" + DELIM + "uri:chickens";
        String s3 = "predicate" + DELIM + "uri:peesOn";
        String s5 = "predicateobject" + DELIM + "uri:scratches" + DELIM + "uri:ears";
        String s4 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:chickens";
        List<Mutation> mList = new ArrayList<Mutation>();
        List<Mutation> mList2 = new ArrayList<Mutation>();
        List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject", "objectsubject",
                "objectpredicate", "objectobject");
        Mutation m1, m2, m3, m4, m5, m6;

        m1 = new Mutation(s1 + DELIM + "1");
        m1.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        m2 = new Mutation(s2 + DELIM + "1");
        m2.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        m3 = new Mutation(s3 + DELIM + "1");
        m3.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        m4 = new Mutation(s4 + DELIM + "1");
        m4.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        m5 = new Mutation(s5 + DELIM + "1");
        m5.put(new Text("count"), new Text(""), new Value("2".getBytes()));
        mList.add(m1);
        mList.add(m2);
        mList.add(m3);
        mList.add(m4);
        mList.add(m5);

        bw1.addMutations(mList);
        bw1.close();

        m1 = new Mutation(s1);
        m2 = new Mutation(s2);
        m3 = new Mutation(s3);
        m4 = new Mutation(s4);
        m5 = new Mutation(s5);
        m6 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
        m6.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);

        for (String s : sList) {

            m1.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
            m2.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
            m3.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
            m4.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
            m5.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);

        }
        mList2.add(m1);
        mList2.add(m2);
        mList2.add(m3);
        mList2.add(m4);
        mList2.add(m5);
        mList2.add(m6);
        bw2.addMutations(mList2);
        bw2.close();

        TupleExpr te = getTupleExpr(q8);
        (new FilterOptimizer()).optimize(te,null,null);
        
        EntityOptimizer cco = new EntityOptimizer(accc);
        System.out.println("Originial query is " + te);
        cco.optimize(te, null, null);
        

        EntityCentricVisitor ccv = new EntityCentricVisitor();
        te.visit(ccv);

        List<QueryModelNode> nodes = Lists.newArrayList(ccv.getCcNodes());
        System.out.println("Test 8 nodes are :" + nodes);
        
        Assert.assertEquals(2, nodes.size());

        for (QueryModelNode q : nodes) {

            if (((EntityTupleSet) q).getStarQuery().getNodes().size() == 2) {
                Assert.assertEquals("m", ((EntityTupleSet) q).getStarQuery().getCommonVarName());
            } else if (((EntityTupleSet) q).getStarQuery().getNodes().size() == 3) {
                Assert.assertEquals("uri:chickens", ((EntityTupleSet) q).getStarQuery().getCommonVarName());
            } else {
                Assert.assertTrue(false);
            }
        }

        System.out.println(te);

    }
 
    
    
    
    
    
    @Test
    public void testOptimizeFilter2() throws Exception {

      AccumuloSelectivityEvalDAO accc = new AccumuloSelectivityEvalDAO();
      accc.setConf(conf);
      accc.setConnector(accCon);
      accc.setRdfEvalDAO(res);
      accc.init();

      BatchWriter bw1 = accCon.createBatchWriter("rya_prospects", config);
      BatchWriter bw2 = accCon.createBatchWriter("rya_selectivity", config);

      String s1 = "predicateobject" + DELIM + "http://www.w3.org/2000/01/rdf-schema#label" + DELIM + "uri:dog";
      String s2 = "predicateobject" + DELIM + "uri:barksAt" + DELIM + "uri:cat";
      String s3 = "predicateobject" + DELIM + "uri:peesOn" + DELIM + "uri:hydrant";
      String s5 = "predicateobject" + DELIM + "uri:scratches" + DELIM + "uri:ears";
      String s4 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:chickens";
      String s6 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:kibble";
      String s7 = "predicateobject" + DELIM + "uri:rollsIn" + DELIM + "uri:mud";
      String s8 = "predicateobject" + DELIM + "uri:runsIn" + DELIM + "uri:field";
      String s9 = "predicate" + DELIM + "uri:smells" ;
      String s10 = "predicateobject" + DELIM + "uri:eats" + DELIM + "uri:sticks";
      String s11 = "predicate" + DELIM + "uri:watches";
      

      
      
      List<Mutation> mList = new ArrayList<Mutation>();
      List<Mutation> mList2 = new ArrayList<Mutation>();
      List<String> sList = Arrays.asList("subjectobject", "subjectpredicate", "subjectsubject","objectsubject", "objectpredicate", "objectobject");
      Mutation m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11, m12;

      m1 = new Mutation(s1 + DELIM + "1");
      m1.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m2 = new Mutation(s2 + DELIM + "1");
      m2.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m3 = new Mutation(s3 + DELIM + "1");
      m3.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m4 = new Mutation(s4 + DELIM + "1");
      m4.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m5 = new Mutation(s5 + DELIM + "1");
      m5.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m6 = new Mutation(s6 + DELIM + "1");
      m6.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m7 = new Mutation(s7 + DELIM + "1");
      m7.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m8 = new Mutation(s8 + DELIM + "1");
      m8.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m9 = new Mutation(s9 + DELIM + "1");
      m9.put(new Text("count"), new Text(""), new Value("2".getBytes()));
      m10 = new Mutation(s10 + DELIM + "1");
      m10.put(new Text("count"), new Text(""), new Value("1".getBytes()));
      m11 = new Mutation(s11 + DELIM + "1");
      m11.put(new Text("count"), new Text(""), new Value("2".getBytes()));

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
      mList.add(m11);

      bw1.addMutations(mList);
      bw1.close();


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
      m11 = new Mutation(s11);
      m12 = new Mutation(new Text("subjectpredicateobject" + DELIM + "FullTableCardinality"));
      m12.put(new Text("FullTableCardinality"), new Text("100"), EMPTY_VAL);
     

      for (String s : sList) {
       
        m1.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m2.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m3.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m4.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m5.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m6.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m7.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);
        m8.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);
        m9.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);
        m10.put(new Text(s), new Text(Integer.toString(1)), EMPTY_VAL);
        m11.put(new Text(s), new Text(Integer.toString(2)), EMPTY_VAL);

       
      }
      mList2.add(m1);
      mList2.add(m2);
      mList2.add(m3);
      mList2.add(m4);
      mList2.add(m5);
      mList2.add(m6);
      mList2.add(m7);
      mList2.add(m8);
      mList2.add(m9);
      mList2.add(m10);
      mList2.add(m11);
      mList2.add(m12);
      bw2.addMutations(mList2);
      bw2.close();



      TupleExpr te = getTupleExpr(q9);
      System.out.println(te);
      (new FilterOptimizer()).optimize(te,null,null);

      EntityOptimizer cco = new EntityOptimizer(accc);
      System.out.println("Originial query is " + te);
      cco.optimize(te, null, null);

      EntityCentricVisitor ccv = new EntityCentricVisitor();
      te.visit(ccv);
      
      List<QueryModelNode> nodes = Lists.newArrayList(ccv.getCcNodes());
      
      Assert.assertEquals(3, nodes.size());
      List<String> cVarList = Lists.newArrayList();
      cVarList.add("i");
      cVarList.add("m");
      
      for(QueryModelNode q: nodes) {
          
          if(((EntityTupleSet)q).getStarQuery().getNodes().size() == 2) {
              String s = ((EntityTupleSet)q).getStarQuery().getCommonVarName();
              System.out.println("node is " + q  + " and common var is " + s);
              System.out.println("star query is " + ((EntityTupleSet)q).getStarQuery());
              Assert.assertTrue(cVarList.contains(s));
              cVarList.remove(s);
          } else if(((EntityTupleSet)q).getStarQuery().getNodes().size() == 3) {
              Assert.assertEquals("h", ((EntityTupleSet)q).getStarQuery().getCommonVarName());
          } else {
              Assert.assertTrue(false);
          }
      }
      
      
      System.out.println(te);

    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    private TupleExpr getTupleExpr(String query) throws MalformedQueryException {

        SPARQLParser sp = new SPARQLParser();
        ParsedQuery pq = sp.parseQuery(query, null);

        return pq.getTupleExpr();
      }
    
    
    
    
    
    
    private class EntityCentricVisitor extends QueryModelVisitorBase<RuntimeException> {
        
        private Set<QueryModelNode> ccNodes = Sets.newHashSet();
        
        public Set<QueryModelNode> getCcNodes() {
            return ccNodes;
        }
        
        
        public void meetNode(QueryModelNode node) {
            
            if(node instanceof EntityTupleSet) {
                ccNodes.add(node);
            }
            
            super.meetNode(node);
        }
        
        
        
        
    }
    
    
    
    
    
    
    
    

}
