package mvm.rya.indexing.accumulo.entity;

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


import info.aduna.iteration.CloseableIteration;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.RyaTableMutationsFactory;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.api.resolver.RyaTripleContext;
import mvm.rya.indexing.accumulo.ConfigUtils;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Lists;

public class AccumuloDocIndexerTest {

    private MockInstance mockInstance;
    private Connector accCon;
    AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
    ValueFactory vf = new ValueFactoryImpl();
    
    private String tableName;
    
    
    @Before
    public void init() throws Exception {
        final String INSTANCE = "instance";
        Configuration config = new Configuration();
        config.set(ConfigUtils.CLOUDBASE_AUTHS, "U");
        config.set(ConfigUtils.CLOUDBASE_INSTANCE, INSTANCE);
        config.set(ConfigUtils.CLOUDBASE_USER, "root");
        config.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
       
        conf = new AccumuloRdfConfiguration(config);
        conf.set(ConfigUtils.USE_MOCK_INSTANCE, "true");
        conf.setAdditionalIndexers(EntityCentricIndex.class);
        conf.setTablePrefix("EntityCentric_");
        tableName =  EntityCentricIndex.getTableName(conf);

        // Access the accumulo instance.  If you assign a name, it persists statically, but otherwise, can't get it by name.
        accCon = new MockInstance(INSTANCE).getConnector("root", new PasswordToken(""));
        if(accCon.tableOperations().exists(tableName)) {
                throw new Exception("New mock accumulo already has a table!  Should be deleted in AfterTest.");
        } 
        // This should happen in the index initialization, but some tests need it before: 
        accCon.tableOperations().create(tableName);
    }

    @After
    public void afterTest() throws Exception {
        if (accCon.tableOperations().exists(tableName)) {
            accCon.tableOperations().delete(tableName);
        }
    }

    @Test
    public void testNoContext1()  throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));

          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);
         
          
          for (int i = 0; i < 20; i++) {
                      
              
                    RyaStatement rs1 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaType(XMLSchema.STRING, "cq1"));
                    RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"));
                    RyaStatement rs3 = null;
                    RyaStatement rs4 = null;
                   
                    if(i == 5 || i == 15) {
                        rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)));
                        rs4 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.STRING,Integer.toString(i)));
                    }
        
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize4 = null;
                    
                    if(rs3 != null) {
                        serialize3 = rtm.serialize(rs3);
                    }
                    
                    if(rs4 != null) {
                        serialize4 = rtm.serialize(rs4);
                    }
                    
                    
                    Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
                    for (Mutation m : m1) {
                        bw.addMutation(m);
                    }
                    Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
                    for (Mutation m : m2) {
                        bw.addMutation(m);
                    }
                    if (serialize3 != null) {
                        Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                        for (Mutation m : m3) {
                            bw.addMutation(m);
                        }
                    }
                    if (serialize4 != null) {
                        Collection<Mutation> m4 = EntityCentricIndex.createMutations(rs4);
                        for (Mutation m : m4) {
                            bw.addMutation(m);
                        }
                    }
         
                    
                    
                    

          }
         
          String q1 = "" //
                  + "SELECT ?X ?Y1 ?Y2 " //
                  + "{"//
                  +  "?X <uri:cf1> ?Y1 ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> 5 ."//
                  +  "}";
          
          
          String q2 = "" //
                  + "SELECT ?X ?Y1 ?Y2 " //
                  + "{"//
                  +  "?X <uri:cf1> ?Y1  ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> \"15\" ."//
                  +  "}";
          
               
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);
                ParsedQuery pq2 = parser.parseQuery(q2, null);

                TupleExpr te1 = pq1.getTupleExpr();
                TupleExpr te2 = pq2.getTupleExpr();

                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                List<StatementPattern> spList2 = StatementPatternCollector.process(te2);

                StarQuery sq1 = new StarQuery(spList1);
                StarQuery sq2 = new StarQuery(spList2);
                
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                List<BindingSet> bsList = Lists.newArrayList();
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                CloseableIteration<BindingSet, QueryEvaluationException> sol2 = adi.queryDocIndex(sq2, bsList);
                
                System.out.println("******************Test 1************************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(1, results);
                
                System.out.println("************************************************");
                results = 0;
                while(sol2.hasNext()) {
                    System.out.println(sol2.next());
                    results++;
                }
                
                Assert.assertEquals(1, results);
                
                
                
                adi.close();
                
                
                
                


    }
    
    
    
    
    
    
    @Test
    public void testNoContext2() throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));


          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);
          
          for (int i = 0; i < 30; i++) {
                      
              
                    RyaStatement rs1 = new RyaStatement(new RyaURI("uri:cq1"), new RyaURI("uri:cf1"), new RyaURI("uri:" + i ));
                    RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"));
                    RyaStatement rs3 = null;
                  
                   
                    if(i == 5 || i == 10 || i == 15 || i == 20 || i == 25) {
                        rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)));
                    }
        
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
                   
                    
                    if(rs3 != null) {
                        serialize3 = rtm.serialize(rs3);
                    }
                    
                    
                    
                    Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
                    for (Mutation m : m1) {
                        bw.addMutation(m);
                    }
                    Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
                    for (Mutation m : m2) {
                        bw.addMutation(m);
                    }
                    if (serialize3 != null) {
                        Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                        for (Mutation m : m3) {
                            bw.addMutation(m);
                        }
                    }
                    
                    
                    
                    

          }
         
          String q1 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  +  "?Y1 <uri:cf1> ?X ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  +  "}";
          
          
          
               
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);

                TupleExpr te1 = pq1.getTupleExpr();
                
                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
               
                StarQuery sq1 = new StarQuery(spList1);
               
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                
                
                List<BindingSet> bsList = Lists.newArrayList();
//                QueryBindingSet b1 = (new QueryBindingSet());
//                b1.addBinding("X", vf.createURI("uri:5"));
//                QueryBindingSet b2 = (new QueryBindingSet());
//                b2.addBinding("X", vf.createURI("uri:15"));
//                QueryBindingSet b3 = (new QueryBindingSet());
//                b3.addBinding("X", vf.createURI("uri:25"));
//                bsList.add(b1);
//                bsList.add(b2);
//                bsList.add(b3);
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                
                System.out.println("**********************TEST 2***********************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(5, results);
                
                
                adi.close();
                   


    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    @Test
    public void testNoContextCommonVarBs() throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));


          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

         
          
          for (int i = 0; i < 30; i++) {
                      
              
                    RyaStatement rs1 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaType(XMLSchema.STRING, "cq1"));
                    RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"));
                    RyaStatement rs3 = null;
                  
                   
                    if(i == 5 || i == 10 || i == 15 || i == 20 || i == 25) {
                        rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)));
                    }
        
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
                   
                    
                    if(rs3 != null) {
                        serialize3 = rtm.serialize(rs3);
                    }
                    
                    
                    
                    Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
                    for (Mutation m : m1) {
                        bw.addMutation(m);
                    }
                    Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
                    for (Mutation m : m2) {
                        bw.addMutation(m);
                    }
                    if (serialize3 != null) {
                        Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                        for (Mutation m : m3) {
                            bw.addMutation(m);
                        }
                    }
                    
                    
                    
                    

          }
         
          String q1 = "" //
                  + "SELECT ?X ?Y1 ?Y2 " //
                  + "{"//
                  +  "?X <uri:cf1> ?Y1 ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  +  "}";
          
          
          
               
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);

                TupleExpr te1 = pq1.getTupleExpr();
                
                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
               
                StarQuery sq1 = new StarQuery(spList1);
               
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                
                
                List<BindingSet> bsList = Lists.newArrayList();
                QueryBindingSet b1 = (new QueryBindingSet());
                b1.addBinding("X", vf.createURI("uri:5"));
                QueryBindingSet b2 = (new QueryBindingSet());
                b2.addBinding("X", vf.createURI("uri:15"));
                QueryBindingSet b3 = (new QueryBindingSet());
                b3.addBinding("X", vf.createURI("uri:25"));
                bsList.add(b1);
                bsList.add(b2);
                bsList.add(b3);
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                
                System.out.println("**********************TEST 3***********************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(3, results);
                
                
                adi.close();
                   


    }
    


    
    
    @Test
    public void testNoContextUnCommonVarBs() throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));


          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

         
          
          for (int i = 0; i < 30; i++) {
                      
              
                    RyaStatement rs1 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaType(XMLSchema.STRING, "cq1"));
                    RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"));
                    RyaStatement rs3 = null;
                    
                   
                    if(i == 5 || i == 10 || i == 15 || i == 20 || i == 25) {
                        rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)));
                    }
        
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
                   
                    
                    if(rs3 != null) {
                        serialize3 = rtm.serialize(rs3);
                    }
                    
                    
                    
                    Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
                    for (Mutation m : m1) {
                        bw.addMutation(m);
                    }
                    Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
                    for (Mutation m : m2) {
                        bw.addMutation(m);
                    }
                    if (serialize3 != null) {
                        Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                        for (Mutation m : m3) {
                            bw.addMutation(m);
                        }
                    }
                    
                    
                    
                    

          }
         
          String q1 = "" //
                  + "SELECT ?X ?Y1 ?Y2 " //
                  + "{"//
                  +  "?X <uri:cf1> ?Y1 ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  +  "}";
          
          
          
               
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);

                TupleExpr te1 = pq1.getTupleExpr();
                
                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
               
                StarQuery sq1 = new StarQuery(spList1);
               
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                
                Value v1 = RyaToRdfConversions.convertValue(new RyaType(XMLSchema.INTEGER,Integer.toString(5)));
                Value v2 = RyaToRdfConversions.convertValue(new RyaType(XMLSchema.INTEGER,Integer.toString(25)));
                
                List<BindingSet> bsList = Lists.newArrayList();
                QueryBindingSet b1 = (new QueryBindingSet());
                b1.addBinding("Y3", v1);
                QueryBindingSet b2 = (new QueryBindingSet());
                b2.addBinding("Y3", v2);
                bsList.add(b1);
                bsList.add(b2);
                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                
                System.out.println("**********************TEST 4***********************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(2, results);
                
                
                adi.close();
                   


    }
    
    
    
    @Test
    public void testNoContextCommonVarBs2() throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));


          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

         
          
          for (int i = 0; i < 30; i++) {
                      
              
                    RyaStatement rs1 = new RyaStatement(new RyaURI("uri:cq1"), new RyaURI("uri:cf1"), new RyaURI("uri:" + i ));
                    RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"));
                    RyaStatement rs3 = null;
                  
                   
                    if(i == 5 || i == 10 || i == 15 || i == 20 || i == 25) {
                        rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)));
                    }
        
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
                   
                    
                    if(rs3 != null) {
                        serialize3 = rtm.serialize(rs3);
                    }
                    
                    
                    
                    Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
                    for (Mutation m : m1) {
                        bw.addMutation(m);
                    }
                    Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
                    for (Mutation m : m2) {
                        bw.addMutation(m);
                    }
                    if (serialize3 != null) {
                        Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                        for (Mutation m : m3) {
                            bw.addMutation(m);
                        }
                    }
                    
                    
                    
                    

          }
         
          String q1 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  +  "?Y1 <uri:cf1> ?X ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  +  "}";
          
          
          
               
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);

                TupleExpr te1 = pq1.getTupleExpr();
                
                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
               
                StarQuery sq1 = new StarQuery(spList1);
               
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                
                
                List<BindingSet> bsList = Lists.newArrayList();
                QueryBindingSet b1 = (new QueryBindingSet());
                b1.addBinding("X", vf.createURI("uri:5"));
                QueryBindingSet b2 = (new QueryBindingSet());
                b2.addBinding("X", vf.createURI("uri:15"));
                QueryBindingSet b3 = (new QueryBindingSet());
                b3.addBinding("X", vf.createURI("uri:25"));
                bsList.add(b1);
                bsList.add(b2);
                bsList.add(b3);
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                
                System.out.println("**********************TEST 5***********************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(3, results);
                
                
                adi.close();
                   


    }
    
    
    
    
    
    @Test
    public void testNoContextUnCommonVarBs2() throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));


          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

         
          
          for (int i = 0; i < 30; i++) {
                      
              
                    RyaStatement rs1 = new RyaStatement(new RyaURI("uri:cq1"), new RyaURI("uri:cf1"), new RyaURI("uri:" + i ));
                    RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"));
                    RyaStatement rs3 = null;
                  
                   
                    if(i == 5 || i == 10 || i == 15 || i == 20 || i == 25) {
                        rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)));
                    }
        
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
                   
                    
                    if(rs3 != null) {
                        serialize3 = rtm.serialize(rs3);
                    }
                    
                    
                    
                    Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
                    for (Mutation m : m1) {
                        bw.addMutation(m);
                    }
                    Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
                    for (Mutation m : m2) {
                        bw.addMutation(m);
                    }
                    if (serialize3 != null) {
                        Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                        for (Mutation m : m3) {
                            bw.addMutation(m);
                        }
                    }
                    
                    
                    
                    

          }
         
          String q1 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  +  "?Y1 <uri:cf1> ?X ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  +  "}";
          
          
               
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);

                TupleExpr te1 = pq1.getTupleExpr();
                
                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
               
                StarQuery sq1 = new StarQuery(spList1);
               
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                
                
                List<BindingSet> bsList = Lists.newArrayList();
                QueryBindingSet b1 = (new QueryBindingSet());
                b1.addBinding("X", vf.createURI("uri:5"));
                QueryBindingSet b2 = (new QueryBindingSet());
                b2.addBinding("X", vf.createURI("uri:15"));
                QueryBindingSet b3 = (new QueryBindingSet());
                b3.addBinding("X", vf.createURI("uri:25"));
                bsList.add(b1);
                bsList.add(b2);
                bsList.add(b3);
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                
                System.out.println("**********************TEST 6***********************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(3, results);
                
                
                adi.close();
                   


    }
    
    
    

    
    
    
    @Test
    public void testContext2() throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));


          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

         
          
          for (int i = 0; i < 30; i++) {
                      
              
                    RyaStatement rs1 = new RyaStatement(new RyaURI("uri:cq1"), new RyaURI("uri:cf1"), new RyaURI("uri:" + i ), new RyaURI("uri:joe"));
                    RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:joe"));
                    RyaStatement rs3 = null;
                    
                    RyaStatement rs4 = new RyaStatement(new RyaURI("uri:cq1"), new RyaURI("uri:cf1"), new RyaURI("uri:" + i ), new RyaURI("uri:hank"));
                    RyaStatement rs5 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:hank"));
                    RyaStatement rs6 = null;
                  
                   
                    if(i == 5 || i == 10 || i == 15 || i == 20 || i == 25) {
                        rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:joe"));
                        rs6 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:hank"));
                    }
        
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize4 = rtm.serialize(rs4);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize5 = rtm.serialize(rs5);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize6 = null;
                   
                    
                    if(rs3 != null) {
                        serialize3 = rtm.serialize(rs3);
                    }
                    
                    if(rs6 != null) {
                        serialize6 = rtm.serialize(rs6);
                    }
                    
                    
                    
                    Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
                    for (Mutation m : m1) {
                        bw.addMutation(m);
                    }
                    Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
                    for (Mutation m : m2) {
                        bw.addMutation(m);
                    }
                    if (serialize3 != null) {
                        Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                        for (Mutation m : m3) {
                            bw.addMutation(m);
                        }
                    }
                    
                    Collection<Mutation> m4 = EntityCentricIndex.createMutations(rs4);
                    for (Mutation m : m4) {
                        bw.addMutation(m);
                    }
                    Collection<Mutation> m5 = EntityCentricIndex.createMutations(rs5);
                    for (Mutation m : m5) {
                        bw.addMutation(m);
                    }
                    if (serialize3 != null) {
                        Collection<Mutation> m6 = EntityCentricIndex.createMutations(rs6);
                        for (Mutation m : m6) {
                            bw.addMutation(m);
                        }
                    }
                    
                    
                    
                    

          }
         
          String q1 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  + " GRAPH <uri:joe> { " //
                  +  "?Y1 <uri:cf1> ?X ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  + " } "//
                  +  "}";
          
          

          String q2 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  +  "?Y1 <uri:cf1> ?X ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  +  "}";
          
          
          
          
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);

                TupleExpr te1 = pq1.getTupleExpr();
                
                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
               
                StarQuery sq1 = new StarQuery(spList1);
               
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                
                
                List<BindingSet> bsList = Lists.newArrayList();
//                QueryBindingSet b1 = (new QueryBindingSet());
//                b1.addBinding("X", vf.createURI("uri:5"));
//                QueryBindingSet b2 = (new QueryBindingSet());
//                b2.addBinding("X", vf.createURI("uri:15"));
//                QueryBindingSet b3 = (new QueryBindingSet());
//                b3.addBinding("X", vf.createURI("uri:25"));
//                bsList.add(b1);
//                bsList.add(b2);
//                bsList.add(b3);
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                
                System.out.println("**********************TEST 7***********************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(5, results);
                
                
                
                ParsedQuery pq2 = parser.parseQuery(q2, null);

                TupleExpr te2 = pq2.getTupleExpr();
                
                List<StatementPattern> spList2 = StatementPatternCollector.process(te2);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList2));
               
                StarQuery sq2 = new StarQuery(spList2);
                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol2 = adi.queryDocIndex(sq2, bsList);
                
                System.out.println("**********************TEST 7***********************");
                results = 0;
                while(sol2.hasNext()) {
                    System.out.println(sol2.next());
                    results++;
                }
                Assert.assertEquals(10, results);
                
                
                adi.close();
                   


    }
    
    
    
    
    
    
    @Test
    public void testContextUnCommonVarBs2() throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));


          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

         
          
          for (int i = 0; i < 30; i++) {
                      
              
                    RyaStatement rs1 = new RyaStatement(new RyaURI("uri:cq1"), new RyaURI("uri:cf1"), new RyaURI("uri:" + i ), new RyaURI("uri:joe"));
                    RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:joe"));
                    RyaStatement rs3 = null;
                    
                    RyaStatement rs4 = new RyaStatement(new RyaURI("uri:cq1"), new RyaURI("uri:cf1"), new RyaURI("uri:" + i ), new RyaURI("uri:hank"));
                    RyaStatement rs5 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:hank"));
                    RyaStatement rs6 = null;
                  
                   
                    if(i == 5 || i == 10 || i == 15 || i == 20 || i == 25) {
                        rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:joe"));
                        rs6 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:hank"));
                    }
        
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize4 = rtm.serialize(rs4);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize5 = rtm.serialize(rs5);
                    Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize6 = null;
                   
                    
                    if(rs3 != null) {
                        serialize3 = rtm.serialize(rs3);
                    }
                    
                    if(rs6 != null) {
                        serialize6 = rtm.serialize(rs6);
                    }
                    
                    
                    
                    Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
                    for (Mutation m : m1) {
                        bw.addMutation(m);
                    }
                    Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
                    for (Mutation m : m2) {
                        bw.addMutation(m);
                    }
                    if (serialize3 != null) {
                        Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                        for (Mutation m : m3) {
                            bw.addMutation(m);
                        }
                    }
                    
                    Collection<Mutation> m4 = EntityCentricIndex.createMutations(rs4);
                    for (Mutation m : m4) {
                        bw.addMutation(m);
                    }
                    Collection<Mutation> m5 = EntityCentricIndex.createMutations(rs5);
                    for (Mutation m : m5) {
                        bw.addMutation(m);
                    }
                    if (serialize3 != null) {
                        Collection<Mutation> m6 = EntityCentricIndex.createMutations(rs6);
                        for (Mutation m : m6) {
                            bw.addMutation(m);
                        }
                    }
                    
                    
                    
                    

          }
         
          String q1 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  + " GRAPH <uri:joe> { " //
                  +  "?Y1 <uri:cf1> ?X ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  + " } "//
                  +  "}";
          
          

          String q2 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  +  "?Y1 <uri:cf1> ?X ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  +  "}";
          
          
          
          
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);

                TupleExpr te1 = pq1.getTupleExpr();
                
                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
               
                StarQuery sq1 = new StarQuery(spList1);
               
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                
                
                List<BindingSet> bsList = Lists.newArrayList();
                QueryBindingSet b1 = (new QueryBindingSet());
                b1.addBinding("X", vf.createURI("uri:5"));
                QueryBindingSet b2 = (new QueryBindingSet());
                b2.addBinding("X", vf.createURI("uri:15"));
                QueryBindingSet b3 = (new QueryBindingSet());
                b3.addBinding("X", vf.createURI("uri:25"));
                bsList.add(b1);
                bsList.add(b2);
                bsList.add(b3);
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                
                System.out.println("**********************TEST 8***********************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(3, results);
                
                
                
                ParsedQuery pq2 = parser.parseQuery(q2, null);

                TupleExpr te2 = pq2.getTupleExpr();
                
                List<StatementPattern> spList2 = StatementPatternCollector.process(te2);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList2));
               
                StarQuery sq2 = new StarQuery(spList2);
                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol2 = adi.queryDocIndex(sq2, bsList);
                
                System.out.println("**********************TEST 8***********************");
                results = 0;
                while(sol2.hasNext()) {
                    System.out.println(sol2.next());
                    results++;
                }
                Assert.assertEquals(6, results);
                
                
                adi.close();
                   



    }
    
    
    @Test
    public void testContext1() throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));


          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

         
          
          for (int i = 0; i < 30; i++) {
              
              
              RyaStatement rs1 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaURI("uri:cq1"),  new RyaURI("uri:joe"));
              RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:joe"));
              RyaStatement rs3 = null;
              
              RyaStatement rs4 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaURI("uri:cq1"), new RyaURI("uri:hank"));
              RyaStatement rs5 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:hank"));
              RyaStatement rs6 = null;
            
             
              if(i == 5 || i == 10 || i == 15 || i == 20 || i == 25) {
                  rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:joe"));
                  rs6 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:hank"));
              }
  
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize4 = rtm.serialize(rs4);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize5 = rtm.serialize(rs5);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize6 = null;
             
              
              if(rs3 != null) {
                  serialize3 = rtm.serialize(rs3);
              }
              
              if(rs6 != null) {
                  serialize6 = rtm.serialize(rs6);
              }
              
              
              
              Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
              for (Mutation m : m1) {
                  bw.addMutation(m);
              }
              Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
              for (Mutation m : m2) {
                  bw.addMutation(m);
              }
              if (serialize3 != null) {
                  Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                  for (Mutation m : m3) {
                      bw.addMutation(m);
                  }
              }
              
              Collection<Mutation> m4 = EntityCentricIndex.createMutations(rs4);
              for (Mutation m : m4) {
                  bw.addMutation(m);
              }
              Collection<Mutation> m5 = EntityCentricIndex.createMutations(rs5);
              for (Mutation m : m5) {
                  bw.addMutation(m);
              }
              if (serialize6 != null) {
                  Collection<Mutation> m6 = EntityCentricIndex.createMutations(rs6);
                  for (Mutation m : m6) {
                      bw.addMutation(m);
                  }
              }
              
              
              
              

    }
         
          String q1 = "" //
                  + "SELECT ?X ?Y1 ?Y2 " //
                  + "{"//
                  +  "?X <uri:cf1> ?Y1 ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  +  "}";
          
          
          
          String q2 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  + " GRAPH <uri:hank> { " //
                  +  "?X <uri:cf1> ?Y1 ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  + " } "//
                  +  "}";
          
          
               
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);

                TupleExpr te1 = pq1.getTupleExpr();
                
                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
               
                StarQuery sq1 = new StarQuery(spList1);
               
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                
//                Value v1 = RyaToRdfConversions.convertValue(new RyaType(XMLSchema.INTEGER,Integer.toString(5)));
//                Value v2 = RyaToRdfConversions.convertValue(new RyaType(XMLSchema.INTEGER,Integer.toString(25)));
                
                List<BindingSet> bsList = Lists.newArrayList();
//                QueryBindingSet b1 = (new QueryBindingSet());
//                b1.addBinding("Y3", v1);
//                QueryBindingSet b2 = (new QueryBindingSet());
//                b2.addBinding("Y3", v2);
//                bsList.add(b1);
//                bsList.add(b2);
//                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                
                System.out.println("**********************TEST 10***********************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(10, results);
                
                
                
                
                ParsedQuery pq2 = parser.parseQuery(q2, null);

                TupleExpr te2 = pq2.getTupleExpr();
                
                List<StatementPattern> spList2 = StatementPatternCollector.process(te2);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList2));
               
                StarQuery sq2 = new StarQuery(spList2);
                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol2 = adi.queryDocIndex(sq2, bsList);
                
                System.out.println("**********************TEST 10***********************");
                results = 0;
                while(sol2.hasNext()) {
                    System.out.println(sol2.next());
                    results++;
                }
                Assert.assertEquals(5, results);
                
                
                adi.close();
                   



    }
    
    
    
    
    
    
    
    
    @Test
    public void testContextUnCommonVarBs1() throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));


          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

         
          
          for (int i = 0; i < 30; i++) {
              
              
              RyaStatement rs1 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaURI("uri:cq1"),  new RyaURI("uri:joe"));
              RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:joe"));
              RyaStatement rs3 = null;
              
              RyaStatement rs4 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaURI("uri:cq1"), new RyaURI("uri:hank"));
              RyaStatement rs5 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:hank"));
              RyaStatement rs6 = null;
            
             
              if(i == 5 || i == 10 || i == 15 || i == 20 || i == 25) {
                  rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:joe"));
                  rs6 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:hank"));
              }
  
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize4 = rtm.serialize(rs4);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize5 = rtm.serialize(rs5);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize6 = null;
             
              
              if(rs3 != null) {
                  serialize3 = rtm.serialize(rs3);
              }
              
              if(rs6 != null) {
                  serialize6 = rtm.serialize(rs6);
              }
              
              
              
              Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
              for (Mutation m : m1) {
                  bw.addMutation(m);
              }
              Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
              for (Mutation m : m2) {
                  bw.addMutation(m);
              }
              if (serialize3 != null) {
                  Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                  for (Mutation m : m3) {
                      bw.addMutation(m);
                  }
              }
              
              Collection<Mutation> m4 = EntityCentricIndex.createMutations(rs4);
              for (Mutation m : m4) {
                  bw.addMutation(m);
              }
              Collection<Mutation> m5 = EntityCentricIndex.createMutations(rs5);
              for (Mutation m : m5) {
                  bw.addMutation(m);
              }
              if (serialize6 != null) {
                  Collection<Mutation> m6 = EntityCentricIndex.createMutations(rs6);
                  for (Mutation m : m6) {
                      bw.addMutation(m);
                  }
              }
              
              
              
              

    }
         
          String q1 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  +  "?X <uri:cf1> ?Y1 ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  +  "}";
          
          
          
          String q2 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  + " GRAPH <uri:hank> { " //
                  +  "?X <uri:cf1> ?Y1 ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  + " } "//
                  +  "}";
          
          
               
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);

                TupleExpr te1 = pq1.getTupleExpr();
                
                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
               
                StarQuery sq1 = new StarQuery(spList1);
               
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                
                Value v1 = RyaToRdfConversions.convertValue(new RyaType(XMLSchema.INTEGER,Integer.toString(5)));
                Value v2 = RyaToRdfConversions.convertValue(new RyaType(XMLSchema.INTEGER,Integer.toString(25)));
                
                List<BindingSet> bsList = Lists.newArrayList();
                QueryBindingSet b1 = (new QueryBindingSet());
                b1.addBinding("Y3", v1);
                QueryBindingSet b2 = (new QueryBindingSet());
                b2.addBinding("Y3", v2);
                bsList.add(b1);
                bsList.add(b2);
                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                
                System.out.println("**********************TEST 11***********************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(4, results);
                
                
                
                
                ParsedQuery pq2 = parser.parseQuery(q2, null);

                TupleExpr te2 = pq2.getTupleExpr();
                
                List<StatementPattern> spList2 = StatementPatternCollector.process(te2);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList2));
               
                StarQuery sq2 = new StarQuery(spList2);
                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol2 = adi.queryDocIndex(sq2, bsList);
                
                System.out.println("**********************TEST 11***********************");
                results = 0;
                while(sol2.hasNext()) {
                    System.out.println(sol2.next());
                    results++;
                }
                Assert.assertEquals(2, results);
                
                
                adi.close();
                   



    }
    
    
    
    
    


    
    
    
    @Test
    public void testContextCommonVarBs1() throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));


          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

         
          
          for (int i = 0; i < 30; i++) {
              
              
              RyaStatement rs1 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaURI("uri:cq1"),  new RyaURI("uri:joe"));
              RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:joe"));
              RyaStatement rs3 = null;
              
              RyaStatement rs4 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaURI("uri:cq1"), new RyaURI("uri:hank"));
              RyaStatement rs5 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:hank"));
              RyaStatement rs6 = null;
            
             
              if(i == 5 || i == 10 || i == 15 || i == 20 || i == 25) {
                  rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:joe"));
                  rs6 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:hank"));
              }
  
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize4 = rtm.serialize(rs4);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize5 = rtm.serialize(rs5);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize6 = null;
             
              
              if(rs3 != null) {
                  serialize3 = rtm.serialize(rs3);
              }
              
              if(rs6 != null) {
                  serialize6 = rtm.serialize(rs6);
              }
              
              
              
              Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
              for (Mutation m : m1) {
                  bw.addMutation(m);
              }
              Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
              for (Mutation m : m2) {
                  bw.addMutation(m);
              }
              if (serialize3 != null) {
                  Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                  for (Mutation m : m3) {
                      bw.addMutation(m);
                  }
              }
              
              Collection<Mutation> m4 = EntityCentricIndex.createMutations(rs4);
              for (Mutation m : m4) {
                  bw.addMutation(m);
              }
              Collection<Mutation> m5 = EntityCentricIndex.createMutations(rs5);
              for (Mutation m : m5) {
                  bw.addMutation(m);
              }
              if (serialize6 != null) {
                  Collection<Mutation> m6 = EntityCentricIndex.createMutations(rs6);
                  for (Mutation m : m6) {
                      bw.addMutation(m);
                  }
              }
              
              
              
              

    }
         
          String q1 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  +  "?X <uri:cf1> ?Y1 ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  +  "}";
          
          
          
          String q2 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  + " GRAPH <uri:hank> { " //
                  +  "?X <uri:cf1> ?Y1 ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  + " } "//
                  +  "}";
          
          
               
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);

                TupleExpr te1 = pq1.getTupleExpr();
                
                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
               
                StarQuery sq1 = new StarQuery(spList1);
               
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                
                Value v1 = RyaToRdfConversions.convertValue(new RyaType(XMLSchema.INTEGER,Integer.toString(5)));
                Value v2 = RyaToRdfConversions.convertValue(new RyaType(XMLSchema.INTEGER,Integer.toString(25)));
                
                List<BindingSet> bsList = Lists.newArrayList();
                QueryBindingSet b1 = (new QueryBindingSet());
                b1.addBinding("X", vf.createURI("uri:5"));
                QueryBindingSet b2 = (new QueryBindingSet());
                b2.addBinding("X", vf.createURI("uri:15"));
                QueryBindingSet b3 = (new QueryBindingSet());
                b3.addBinding("X", vf.createURI("uri:25"));
                bsList.add(b1);
                bsList.add(b2);
                bsList.add(b3);
                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                
                System.out.println("**********************TEST 12***********************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(6, results);
                
                
                
                
                ParsedQuery pq2 = parser.parseQuery(q2, null);

                TupleExpr te2 = pq2.getTupleExpr();
                
                List<StatementPattern> spList2 = StatementPatternCollector.process(te2);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList2));
               
                StarQuery sq2 = new StarQuery(spList2);
                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol2 = adi.queryDocIndex(sq2, bsList);
                
                System.out.println("**********************TEST 12***********************");
                results = 0;
                while(sol2.hasNext()) {
                    System.out.println(sol2.next());
                    results++;
                }
                Assert.assertEquals(3, results);
                
                
                adi.close();
                   



    }
    
    
    
    
    @Test
    public void testContextCommonAndUnCommonVarBs1() throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));


          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

         
          
          for (int i = 0; i < 30; i++) {
              
              
              RyaStatement rs1 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaURI("uri:cq1"),  new RyaURI("uri:joe"));
              RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:joe"));
              RyaStatement rs3 = null;
              
              RyaStatement rs4 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaURI("uri:cq1"), new RyaURI("uri:hank"));
              RyaStatement rs5 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:hank"));
              RyaStatement rs6 = null;
              
              RyaStatement rs7 = null;
              RyaStatement rs8 = null;
            
             
              if(i == 5 || i == 10 || i == 15 || i == 20 || i == 25) {
                  rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:joe"));
                  rs6 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:hank"));
                  rs7 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(100+i)), new RyaURI("uri:joe"));
                  rs8 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(100+i)), new RyaURI("uri:hank"));
              }
  
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize4 = rtm.serialize(rs4);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize5 = rtm.serialize(rs5);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize6 = null;
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize7 = null;
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize8 = null;
              
              if(rs3 != null) {
                  serialize3 = rtm.serialize(rs3);
              }
              
              if(rs6 != null) {
                  serialize6 = rtm.serialize(rs6);
              }
              
              if(rs7 != null) {
                  serialize7 = rtm.serialize(rs7);
              }
              
              if(rs8 != null) {
                  serialize8 = rtm.serialize(rs8);
              }
              
              
              Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
              for (Mutation m : m1) {
                  bw.addMutation(m);
              }
              Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
              for (Mutation m : m2) {
                  bw.addMutation(m);
              }
              if (serialize3 != null) {
                  Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                  for (Mutation m : m3) {
                      bw.addMutation(m);
                  }
              }
              
              Collection<Mutation> m4 = EntityCentricIndex.createMutations(rs4);
              for (Mutation m : m4) {
                  bw.addMutation(m);
              }
              Collection<Mutation> m5 = EntityCentricIndex.createMutations(rs5);
              for (Mutation m : m5) {
                  bw.addMutation(m);
              }
              if (serialize6 != null) {
                  Collection<Mutation> m6 = EntityCentricIndex.createMutations(rs6);
                  for (Mutation m : m6) {
                      bw.addMutation(m);
                  }
              }
              
              if (serialize7 != null) {
                  Collection<Mutation> m7 = EntityCentricIndex.createMutations(rs7);
                  for (Mutation m : m7) {
                      bw.addMutation(m);
                  }
              }
              if (serialize8 != null) {
                  Collection<Mutation> m8 = EntityCentricIndex.createMutations(rs8);
                  for (Mutation m : m8) {
                      bw.addMutation(m);
                  }
              }
                   

    }
         
          String q1 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  +  "?X <uri:cf1> ?Y1 ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  +  "}";
          
          
          
          String q2 = "" //
                  + "SELECT ?X ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  + " GRAPH <uri:hank> { " //
                  +  "?X <uri:cf1> ?Y1 ."//
                  +  "?X <uri:cf2> ?Y2 ."//
                  +  "?X <uri:cf3> ?Y3 ."//
                  + " } "//
                  +  "}";
          
          
               
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);

                TupleExpr te1 = pq1.getTupleExpr();
                
                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
               
                StarQuery sq1 = new StarQuery(spList1);
               
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                
                Value v1 = RyaToRdfConversions.convertValue(new RyaType(XMLSchema.INTEGER,Integer.toString(105)));
                Value v2 = RyaToRdfConversions.convertValue(new RyaType(XMLSchema.INTEGER,Integer.toString(125)));
                
                List<BindingSet> bsList = Lists.newArrayList();
                QueryBindingSet b1 = new QueryBindingSet();
                b1.addBinding("X", vf.createURI("uri:5"));
                b1.addBinding("Y3", v1);
                QueryBindingSet b2 = new QueryBindingSet();
                b2.addBinding("X", vf.createURI("uri:25"));
                b2.addBinding("Y3", v2);
                bsList.add(b1);
                bsList.add(b2);
                
                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                
                System.out.println("**********************TEST 13***********************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(4, results);
                
                
                
                
                ParsedQuery pq2 = parser.parseQuery(q2, null);

                TupleExpr te2 = pq2.getTupleExpr();
                
                List<StatementPattern> spList2 = StatementPatternCollector.process(te2);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList2));
               
                StarQuery sq2 = new StarQuery(spList2);
                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol2 = adi.queryDocIndex(sq2, bsList);
                
                System.out.println("**********************TEST 13***********************");
                results = 0;
                while(sol2.hasNext()) {
                    System.out.println(sol2.next());
                    results++;
                }
                Assert.assertEquals(2, results);
                
                
                adi.close();
                   



    }
    
    
    
    
    
    
    
    
    @Test
    public void testContextConstantCommonVar() throws Exception {

      BatchWriter bw = null;
      RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(conf));
      

          bw = accCon.createBatchWriter(tableName, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

         
          
          for (int i = 0; i < 30; i++) {
              
              
              RyaStatement rs1 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaURI("uri:cq1"),  new RyaURI("uri:joe"));
              RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:joe"));
              RyaStatement rs3 = null;
              
              RyaStatement rs4 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaURI("uri:cq1"), new RyaURI("uri:hank"));
              RyaStatement rs5 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"), new RyaURI("uri:hank"));
              RyaStatement rs6 = null;
            
             
              if(i == 5 || i == 10 || i == 15 || i == 20 || i == 25) {
                  rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:joe"));
                  rs6 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)), new RyaURI("uri:hank"));
              }
  
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize1 = rtm.serialize(rs1);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize2 = rtm.serialize(rs2);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize3 = null;
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize4 = rtm.serialize(rs4);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize5 = rtm.serialize(rs5);
              Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, Collection<Mutation>> serialize6 = null;
             
              
              if(rs3 != null) {
                  serialize3 = rtm.serialize(rs3);
              }
              
              if(rs6 != null) {
                  serialize6 = rtm.serialize(rs6);
              }
              
              
              
              Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
              for (Mutation m : m1) {
                  bw.addMutation(m);
              }
              Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
              for (Mutation m : m2) {
                  bw.addMutation(m);
              }
              if (serialize3 != null) {
                  Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                  for (Mutation m : m3) {
                      bw.addMutation(m);
                  }
              }
              
              Collection<Mutation> m4 = EntityCentricIndex.createMutations(rs4);
              for (Mutation m : m4) {
                  bw.addMutation(m);
              }
              Collection<Mutation> m5 = EntityCentricIndex.createMutations(rs5);
              for (Mutation m : m5) {
                  bw.addMutation(m);
              }
              if (serialize6 != null) {
                  Collection<Mutation> m6 = EntityCentricIndex.createMutations(rs6);
                  for (Mutation m : m6) {
                      bw.addMutation(m);
                  }
              }
              
              
              
              

    }
         
          String q1 = "" //
                  + "SELECT ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  +  "<uri:5> <uri:cf1> ?Y1 ."//
                  +  "<uri:5> <uri:cf2> ?Y2 ."//
                  +  "<uri:5> <uri:cf3> ?Y3 ."//
                  +  "}";
          
          
          
          String q2 = "" //
                  + "SELECT ?Y1 ?Y2 ?Y3 " //
                  + "{"//
                  + " GRAPH <uri:hank> { " //
                  +  "<uri:5> <uri:cf1> ?Y1 ."//
                  +  "<uri:5> <uri:cf2> ?Y2 ."//
                  +  "<uri:5> <uri:cf3> ?Y3 ."//
                  + " } "//
                  +  "}";
          
          
               
          
                SPARQLParser parser = new SPARQLParser();

                ParsedQuery pq1 = parser.parseQuery(q1, null);

                TupleExpr te1 = pq1.getTupleExpr();
                
                List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
                
                String rowString = spList1.get(0).getSubjectVar().getValue().stringValue();
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
               
                StarQuery sq1 = new StarQuery(spList1);
               
                AccumuloDocIdIndexer adi = new AccumuloDocIdIndexer(conf);
                
//                Value v1 = RyaToRdfConversions.convertValue(new RyaType(XMLSchema.INTEGER,Integer.toString(5)));
//                Value v2 = RyaToRdfConversions.convertValue(new RyaType(XMLSchema.INTEGER,Integer.toString(25)));
                
                List<BindingSet> bsList = Lists.newArrayList();
//                QueryBindingSet b1 = (new QueryBindingSet());
//                b1.addBinding("X", vf.createURI("uri:5"));
//                QueryBindingSet b2 = (new QueryBindingSet());
//                b2.addBinding("X", vf.createURI("uri:15"));
//                QueryBindingSet b3 = (new QueryBindingSet());
//                b3.addBinding("X", vf.createURI("uri:25"));
//                bsList.add(b1);
//                bsList.add(b2);
//                bsList.add(b3);
                
                
//                BatchScanner bs = accCon.createBatchScanner(tablename + "doc_partitioned_index", new Authorizations("U"), 15);
//                bs.setRanges(Collections.singleton(new Range(rowString)));
//                Iterator<Entry<Key,org.apache.accumulo.core.data.Value>> bsIt = bs.iterator();
//                while(bsIt.hasNext()) {
//                    String otherRowString = bsIt.next().getKey().getRow().toString();
//                    if(rowString.equals(otherRowString)) {
//                        System.out.println(otherRowString);
//                    }
//                    
//                }
                
                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol1 = adi.queryDocIndex(sq1, bsList);
                
                System.out.println("**********************TEST 14***********************");
                int results = 0;
                while(sol1.hasNext()) {
                    System.out.println(sol1.next());
                    results++;
                }
                Assert.assertEquals(2, results);
                
                
                
                
                ParsedQuery pq2 = parser.parseQuery(q2, null);

                TupleExpr te2 = pq2.getTupleExpr();
                
                List<StatementPattern> spList2 = StatementPatternCollector.process(te2);
                
                Assert.assertTrue(StarQuery.isValidStarQuery(spList2));
               
                StarQuery sq2 = new StarQuery(spList2);
                
                
                CloseableIteration<BindingSet, QueryEvaluationException> sol2 = adi.queryDocIndex(sq2, bsList);
                
                System.out.println("**********************TEST 14***********************");
                results = 0;
                while(sol2.hasNext()) {
                    System.out.println(sol2.next());
                    results++;
                }
                Assert.assertEquals(1, results);
                
                
                adi.close();
                   



    }
    
    
    
    
    
    

}
