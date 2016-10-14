package org.apache.rya.accumulo.documentIndex;

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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.RyaTableMutationsFactory;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.indexing.accumulo.entity.EntityCentricIndex;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryException;

import com.google.common.primitives.Bytes;


public class DocumentIndexIntersectingIteratorTest {

    
 
    private Connector accCon;
    String tablename = "table";
    

    @Before
    public void init() throws RepositoryException, TupleQueryResultHandlerException, QueryEvaluationException,
            MalformedQueryException, AccumuloException, AccumuloSecurityException, TableExistsException {

        accCon = new MockInstance().getConnector("root", "".getBytes());
        accCon.tableOperations().create(tablename);

    }
    
    
    
    
    
    
    
@Test
    public void testBasicColumnObj() throws Exception {

        BatchWriter bw = null;

            bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

            for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
                m.put(new Text("cf"), new Text(null + "\u0000" + "obj" + "\u0000" + "cq"), new Value(new byte[0]));
                m.put(new Text("cF"), new Text(null + "\u0000" +"obj" + "\u0000" + "cQ"), new Value(new byte[0]));

                if (i == 30 || i == 60) {
                    m.put(new Text("CF"), new Text(null + "\u0000" +"obj" + "\u0000" + "CQ"), new Value(new byte[0]));
                }

                bw.addMutation(m);

            }
            
            DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
            TextColumn tc1 = new TextColumn(new Text("cf"), new Text("obj" + "\u0000" + "cq" ));
            TextColumn tc2 = new TextColumn(new Text("cF"), new Text("obj" + "\u0000" + "cQ" ));
            TextColumn tc3 = new TextColumn(new Text("CF"), new Text("obj" + "\u0000" + "CQ" ));

            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            dii.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 1****************************");
            for (Map.Entry<Key, Value> e : scan) {
                System.out.println(e);
                results++;
            }
            
            
            Assert.assertEquals(2, results);

            
            

    }
    
    
    
    
    
    
    
@Test
    public void testBasicColumnObjPrefix()  throws Exception {

        BatchWriter bw = null;

            bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

            for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
                m.put(new Text("cf"), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" ), new Value(new byte[0]));
                m.put(new Text("cF"), new Text(null + "\u0000" +"obj" + "\u0000" + "cQ"), new Value(new byte[0]));

                if (i == 30 || i == 60) {
                    m.put(new Text("CF"), new Text(null + "\u0000" +"obj" + "\u0000" + "CQ" ), new Value(new byte[0]));
                }
                
                

                bw.addMutation(m);

            }
            
            DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
            TextColumn tc1 = new TextColumn(new Text("cf"), new Text("obj" + "\u0000" + "cq"));
            TextColumn tc2 = new TextColumn(new Text("cF"), new Text("obj" + "\u0000" + "cQ"));
            TextColumn tc3 = new TextColumn(new Text("CF"), new Text("obj"));

            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;
            
            tc3.setIsPrefix(true);

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            dii.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 2****************************");
            for (Map.Entry<Key, Value> e : scan) {
                System.out.println(e);
                results++;
            }
            
            
            Assert.assertEquals(2, results);

            
            

    }
    
    
    
    
@Test
    public void testBasicColumnSubjObjPrefix() throws Exception {

        BatchWriter bw = null;

            bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

            for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
                m.put(new Text("cf"), new Text(null + "\u0000" +"obj" + "\u0000" + "cq"), new Value(new byte[0]));
                m.put(new Text("cF"), new Text(null + "\u0000" +"obj" + "\u0000" + "cQ"), new Value(new byte[0]));

                if (i == 30 ) {
                    m.put(new Text("CF"), new Text(null + "\u0000" +"obj" + "\u0000" + "CQ"), new Value(new byte[0]));
                }
                
                if  (i == 60) {
                    m.put(new Text("CF"), new Text(null + "\u0000" +"subj" + "\u0000" + "CQ"), new Value(new byte[0]));
                }
                
                
                

                bw.addMutation(m);

            }
            
            DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
            TextColumn tc1 = new TextColumn(new Text("cf"), new Text("obj" + "\u0000" + "cq" ));
            TextColumn tc2 = new TextColumn(new Text("cF"), new Text("obj" + "\u0000" + "cQ"));
            TextColumn tc3 = new TextColumn(new Text("CF"), new Text("subj"));

            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;
            
            tc3.setIsPrefix(true);

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            dii.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 3****************************");
            for (Map.Entry<Key, Value> e : scan) {
                System.out.println(e);
                results++;
            }
            
            
            Assert.assertEquals(1, results);

            
            

    }
    
    
    
    
@Test
    public void testOneHundredColumnSubjObj() throws Exception {

        BatchWriter bw = null;

            bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

            for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
                
                for(int j= 0; j < 100; j++) {
                    m.put(new Text("cf" + j), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + j), new Value(new byte[0]));
                }
                
                if (i == 30 ) {
                    m.put(new Text("cf" + 100), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 100), new Value(new byte[0]));
                }
                
                if  (i == 60) {
                    m.put(new Text("cf" + 100), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 100), new Value(new byte[0]));
                }
                
                
                

                bw.addMutation(m);

            }
            
            DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
            TextColumn tc1 = new TextColumn(new Text("cf" + 20), new Text("obj" + "\u0000" + "cq" + 20));
            TextColumn tc2 = new TextColumn(new Text("cf" + 50), new Text("obj" + "\u0000" + "cq" + 50));
            TextColumn tc3 = new TextColumn(new Text("cf" + 100), new Text("obj" + "\u0000" + "cq" + 100));

            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            dii.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 4****************************");
            for (Map.Entry<Key, Value> e : scan) {
                System.out.println(e);
                results++;
            }
            
            
            Assert.assertEquals(1, results);

            
            

    }
    
    
    
    
@Test
    public void testOneHundredColumnObjPrefix() throws Exception {

        BatchWriter bw = null;

            bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

            for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
                
                for(int j= 0; j < 100; j++) {
                    m.put(new Text("cf" + j), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + j ), new Value(new byte[0]));
                }
                
                if (i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 100), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + (100 + i)), new Value(new byte[0]));
                }
                
                
                
                
                

                bw.addMutation(m);

            }
            
            DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
            TextColumn tc1 = new TextColumn(new Text("cf" + 20), new Text("obj" + "\u0000" + "cq" + 20));
            TextColumn tc2 = new TextColumn(new Text("cf" + 50), new Text("obj" + "\u0000" + "cq" + 50));
            TextColumn tc3 = new TextColumn(new Text("cf" + 100), new Text("obj"));

            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;
            
            tc3.setIsPrefix(true);

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            dii.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 5****************************");
            for (Map.Entry<Key, Value> e : scan) {
                System.out.println(e);
                results++;
            }
            
            
            Assert.assertEquals(4, results);

            
            

    }
    
    
    
    
    
    
    
@Test
    public void testOneHundredColumnMultipleEntriesPerSubject() throws Exception {

        BatchWriter bw = null;

            bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

            for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
                
                for(int j= 0; j < 100; j++) {
                    m.put(new Text("cf" + j), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + j ), new Value(new byte[0]));
                }
                
                if (i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 100), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + (100 + i)), new Value(new byte[0]));
                    m.put(new Text("cf" + 100), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + (100 + i + 1)), new Value(new byte[0]));
                }
                
                
                
                
                

                bw.addMutation(m);

            }
            
            DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
            TextColumn tc1 = new TextColumn(new Text("cf" + 20), new Text("obj" + "\u0000" + "cq" + 20 ));
            TextColumn tc2 = new TextColumn(new Text("cf" + 50), new Text("obj" + "\u0000" + "cq" + 50));
            TextColumn tc3 = new TextColumn(new Text("cf" + 100), new Text("obj"));

            tc3.setIsPrefix(true);
            
            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            dii.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 6****************************");
            for (Map.Entry<Key, Value> e : scan) {
                System.out.println(e);
                results++;
            }
            
            
            Assert.assertEquals(8, results);

            
            

    }
    
    
    
    

@Test
public void testOneHundredColumnSubjObjPrefix() throws Exception {

    BatchWriter bw = null;

        bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

        for (int i = 0; i < 100; i++) {

            Mutation m = new Mutation(new Text("" + i));
            
            for(int j= 0; j < 100; j++) {
                m.put(new Text("cf" + j), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + j), new Value(new byte[0]));
            }
            
            if (i == 30 || i == 60 || i == 90 || i == 99) {
                m.put(new Text("cf" + 100), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + (100 + i)), new Value(new byte[0]));
                m.put(new Text("cf" + 100), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + (100 + i + 1)), new Value(new byte[0]));
            }
            
            
            
            
            

            bw.addMutation(m);

        }
        
        DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
        TextColumn tc1 = new TextColumn(new Text("cf" + 20), new Text("obj" + "\u0000" + "cq" + 20));
        TextColumn tc2 = new TextColumn(new Text("cf" + 50), new Text("obj" + "\u0000" + "cq" + 50));
        TextColumn tc3 = new TextColumn(new Text("cf" + 100), new Text("subj"));

        tc3.setIsPrefix(true);
        
        TextColumn[] tc = new TextColumn[3];
        tc[0] = tc1;
        tc[1] = tc2;
        tc[2] = tc3;

        IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

        dii.setColumnFamilies(is, tc);

        Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
        scan.addScanIterator(is);

        int results = 0;
        System.out.println("************************Test 7****************************");
        for (Map.Entry<Key, Value> e : scan) {
            System.out.println(e);
            results++;
        }
        
        
        Assert.assertEquals(4, results);

        
        

}






@Test
public void testOneHundredColumnSubjObjPrefixFourTerms() throws Exception {

    BatchWriter bw = null;

        bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

        for (int i = 0; i < 100; i++) {

            Mutation m = new Mutation(new Text("" + i));
            
            for(int j= 0; j < 100; j++) {
                m.put(new Text("cf" + j), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + j), new Value(new byte[0]));
            }
            
            if (i == 30 || i == 60 || i == 90 || i == 99) {
                m.put(new Text("cf" + 100), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + (100 + i)), new Value(new byte[0]));
                m.put(new Text("cf" + 100), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + (100 + i + 1)), new Value(new byte[0]));
            }
            
            
            
            
            

            bw.addMutation(m);

        }
        
        DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
        TextColumn tc1 = new TextColumn(new Text("cf" + 20), new Text("obj" + "\u0000" + "cq" + 20));
        TextColumn tc2 = new TextColumn(new Text("cf" + 50), new Text("obj" + "\u0000" + "cq" + 50));
        TextColumn tc3 = new TextColumn(new Text("cf" + 100), new Text("subj"));
        TextColumn tc4 = new TextColumn(new Text("cf" + 100), new Text("obj"));

        tc3.setIsPrefix(true);
        tc4.setIsPrefix(true);
        
        TextColumn[] tc = new TextColumn[4];
        tc[0] = tc1;
        tc[1] = tc2;
        tc[2] = tc3;
        tc[3] = tc4;

        IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

        dii.setColumnFamilies(is, tc);

        Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
        scan.addScanIterator(is);

        int results = 0;
        System.out.println("************************Test 8****************************");
        for (Map.Entry<Key, Value> e : scan) {
            System.out.println(e);
            results++;
        }
        
        
        Assert.assertEquals(4, results);

        
        

}






//@Test
public void testOneHundredColumnSameCf() throws Exception {

    BatchWriter bw = null;

        bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

        for (int i = 0; i < 100; i++) {

            Mutation m = new Mutation(new Text("" + i));
            
            for(int j= 0; j < 100; j++) {
                m.put(new Text("cf"), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + j), new Value(new byte[0]));
            }
             
            

            bw.addMutation(m);

        }
        
        DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
        TextColumn tc1 = new TextColumn(new Text("cf" ), new Text("obj" + "\u0000" + "cq" + 20));
        TextColumn tc2 = new TextColumn(new Text("cf"), new Text("obj" + "\u0000" + "cq" + 50));
        TextColumn tc3 = new TextColumn(new Text("cf" ), new Text("obj" + "\u0000" + "cq" + 80));
        TextColumn tc4 = new TextColumn(new Text("cf"), new Text("obj"));

        tc4.setIsPrefix(true);
        
        TextColumn[] tc = new TextColumn[4];
        tc[0] = tc1;
        tc[1] = tc2;
        tc[2] = tc3;
        tc[3] = tc4;

        IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

        dii.setColumnFamilies(is, tc);

        Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
        scan.addScanIterator(is);

        int results = 0;
        System.out.println("************************Test 9****************************");
        for (Map.Entry<Key, Value> e : scan) {
            //System.out.println(e);
            results++;
        }
        
        
        Assert.assertEquals(10000, results);

        
        

}





@Test
public void testGeneralStarQuery() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                
                

                if(i == 30 || i == 60 ) {
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                }
                
                bw.addMutation(m);

      }
     
      
      DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 3));

      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      dii.setColumnFamilies(is, tc);

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 10****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(2, results);

      
      

}







@Test
public void testGeneralStarQuerySubjPrefix() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                }
                
                bw.addMutation(m);

      }
     
      
      DocumentIndexIntersectingIterator dii = new DocumentIndexIntersectingIterator();
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("subj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      dii.setColumnFamilies(is, tc);

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 11****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(4, results);

      
      

}





@Test
public void testGeneralStarQueryMultipleSubjPrefix() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 3 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 5), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 5), new Value(new byte[0]));
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("subj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 12****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(12, results);

      
      

}




@Test
public void testFixedRangeColumnValidateExact() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 5), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 5), new Value(new byte[0]));
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 3));
      TextColumn tc4 = new TextColumn(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 4));
      TextColumn tc5 = new TextColumn(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 5));


      
      TextColumn[] tc = new TextColumn[5];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
      tc[3] = tc4;
      tc[4] = tc5;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
      scan.setRange(Range.exact(new Text("" + 30)));
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 14****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(1, results);

      
      

}






@Test
public void testLubmLikeTest() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m1 = new Mutation(new Text("ProfessorA" + i));
                Mutation m2= new Mutation(new Text("ProfessorB" + i));
    
                m1.put(new Text("http://swat.cse.lehigh.edu/onto/univ-bench.owl#doctoralDegreeFrom"), 
                        new Text(null + "\u0000" +"object" + "\u0000" + "http://www.University" + i + ".edu"), new Value(new byte[0]));
                m2.put(new Text("http://swat.cse.lehigh.edu/onto/univ-bench.owl#doctoralDegreeFrom"), 
                        new Text(null + "\u0000" +"object" + "\u0000" + "http://www.University" + i + ".edu"), new Value(new byte[0]));
                m1.put(new Text("http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf"), 
                        new Text(null + "\u0000" +"object" + "\u0000" + "http://Course" + i), new Value(new byte[0]));
                m2.put(new Text("http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf"), 
                        new Text(null + "\u0000" +"object" + "\u0000" + "http://Course" + i), new Value(new byte[0]));
            
                
                bw.addMutation(m1);
                bw.addMutation(m2);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("http://swat.cse.lehigh.edu/onto/univ-bench.owl#doctoralDegreeFrom" ), 
              new Text("object" + "\u0000" + "http://www.University" + 30 + ".edu"));
      TextColumn tc2 = new TextColumn(new Text("http://swat.cse.lehigh.edu/onto/univ-bench.owl#teacherOf"), 
              new Text("object" + "\u0000" + "http://Course" + 30));
      


      
      TextColumn[] tc = new TextColumn[2];
      tc[0] = tc1;
      tc[1] = tc2;
     

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 15****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(2, results);

      
      

}

















@Test
public void testFixedRangeColumnValidateSubjPrefix() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 3 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 4 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"obj" + "\u0000" + "cq" + 5 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000" +"subj" + "\u0000" + "cq" + 5 ), new Value(new byte[0]));
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("subj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
      scan.setRange(Range.exact(new Text("" + 30)));
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 13****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(3, results);

      
      

}





//@Test
//public void testRangeBound() {
//
//  BatchWriter bw = null;
//
//  try {
//    
//
//     
//      
//      for (int i = 0; i < 100; i++) {
//
//                Mutation m = new Mutation(new Text("" + i));
//    
//                m.put(new Text("cf" + 1), new Text("obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
//                m.put(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
//                m.put(new Text("cf" + 1), new Text("subj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
//                m.put(new Text("cf" + 2), new Text("subj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
//                
//                
//
//                if(i == 30 || i == 60 || i == 90 || i == 99) {
//                    m.put(new Text("cf" + 3), new Text("obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
//                    m.put(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
//                    m.put(new Text("cf" + 3), new Text("obj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
//                    m.put(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 4), new Value(new byte[0]));
//                    m.put(new Text("cf" + 3), new Text("obj" + "\u0000" + "cq" + 5), new Value(new byte[0]));
//                    m.put(new Text("cf" + 3), new Text("subj" + "\u0000" + "cq" + 5), new Value(new byte[0]));
//                }
//                
//                bw.addMutation(m);
//
//      }
//     
//    
//      
//     Text cf = new Text("cf" + 3); 
//     Text cq = new Text("obj" + "\u0000" + "cq" + 3);
//    
//      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
//      scan.fetchColumn(cf, cq );
//      scan.setRange(new Range());
//      
//
//      int results = 0;
//      System.out.println("************************Test 14****************************");
//      for (Map.Entry<Key, Value> e : scan) {
//          System.out.println(e);
//          results++;
//      }
//      
//      
//      
//
//      
//      
//  } catch (MutationsRejectedException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//  } catch (TableNotFoundException e) {
//      // TODO Auto-generated catch block
//      e.printStackTrace();
//  }
//
//}



  

@Test
public void testContext1() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000" + "obj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 3 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 4 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 5 ), new Value(new byte[0]));
                 
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("obj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
      DocumentIndexIntersectingIterator.setContext(is, "context1");

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 14****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(8, results);

      
      

}







@Test
public void testContext2() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 4 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context3" + "\u0000" +"obj" + "\u0000" + "cq" + 5 ), new Value(new byte[0]));
                 
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("obj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
      DocumentIndexIntersectingIterator.setContext(is, "context2");

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 15****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(0, results);

      
      

}








@Test
public void testContext3() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 1 + "\u0000" + "context1"), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 2 + "\u0000" + "context1"), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 1 + "\u0000" + "context2"), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 2 + "\u0000" + "context2"), new Value(new byte[0]));
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 3 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 4 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context3" + "\u0000" +"obj" + "\u0000" + "cq" + 5 ), new Value(new byte[0]));
                 
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("obj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
      DocumentIndexIntersectingIterator.setContext(is, "context2");

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 16****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(4, results);

      
      

}









@Test
public void testContext4() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text("context1" + "\u0000" +"obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context2" + "\u0000" +"obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    
                 
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("obj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
     

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 17****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(8, results);

      
      


}





@Test
public void testContext5() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("" + i));
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + 1), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + 2), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text(null + "\u0000" + "obj" + "\u0000" + "cq" + 1 ), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text(null + "\u0000" + "obj" + "\u0000" + "cq" + 2 ), new Value(new byte[0]));
                

                if(i == 30 || i == 60 || i == 90 || i == 99) {
                    m.put(new Text("cf" + 3), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + 3), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + 3 ), new Value(new byte[0]));
                    m.put(new Text("cf" + 3), new Text(null + "\u0000"  + "obj" + "\u0000" + "cq" + 3 ), new Value(new byte[0]));
                    
                 
                }
                
                bw.addMutation(m);

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" + "\u0000" + "cq" + 1));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" + "\u0000" + "cq" + 2));
      TextColumn tc3 = new TextColumn(new Text("cf" + 3), new Text("obj"));

      tc3.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[3];
      tc[0] = tc1;
      tc[1] = tc2;
      tc[2] = tc3;
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
     

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 18****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(12, results);

      
      

}






@Test
public void testContext6() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 100; i++) {

                Mutation m = new Mutation(new Text("row" + i));
               
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000"  + "subj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000"  + "subj" + "\u0000" + "cq" + i), new Value(new byte[0]));
     
                
                bw.addMutation(m);
                

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" ));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("subj" ));
      

      tc1.setIsPrefix(true);
      tc2.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[2];
      tc[0] = tc1;
      tc[1] = tc2;
      
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
      DocumentIndexIntersectingIterator.setContext(is, "context2");

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 19****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(100, results);

      
      

}



@Test
public void testContext7() throws Exception {

  BatchWriter bw = null;

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 10; i++) {

                Mutation m = new Mutation(new Text("row" + i));
               
    
                m.put(new Text("cf" + 1), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context1" + "\u0000"  + "obj" + "\u0000" + "cq" + 100 + i), new Value(new byte[0]));
                m.put(new Text("cf" + 1), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + i), new Value(new byte[0]));
                m.put(new Text("cf" + 2), new Text("context2" + "\u0000"  + "obj" + "\u0000" + "cq" + 100+i), new Value(new byte[0]));
     
                
                bw.addMutation(m);
                

      }
     
      
     
      TextColumn tc1 = new TextColumn(new Text("cf" + 1 ), new Text("obj" ));
      TextColumn tc2 = new TextColumn(new Text("cf" + 2), new Text("obj" ));
      

      tc1.setIsPrefix(true);
      tc2.setIsPrefix(true);
      
      TextColumn[] tc = new TextColumn[2];
      tc[0] = tc1;
      tc[1] = tc2;
      
    

      IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

      DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);
      

      Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));
     
      scan.addScanIterator(is);

      int results = 0;
      System.out.println("************************Test 20****************************");
      for (Map.Entry<Key, Value> e : scan) {
          System.out.println(e);
          results++;
      }
      
      
      Assert.assertEquals(40, results);

      
      

}







@Test
public void testSerialization1() throws Exception {

  BatchWriter bw = null;
  AccumuloRdfConfiguration acc = new AccumuloRdfConfiguration();
  acc.set(AccumuloRdfConfiguration.CONF_ADDITIONAL_INDEXERS, EntityCentricIndex.class.getName());
  RyaTableMutationsFactory rtm = new RyaTableMutationsFactory(RyaTripleContext.getInstance(acc));

      bw = accCon.createBatchWriter(tablename, 500L * 1024L * 1024L, Long.MAX_VALUE, 30);

     
      
      for (int i = 0; i < 20; i++) {
                  
          
                RyaStatement rs1 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf1"), new RyaType(XMLSchema.STRING, "cq1"));
                RyaStatement rs2 = new RyaStatement(new RyaURI("uri:" + i ), new RyaURI("uri:cf2"), new RyaType(XMLSchema.STRING, "cq2"));
                RyaStatement rs3 = null;
                RyaStatement rs4 = null;
               
                if(i == 5 || i == 15) {
                    rs3 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.INTEGER,Integer.toString(i)));
                    rs4 = new RyaStatement(new RyaURI("uri:" +i ), new RyaURI("uri:cf3"), new RyaType(XMLSchema.STRING,Integer.toString(i)));
                }
                

                
                Collection<Mutation> m1 = EntityCentricIndex.createMutations(rs1);
                for (Mutation m : m1) {
                    bw.addMutation(m);
                }
                Collection<Mutation> m2 = EntityCentricIndex.createMutations(rs2);
                for (Mutation m : m2) {
                    bw.addMutation(m);
                }
                if (rs3 != null) {
                    Collection<Mutation> m3 = EntityCentricIndex.createMutations(rs3);
                    for (Mutation m : m3) {
                        bw.addMutation(m);
                    }
                }
                if (rs4 != null) {
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

            System.out.println(spList1);
            System.out.println(spList2);

            RyaType rt1 = RdfToRyaConversions.convertValue(spList1.get(2).getObjectVar().getValue());
            RyaType rt2 = RdfToRyaConversions.convertValue(spList2.get(2).getObjectVar().getValue());
            
            RyaURI predURI1 = (RyaURI) RdfToRyaConversions.convertValue(spList1.get(0).getPredicateVar().getValue());
            RyaURI predURI2 = (RyaURI) RdfToRyaConversions.convertValue(spList1.get(1).getPredicateVar().getValue());
            RyaURI predURI3 = (RyaURI) RdfToRyaConversions.convertValue(spList1.get(2).getPredicateVar().getValue());
            
//            System.out.println("to string" + spList1.get(2).getObjectVar().getValue().stringValue());
//            System.out.println("converted obj" + rt1.getData());
//            System.out.println("equal: " + rt1.getData().equals(spList1.get(2).getObjectVar().getValue().stringValue()));
            
            
            System.out.println(rt1);
            System.out.println(rt2);

            RyaContext rc = RyaContext.getInstance();

            byte[][] b1 = rc.serializeType(rt1);
            byte[][] b2 = rc.serializeType(rt2);

            byte[] b3 = Bytes.concat("object".getBytes(), "\u0000".getBytes(), b1[0], b1[1]);
            byte[] b4 = Bytes.concat("object".getBytes(), "\u0000".getBytes(), b2[0], b2[1]);

            System.out.println(new String(b3));
            System.out.println(new String(b4));

            TextColumn tc1 = new TextColumn(new Text(predURI1.getData()), new Text("object"));
            TextColumn tc2 = new TextColumn(new Text(predURI2.getData()), new Text("object"));
            TextColumn tc3 = new TextColumn(new Text(predURI3.getData()), new Text(b3));

            tc1.setIsPrefix(true);
            tc2.setIsPrefix(true);
      
            TextColumn[] tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;

            IteratorSetting is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);

            Scanner scan = accCon.createScanner(tablename, new Authorizations("auths"));

            scan.addScanIterator(is);

            int results = 0;
            System.out.println("************************Test 21****************************");
            Text t = null;
            for (Map.Entry<Key, Value> e : scan) {
                t = e.getKey().getColumnQualifier();
                System.out.println(e);
                results++;
            }

            Assert.assertEquals(1, results);
            String [] s = t.toString().split("\u001D" + "\u001E");
            String[] s1 = s[2].split("\u0000");
            RyaType rt = rc.deserialize(s1[2].getBytes());
            System.out.println("Rya type is " + rt);
            org.openrdf.model.Value v = RyaToRdfConversions.convertValue(rt);
            Assert.assertTrue(v.equals(spList1.get(2).getObjectVar().getValue()));

            tc1 = new TextColumn(new Text(predURI1.getData()), new Text("object"));
            tc2 = new TextColumn(new Text(predURI2.getData()), new Text("object"));
            tc3 = new TextColumn(new Text(predURI3.getData()), new Text(b4));

            tc1.setIsPrefix(true);
            tc2.setIsPrefix(true);

            tc = new TextColumn[3];
            tc[0] = tc1;
            tc[1] = tc2;
            tc[2] = tc3;

            is = new IteratorSetting(30, "fii", DocumentIndexIntersectingIterator.class);

            DocumentIndexIntersectingIterator.setColumnFamilies(is, tc);

            scan = accCon.createScanner(tablename, new Authorizations("auths"));

            scan.addScanIterator(is);

            results = 0;
            System.out.println("************************Test 21****************************");
            
            for (Map.Entry<Key, Value> e : scan) {
                t = e.getKey().getColumnQualifier();
                System.out.println(e);
                results++;
            }

            Assert.assertEquals(1, results);
            s = t.toString().split("\u001D" + "\u001E");
            s1 = s[2].split("\u0000");
            rt = rc.deserialize(s1[2].getBytes());
            System.out.println("Rya type is " + rt);
            v = RyaToRdfConversions.convertValue(rt);
            Assert.assertTrue(v.equals(spList2.get(2).getObjectVar().getValue()));
            
            


}









    
    

}
