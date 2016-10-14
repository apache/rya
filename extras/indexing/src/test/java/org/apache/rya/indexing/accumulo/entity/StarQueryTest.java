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


import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rya.accumulo.documentIndex.TextColumn;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.apache.rya.indexing.accumulo.entity.StarQuery;

import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;

public class StarQueryTest {

    ValueFactory vf = new ValueFactoryImpl();
    
    
    @Test
    public void testBasicFunctionality() {
      
        String q1 = "" //
                + "SELECT ?X ?Y1 ?Y2 " //
                + "{"//
                + "GRAPH <http://joe> { " //
                +  "?X <uri:cf1> ?Y1 ."//
                +  "?X <uri:cf2> ?Y2 ."//
                +  "?X <uri:cf3> ?Y3 ."//
                +  "}" //
                +  "}";
        
        
        SPARQLParser parser = new SPARQLParser();
        
        ParsedQuery pq1 = null;
        try {
            pq1 = parser.parseQuery(q1, null);
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        }

        TupleExpr te1 = pq1.getTupleExpr();
        
        System.out.println(te1);
        List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
        
        Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
        
       
        StarQuery sq1 = new StarQuery(spList1);
        
        Var v = sq1.getCommonVar();
        
        Assert.assertEquals("X", v.getName());
        Assert.assertEquals(null, v.getValue());
        Assert.assertEquals(v.getValue(), sq1.getCommonVarValue());
        Assert.assertTrue(!sq1.commonVarHasValue());
        Assert.assertEquals("X", sq1.getCommonVarName());
        Assert.assertTrue(sq1.isCommonVarURI());
        
        Assert.assertTrue(sq1.hasContext());
        Assert.assertEquals("http://joe", sq1.getContextURI());
        
        TextColumn[] cond = sq1.getColumnCond(); 
        
        for(int i = 0; i < cond.length; i++ ) {
        
            Assert.assertEquals(cond[i].getColumnFamily().toString(), "uri:cf" + (i+1));
            Assert.assertEquals(cond[i].getColumnQualifier().toString(), "object");
        
        }
        
        Set<String> unCommonVars = Sets.newHashSet();
        unCommonVars.add("Y1");
        unCommonVars.add("Y2");
        unCommonVars.add("Y3");
        Assert.assertEquals(unCommonVars, sq1.getUnCommonVars());
        
        Map<String, Integer> varPos = sq1.getVarPos();
        
        Assert.assertEquals(0, varPos.get("Y1").intValue());
        Assert.assertEquals(1, varPos.get("Y2").intValue());
        Assert.assertEquals(2, varPos.get("Y3").intValue());
        
        QueryBindingSet bs1 = new QueryBindingSet();
        QueryBindingSet bs2 = new QueryBindingSet();
        
        Value v1 = vf.createURI("uri:hank");
        Value v2 = vf.createURI("uri:bob");
        
        bs1.addBinding("X",v1);
        bs2.addBinding("X", v1);
        bs2.addBinding("Y3", v2);
        
        Set<String> s1 = StarQuery.getCommonVars(sq1, bs1);
        Set<String> s2 = StarQuery.getCommonVars(sq1, bs2);
        
        Set<String> s3 = Sets.newHashSet();
        Set<String> s4 = Sets.newHashSet();
        s3.add("X");
        s4.add("X");
        s4.add("Y3");
        
        
        Assert.assertEquals(s1, s3);
        Assert.assertEquals(s2, s4);
        
        
        
    }
    
    
    
    
    
    
    
    
    
    @Test
    public void testGetContrainedQuery() {
      
        String q1 = "" //
                + "SELECT ?X ?Y1 ?Y2 " //
                + "{"//
                + "GRAPH <http://joe> { " //
                +  "?X <uri:cf1> ?Y1 ."//
                +  "?X <uri:cf2> ?Y2 ."//
                +  "?X <uri:cf3> ?Y3 ."//
                +  "}" //
                +  "}";
        
        
        SPARQLParser parser = new SPARQLParser();
        
        ParsedQuery pq1 = null;
        try {
            pq1 = parser.parseQuery(q1, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        TupleExpr te1 = pq1.getTupleExpr();
        
        System.out.println(te1);
        List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
        
        StarQuery sq1 = new StarQuery(spList1);
        
        QueryBindingSet bs1 = new QueryBindingSet();
        QueryBindingSet bs2 = new QueryBindingSet();
        
        Value v1 = vf.createURI("uri:hank");
        Value v2 = vf.createURI("uri:bob");
        
        bs1.addBinding("X",v1);
        bs2.addBinding("X", v1);
        bs2.addBinding("Y3", v2);
        
       StarQuery sq2 = StarQuery.getConstrainedStarQuery(sq1, bs1);
       StarQuery sq3 = StarQuery.getConstrainedStarQuery(sq1, bs2);
       
       Assert.assertTrue(sq2.commonVarHasValue());
       Assert.assertEquals(sq2.getCommonVarValue(), "uri:hank");
       
       Assert.assertTrue(sq3.commonVarHasValue());
       Assert.assertEquals(sq3.getCommonVarValue(), "uri:hank");
       
       
       TextColumn[] tc1 = sq1.getColumnCond();
       TextColumn[] tc2 = sq2.getColumnCond();
       TextColumn[] tc3 = sq3.getColumnCond();
       
       for(int i = 0; i < tc1.length; i++) {
           
           Assert.assertTrue(tc1[i].equals(tc2[i]));
           if(i != 2) {
               Assert.assertTrue(tc1[i].equals(tc3[i]));
           } else {
               Assert.assertEquals(tc3[i].getColumnFamily(), new Text("uri:cf3"));
               RyaType objType = RdfToRyaConversions.convertValue(v2);
               byte[][] b1 = null;
            try {
                b1 = RyaContext.getInstance().serializeType(objType);
            } catch (RyaTypeResolverException e) {
                e.printStackTrace();
            }
               byte[] b2 = Bytes.concat("object".getBytes(),
                       "\u0000".getBytes(), b1[0], b1[1]);
               Assert.assertEquals(tc3[i].getColumnQualifier(), new Text(b2));
               Assert.assertTrue(!tc3[i].isPrefix());
           }
       }
        
        
        
    }
    
    
    
    
    @Test
    public void testConstantPriority() {
      
        String q1 = "" //
                + "SELECT ?X " //
                + "{"//
                + "GRAPH <http://joe> { " //
                +  "?X <uri:cf1> <uri:obj1> ."//
                +  "?X <uri:cf2> <uri:obj1> ."//
                +  "?X <uri:cf3> <uri:obj1> ."//
                +  "}" //
                +  "}";
        
        
        SPARQLParser parser = new SPARQLParser();
        
        ParsedQuery pq1 = null;
        try {
            pq1 = parser.parseQuery(q1, null);
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        }

        TupleExpr te1 = pq1.getTupleExpr();
        
        System.out.println(te1);
        List<StatementPattern> spList1 = StatementPatternCollector.process(te1);
        
        Assert.assertTrue(StarQuery.isValidStarQuery(spList1));
        
       
        StarQuery sq1 = new StarQuery(spList1);
        Var v = sq1.getCommonVar();
        
        Assert.assertEquals("uri:obj1",v.getValue().stringValue());
        
        
        
    }
    
    
    
    
    
    
    

}
