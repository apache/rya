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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;
import org.apache.rya.indexing.IndexPlanValidator.IndexedExecutionPlanGenerator;
import org.apache.rya.indexing.IndexPlanValidator.ValidIndexCombinationGenerator;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.SimpleExternalTupleSet;

import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Lists;


public class ValidIndexCombinationGeneratorTest {

    
    
    
    

    @Test
    public void singleIndex() {
        String q1 = ""//
                + "SELECT ?f ?m ?d " //
                + "{" //
                + "  ?f a ?m ."//
                + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
                + "  ?d <uri:talksTo> ?f . "//
                + "  ?f <uri:hangOutWith> ?m ." //
                + "  ?m <uri:hangOutWith> ?d ." //
                + "  ?f <uri:associatesWith> ?m ." //
                + "  ?m <uri:associatesWith> ?d ." //
                + "}";//
        
        
       
        
        

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq1 = null;
       
        
        SimpleExternalTupleSet extTup1 = null;
        
        
        
        
        
        
        try {
            pq1 = parser.parseQuery(q1, null);
            
           

            extTup1 = new SimpleExternalTupleSet((Projection) pq1.getTupleExpr());
            
          
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        List<ExternalTupleSet> indexList = Lists.newArrayList();
        indexList.add(extTup1);
        
        
        ValidIndexCombinationGenerator vic = new ValidIndexCombinationGenerator(pq1.getTupleExpr());
        Iterator<List<ExternalTupleSet>> combos = vic.getValidIndexCombos(indexList);
        int size = 0;
        while(combos.hasNext()) {
            combos.hasNext();
            size++;
            combos.next();
            combos.hasNext();
        }
        
       Assert.assertTrue(!combos.hasNext());
       Assert.assertEquals(1,size);
        
        
    }
    
    
    
    
    
    
    @Test
    public void medQueryEightOverlapIndex() {
        String q1 = ""//
                + "SELECT ?f ?m ?d " //
                + "{" //
                + "  ?f a ?m ."//
                + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
                + "  ?d <uri:talksTo> ?f . "//
                + "  ?f <uri:hangOutWith> ?m ." //
                + "  ?m <uri:hangOutWith> ?d ." //
                + "  ?f <uri:associatesWith> ?m ." //
                + "  ?m <uri:associatesWith> ?d ." //
                + "}";//
        
        
        String q2 = ""//
                + "SELECT ?t ?s ?u " //
                + "{" //
                + "  ?s a ?t ."//
                + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "}";//
        
        
        String q3 = ""//
                + "SELECT ?s ?t ?u " //
                + "{" //
                + "  ?s <uri:hangOutWith> ?t ." //
                + "  ?t <uri:hangOutWith> ?u ." //
                + "}";//
        
        String q4 = ""//
                + "SELECT ?s ?t ?u " //
                + "{" //
                + "  ?s <uri:associatesWith> ?t ." //
                + "  ?t <uri:associatesWith> ?u ." //
                + "}";//
        
        
        String q5 = ""//
                + "SELECT ?t ?s ?u " //
                + "{" //
                + "  ?s a ?t ."//
                + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "  ?s <uri:hangOutWith> ?t ." //
                + "  ?t <uri:hangOutWith> ?u ." //
                + "}";//
        
        String q6 = ""//
                + "SELECT ?s ?t ?u " //
                + "{" //
                + "  ?s <uri:associatesWith> ?t ." //
                + "  ?t <uri:associatesWith> ?u ." //
                + "  ?s <uri:hangOutWith> ?t ." //
                + "  ?t <uri:hangOutWith> ?u ." //
                + "}";//
        
        
        String q7 = ""//
                + "SELECT ?s ?t ?u " //
                + "{" //
                + "  ?s <uri:associatesWith> ?t ." //
                + "  ?t <uri:associatesWith> ?u ." //
                + "  ?t <uri:hangOutWith> ?u ." //
                + "}";//
        
        
        
        String q8 = ""//
                + "SELECT ?t ?s ?u " //
                + "{" //
                + "  ?s a ?t ."//
                + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "  ?s <uri:associatesWith> ?t ." //
                + "}";//
        
        
        String q9 = ""//
                + "SELECT ?t ?s ?u " //
                + "{" //
                + "  ?s a ?t ."//
                + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
                + "}";//
        
        
        
        
        
        
        
        

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery pq1 = null;
        ParsedQuery pq2 = null;
        ParsedQuery pq3 = null;
        ParsedQuery pq4 = null;
        ParsedQuery pq5 = null;
        ParsedQuery pq6 = null;
        ParsedQuery pq7 = null;
        ParsedQuery pq8 = null;
        ParsedQuery pq9 = null;
        
        SimpleExternalTupleSet extTup1 = null;
        SimpleExternalTupleSet extTup2 = null;
        SimpleExternalTupleSet extTup3 = null;
        SimpleExternalTupleSet extTup4 = null;
        SimpleExternalTupleSet extTup5 = null;
        SimpleExternalTupleSet extTup6 = null;
        SimpleExternalTupleSet extTup7 = null;
        SimpleExternalTupleSet extTup8 = null;
        
        
        
        
        
        try {
            pq1 = parser.parseQuery(q1, null);
            pq2 = parser.parseQuery(q2, null);
            pq3 = parser.parseQuery(q3, null);
            pq4 = parser.parseQuery(q4, null);
            pq5 = parser.parseQuery(q5, null);
            pq6 = parser.parseQuery(q6, null);
            pq7 = parser.parseQuery(q7, null);
            pq8 = parser.parseQuery(q8, null);
            pq9 = parser.parseQuery(q9, null);
           

            extTup1 = new SimpleExternalTupleSet((Projection) pq2.getTupleExpr());
            extTup2 = new SimpleExternalTupleSet((Projection) pq3.getTupleExpr());
            extTup3 = new SimpleExternalTupleSet((Projection) pq4.getTupleExpr());
            extTup4 = new SimpleExternalTupleSet((Projection) pq5.getTupleExpr());
            extTup5 = new SimpleExternalTupleSet((Projection) pq6.getTupleExpr());
            extTup6 = new SimpleExternalTupleSet((Projection) pq7.getTupleExpr());
            extTup7 = new SimpleExternalTupleSet((Projection) pq8.getTupleExpr());
            extTup8 = new SimpleExternalTupleSet((Projection) pq9.getTupleExpr());
            
          
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        List<ExternalTupleSet> indexList = Lists.newArrayList();
        indexList.add(extTup1);
        indexList.add(extTup2);
        indexList.add(extTup3);
        indexList.add(extTup4);
        indexList.add(extTup5);
        indexList.add(extTup6);
        indexList.add(extTup7);
        indexList.add(extTup8);
        
        
        ValidIndexCombinationGenerator vic = new ValidIndexCombinationGenerator(pq1.getTupleExpr());
        Iterator<List<ExternalTupleSet>> combos = vic.getValidIndexCombos(indexList);
        int size = 0;
        while(combos.hasNext()) {
            combos.hasNext();
            size++;
            combos.next();
            combos.hasNext();
        }
        
       Assert.assertTrue(!combos.hasNext());
       Assert.assertEquals(21,size);
        
        
    }
    
    
    
    
    
    @Test
    public void largeQuerySixteenIndexTest() {
        
        
        String q1 = ""//
                + "SELECT ?f ?m ?d ?e ?l ?c ?n ?o ?p ?a ?h ?r " //
                + "{" //
                + "  ?f a ?m ."//
                + "  ?e a ?l ."//
                + "  ?n a ?o ."//
                + "  ?a a ?h ."//
                + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
                + "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?p ."//
                + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?r ."//
                + "  ?d <uri:talksTo> ?f . "//
                + "  ?c <uri:talksTo> ?e . "//
                + "  ?p <uri:talksTo> ?n . "//
                + "  ?r <uri:talksTo> ?a . "//
                + "}";//
        
        
        String q2 = ""//
                + "SELECT ?s ?t ?u " //
                + "{" //
                + "  ?s a ?t ."//
                + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "}";//
        
        
        
        String q3 = ""//
                + "SELECT  ?s ?t ?u ?d ?f ?g " //
                + "{" //
                + "  ?s a ?t ."//
                + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "  ?d a ?f ."//
                + "  ?f <http://www.w3.org/2000/01/rdf-schema#label> ?g ."//
                + "  ?g <uri:talksTo> ?d . "//
                + "}";//
        
     
        
        
        SPARQLParser parser = new SPARQLParser();

        ParsedQuery pq1 = null;
        ParsedQuery pq2 = null;
        ParsedQuery pq3 = null;
       

        try {
            pq1 = parser.parseQuery(q1, null);
            pq2 = parser.parseQuery(q2, null);
            pq3 = parser.parseQuery(q3, null);
          
        } catch (Exception e) {
            e.printStackTrace();
        }

        SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet((Projection) pq2.getTupleExpr());
        SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet((Projection) pq3.getTupleExpr());
       
     
        List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

        list.add(extTup2);
        list.add(extTup1);
      

        IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(pq1.getTupleExpr(), list);
        List<ExternalTupleSet> indexSet = iep.getNormalizedIndices();
        
        
        Assert.assertEquals(16, indexSet.size());
        
        ValidIndexCombinationGenerator vic = new ValidIndexCombinationGenerator(pq1.getTupleExpr());
        Iterator<List<ExternalTupleSet>> eSet = vic.getValidIndexCombos(Lists.newArrayList(indexSet));
        
        int size = 0;
        while(eSet.hasNext()) {
            size++;
            Assert.assertTrue(eSet.hasNext());
            eSet.next();
        }
        
        
        Assert.assertTrue(!eSet.hasNext());
        Assert.assertEquals(75, size);
        
    }
    
    
    
    
    
    
    @Test
    public void largeQueryFourtyIndexTest() {
        
        
        String q1 = ""//
                + "SELECT ?f ?m ?d ?e ?l ?c ?n ?o ?p ?a ?h ?r " //
                + "{" //
                + "  ?f a ?m ."//
                + "  ?e a ?l ."//
                + "  ?n a ?o ."//
                + "  ?a a ?h ."//
                + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
                + "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?p ."//
                + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?r ."//
                + "  ?d <uri:talksTo> ?f . "//
                + "  ?c <uri:talksTo> ?e . "//
                + "  ?p <uri:talksTo> ?n . "//
                + "  ?r <uri:talksTo> ?a . "//
                + "}";//
        
        
        String q2 = ""//
                + "SELECT ?s ?t ?u " //
                + "{" //
                + "  ?s a ?t ."//
                + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "}";//
        
        
        
        String q3 = ""//
                + "SELECT  ?s ?t ?u ?d ?f ?g " //
                + "{" //
                + "  ?s a ?t ."//
                + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "  ?d a ?f ."//
                + "  ?f <http://www.w3.org/2000/01/rdf-schema#label> ?g ."//
                + "  ?g <uri:talksTo> ?d . "//
                + "}";//
        
        
        
        String q4 = ""//
                + "SELECT  ?s ?t ?u ?d ?f ?g ?a ?b ?c" //
                + "{" //
                + "  ?s a ?t ."//
                + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "  ?d a ?f ."//
                + "  ?f <http://www.w3.org/2000/01/rdf-schema#label> ?g ."//
                + "  ?g <uri:talksTo> ?d . "//
                + "  ?a a ?b ."//
                + "  ?b <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
                + "  ?c <uri:talksTo> ?a . "//
                + "}";//
        
        
        SPARQLParser parser = new SPARQLParser();

        ParsedQuery pq1 = null;
        ParsedQuery pq2 = null;
        ParsedQuery pq3 = null;
        ParsedQuery pq4 = null;
       

        try {
            pq1 = parser.parseQuery(q1, null);
            pq2 = parser.parseQuery(q2, null);
            pq3 = parser.parseQuery(q3, null);
            pq4 = parser.parseQuery(q4, null);
           
        } catch (Exception e) {
            e.printStackTrace();
        }

        SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet((Projection) pq2.getTupleExpr());
        SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet((Projection) pq3.getTupleExpr());
        SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet((Projection) pq4.getTupleExpr());
     
        List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

        list.add(extTup2);
        list.add(extTup1);
        list.add(extTup3);

        IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(pq1.getTupleExpr(), list);
        List<ExternalTupleSet> indexSet = iep.getNormalizedIndices();
        Assert.assertEquals(40, indexSet.size());
        
        ValidIndexCombinationGenerator vic = new ValidIndexCombinationGenerator(pq1.getTupleExpr());
        Iterator<List<ExternalTupleSet>> eSet = vic.getValidIndexCombos(Lists.newArrayList(indexSet));
        
        int size = 0;
        while(eSet.hasNext()) {
            size++;
            Assert.assertTrue(eSet.hasNext());
            eSet.next();
        }
        
        Assert.assertTrue(!eSet.hasNext());
        Assert.assertEquals(123, size);
    }
    
    
    
    
    
    
    
    

}
