package org.apache.rya.indexing.IndexPlanValidator;

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



import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Sets;

public class TupleExecutionPlanGeneratorTest {



    private String q1 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  ?s a ?t ."//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "  ?u <uri:talksTo> ?s . "//
            + "}";//


    private String q2 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "  ?s a ?t ."//
            + "  ?u <uri:talksTo> ?s . "//
            + "}";//


    private String q3 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  ?u <uri:talksTo> ?s . "//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "  ?s a ?t ."//
            + "}";//


    private String q4 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  ?s a ?t ."//
            + "  ?u <uri:talksTo> ?s . "//
             + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "}";//


    private String q5 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "  ?u <uri:talksTo> ?s . "//
            + "  ?s a ?t ."//
            + "}";//


    private String q6 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  ?u <uri:talksTo> ?s . "//
            + "  ?s a ?t ."//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "}";//




    private String q7 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  ?s a ?t ."//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "}";//


    private String q8 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "  ?s a ?t ."//
            + "}";//






    private String q9 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  Filter(?t > 2). "//
            + "  Filter(?s > 1). "//
            + "  ?s a ?t ."//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "  ?u <uri:talksTo> ?s . "//
            + "}";//


    private String q10 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  Filter(?t > 2). "//
            + "  Filter(?s > 1). "//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "  ?s a ?t ."//
            + "  ?u <uri:talksTo> ?s . "//
            + "}";//


    private String q11 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  Filter(?t > 2). "//
            + "  Filter(?s > 1). "//
            + "  ?u <uri:talksTo> ?s . "//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "  ?s a ?t ."//
            + "}";//


    private String q12 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  Filter(?t > 2). "//
            + "  Filter(?s > 1). "//
            + "  ?s a ?t ."//
            + "  ?u <uri:talksTo> ?s . "//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "}";//


    private String q13 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  Filter(?t > 2). "//
            + "  Filter(?s > 1). "//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "  ?u <uri:talksTo> ?s . "//
            + "  ?s a ?t ."//
            + "}";//


    private String q14 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  Filter(?t > 2). "//
            + "  Filter(?s > 1). "//
            + "  ?u <uri:talksTo> ?s . "//
            + "  ?s a ?t ."//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "}";//


    private String q15 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  Filter(?s > 1). "//
            + "  Filter(?t > 2). "//
            + "  ?s a ?t ."//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "  ?u <uri:talksTo> ?s . "//
            + "}";//




    @Test
    public void testTwoNodeOrder() {

        SPARQLParser parser = new SPARQLParser();

        ParsedQuery pq1 = null;
        ParsedQuery pq2 = null;

        try {
            pq1 = parser.parseQuery(q7, null);
            pq2 = parser.parseQuery(q8, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Set<TupleExpr> tupSet = Sets.newHashSet();
        tupSet.add(pq1.getTupleExpr());

        TupleExecutionPlanGenerator tep = new TupleExecutionPlanGenerator();
        Iterator<TupleExpr> processedTups = tep.getPlans(tupSet.iterator());

        List<TupleExpr> processedTupList = Lists.newArrayList();

        int size = 0;

        while(processedTups.hasNext()) {
            Assert.assertTrue(processedTups.hasNext());
            processedTupList.add(processedTups.next());
            size++;
        }

        Assert.assertEquals(2, size);

        Assert.assertTrue(processedTupList.get(0).equals(pq2.getTupleExpr()));
        Assert.assertTrue(processedTupList.get(1).equals(pq1.getTupleExpr()));

    }





    @Test
    public void testThreeNodeOrder() {

        SPARQLParser parser = new SPARQLParser();

        ParsedQuery pq1 = null;
        ParsedQuery pq2 = null;
        ParsedQuery pq3 = null;
        ParsedQuery pq4 = null;
        ParsedQuery pq5 = null;
        ParsedQuery pq6 = null;

        try {
            pq1 = parser.parseQuery(q1, null);
            pq2 = parser.parseQuery(q2, null);
            pq3 = parser.parseQuery(q3, null);
            pq4 = parser.parseQuery(q4, null);
            pq5 = parser.parseQuery(q5, null);
            pq6 = parser.parseQuery(q6, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Set<TupleExpr> tupSet = Sets.newHashSet();
        tupSet.add(pq1.getTupleExpr());

        TupleExecutionPlanGenerator tep = new TupleExecutionPlanGenerator();
        Iterator<TupleExpr> processedTups= tep.getPlans(tupSet.iterator());

        List<TupleExpr> processedTupList = Lists.newArrayList();

        int size = 0;

        while(processedTups.hasNext()) {
            Assert.assertTrue(processedTups.hasNext());
            processedTupList.add(processedTups.next());
            size++;
        }

        Assert.assertTrue(!processedTups.hasNext());
        Assert.assertEquals(6, size);

        Assert.assertTrue(processedTupList.get(5).equals(pq1.getTupleExpr()));
        Assert.assertTrue(processedTupList.get(0).equals(pq2.getTupleExpr()));
        Assert.assertTrue(processedTupList.get(2).equals(pq3.getTupleExpr()));
        Assert.assertTrue(processedTupList.get(4).equals(pq4.getTupleExpr()));
        Assert.assertTrue(processedTupList.get(1).equals(pq5.getTupleExpr()));
        Assert.assertTrue(processedTupList.get(3).equals(pq6.getTupleExpr()));

    }



    @Test
    public void testThreeNodeOrderFilter() {

        SPARQLParser parser = new SPARQLParser();

        ParsedQuery pq1 = null;
        ParsedQuery pq2 = null;
        ParsedQuery pq3 = null;
        ParsedQuery pq4 = null;
        ParsedQuery pq5 = null;
        ParsedQuery pq6 = null;
        ParsedQuery pq7 = null;

        try {
            pq1 = parser.parseQuery(q9, null);
            pq2 = parser.parseQuery(q10, null);
            pq3 = parser.parseQuery(q11, null);
            pq4 = parser.parseQuery(q12, null);
            pq5 = parser.parseQuery(q13, null);
            pq6 = parser.parseQuery(q14, null);
            pq7 = parser.parseQuery(q15, null);
        } catch (MalformedQueryException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println(pq1.getTupleExpr());

        Set<TupleExpr> tupSet = Sets.newHashSet();
        tupSet.add(pq7.getTupleExpr());

        TupleExecutionPlanGenerator tep = new TupleExecutionPlanGenerator();
        Iterator<TupleExpr> processedTups= tep.getPlans(tupSet.iterator());

        List<TupleExpr> processedTupList = Lists.newArrayList();

        int size = 0;

        while(processedTups.hasNext()) {

            Assert.assertTrue(processedTups.hasNext());
            TupleExpr te = processedTups.next();
            processedTupList.add(te);
            System.out.println("Processed tups are " + te);
            size++;
        }

        Assert.assertTrue(!processedTups.hasNext());
        Assert.assertEquals(6, size);

        Assert.assertTrue(processedTupList.get(5).equals(pq1.getTupleExpr()));
        Assert.assertTrue(processedTupList.get(0).equals(pq2.getTupleExpr()));
        Assert.assertTrue(processedTupList.get(2).equals(pq3.getTupleExpr()));
        Assert.assertTrue(processedTupList.get(4).equals(pq4.getTupleExpr()));
        Assert.assertTrue(processedTupList.get(1).equals(pq5.getTupleExpr()));
        Assert.assertTrue(processedTupList.get(3).equals(pq6.getTupleExpr()));

    }










}
