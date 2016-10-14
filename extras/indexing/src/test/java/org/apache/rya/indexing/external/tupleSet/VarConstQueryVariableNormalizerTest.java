package org.apache.rya.indexing.external.tupleSet;

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


import java.util.List;
import java.util.Set;

import org.apache.rya.indexing.pcj.matching.QueryVariableNormalizer;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class VarConstQueryVariableNormalizerTest {

    private String query1 = " " //
            + "SELECT ?person ?address ?otherValue" //
            + "{"  //
            + "?person a <uri:Person>. " //
            + "?person <uri:hasName> <uri:name>."//
            + "?person <uri:hasAddress> ?address." //
            + "?person <uri:blah> ?otherValue" //
            + "}"; //

    private String index1 = " " //
            + "SELECT ?X ?Y ?Z ?W" //
            + "{"//
            + "?X a <uri:Person>.  " //
            + "?X <uri:hasName> ?Y."//
            + "?X <uri:hasAddress> ?Z." //
            + "?X <uri:blah> ?W" //
            + "}"; //
    
    
    
    private String q4 = ""//
            + "SELECT ?s ?t ?u " //
            + "{" //
            + "  ?s a ?t . "//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u "//
            + "}";//
    
    
    
    private String q7 = ""//
            + "SELECT ?s ?t ?u ?x ?y ?z " //
            + "{" //
            + "  ?s a ?t ."//
            + "  ?x a ?y ."//
            + "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
            + "  ?y <http://www.w3.org/2000/01/rdf-schema#label> ?z ."//
            + "}";//

    private String q8 = ""//
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
            + "  ?f <uri:talksTo> ?m . "//
            + "  ?m <uri:talksTo> ?a . "//
            + "  ?o <uri:talksTo> ?r . "//
            + "}";//

    private String q9 = ""//
            + "SELECT ?f  ?d ?e ?c ?n ?p ?a ?r " //
            + "{" //
            + "  ?f a <uri:dog> ."//
            + "  ?e a <uri:chicken> ."//
            + "  ?n a <uri:cow> ."//
            + "  ?a a <uri:elephant> ."//
            + "  <uri:dog> <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
            + "  <uri:chicken> <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
            + "  <uri:cow> <http://www.w3.org/2000/01/rdf-schema#label> ?p ."//
            + "  <uri:elephant> <http://www.w3.org/2000/01/rdf-schema#label> ?r ."//
            + "  ?d <uri:talksTo> ?f . "//
            + "  ?c <uri:talksTo> ?e . "//
            + "  ?p <uri:talksTo> ?n . "//
            + "  ?r <uri:talksTo> ?a . "//
            + "}";//

    private String q10 = ""//
            + "SELECT ?f ?m ?d " //
            + "{" //
            + "  ?f a ?m ."//
            + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
            + "  ?d <uri:talksTo> ?f . "//
            + "}";//
    
    String q15 = ""//
            + "SELECT ?x ?y ?z ?w " //
            + "{" //
            + "  ?x ?y ?z ."//
            + "  ?y ?z ?w ."//
            + "}";//

    String q16 = ""//
            + "SELECT ?a ?b ?c " //
            + "{" //
            + "  ?a ?b ?c ."//
            + "}";//
    
    String q17 = ""//
            + "SELECT ?q ?r " //
            + "{" //
            + "  ?q ?r \"url:\" ."//
            + "}";//
    
    private String q18 = ""//
            + "SELECT ?f ?m ?d ?e ?l ?c ?n ?o ?p ?a ?r " //
            + "{" //
            + "  ?f a ?m ."//
            + "  ?e a ?l ."//
            + "  ?n a ?o ."//
            + "  ?a a <uri:elephant> ."//
            + "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
            + "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
            + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?p ."//
            + "  <uri:elephant> <http://www.w3.org/2000/01/rdf-schema#label> ?r ."//
            + "  ?d <uri:talksTo> ?f . "//
            + "  ?c <uri:talksTo> ?e . "//
            + "  ?p <uri:talksTo> ?n . "//
            + "  ?r <uri:talksTo> ?a . "//
            + "}";//
    
    
    String q32 = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
            + "SELECT ?feature ?point " //
            + "{" //
            + "  ?feature a geo:Feature . "//
            + "  ?feature geo:hasGeometry ?point . "//
            + "  ?point a geo:Point . "//
            + "  ?point geo:asWKT \"wkt\" . "//
            + "  FILTER(geof:sfWithin(\"wkt\", \"Polygon\")) " //
            + "}";//
    
    
     String q33 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
             + "SELECT ?person ?commentmatch ?labelmatch" //
             + "{" //
             + "  ?person a <http://example.org/ontology/Person> . "//
             + "  ?person <http://www.w3.org/2000/01/rdf-schema#comment> ?labelmatch . "//
             + "  ?person <http://www.w3.org/2000/01/rdf-schema#comment> ?commentmatch . "//
             + "  FILTER(fts:text(?labelmatch, \"sally\")) . " //
             + "  FILTER(fts:text(?commentmatch, \"bob\"))  " //
             + "}";//
     
     
     String q34 = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
                + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
                + "SELECT ?a ?b ?c ?d" //
                + "{" //
                + "  ?a a geo:Feature . "//
                + "  ?b a geo:Point . "//
                + "  ?b geo:asWKT ?c . "//
                + "  FILTER(geof:sfWithin(?c, ?d)) " //
                + "}";//
     
     
     String q35 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
             + "SELECT ?a ?b ?c" //
             + "{" //
             + "  ?a <http://www.w3.org/2000/01/rdf-schema#comment> ?b . "//
             + "  FILTER(fts:text(?b, ?c))  " //
             + "}";//
     

    
    
    
    
    
    
    /**
     * @param tuple1
     * @param tuple2
     * @return
     * @throws Exception
     */
    public boolean tupleEquals(TupleExpr tuple1, TupleExpr tuple2) throws Exception {
        
        Set<StatementPattern> spSet1 = Sets.newHashSet(StatementPatternCollector.process(tuple1));
        Set<StatementPattern> spSet2 = Sets.newHashSet(StatementPatternCollector.process(tuple2));
       
        return spSet1.equals(spSet2);

    }

    /**
     * @param tuple1
     * @param tuple2
     * @return
     * @throws Exception
     */
    public boolean isTupleSubset(TupleExpr tuple1, TupleExpr tuple2) throws Exception {

        Set<StatementPattern> spSet1 = Sets.newHashSet(StatementPatternCollector.process(tuple1));
        Set<StatementPattern> spSet2 = Sets.newHashSet(StatementPatternCollector.process(tuple2));

        return (Sets.intersection(spSet1, spSet2).equals(spSet2));

    }
    
    


    /**
     * @throws Exception
     *             Tests QueryVariableNormalizerContext with two queries whose
     *             StatementPattern nodes contain no constant Vars.
     */
    @Test
    public void testNoConstants() throws Exception {

        SPARQLParser parser1 = new SPARQLParser();
        SPARQLParser parser2 = new SPARQLParser();

        ParsedQuery pq1 = parser1.parseQuery(q15, null);
        ParsedQuery pq2 = parser2.parseQuery(q16, null);

        List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
                pq2.getTupleExpr());

        Assert.assertEquals(2,normalize.size());
        for (TupleExpr s : normalize) {
            Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), s));
        }

        pq1 = parser1.parseQuery(q16, null);
        pq2 = parser2.parseQuery(q17, null);
        normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(), pq2.getTupleExpr());

        Assert.assertTrue(normalize.size() == 0);

    }
    

    
        
    
    @Test
    public void queryConstantNodeOneMatch() throws Exception {

        SPARQLParser p = new SPARQLParser();

        ParsedQuery pq1 = p.parseQuery(query1, null);
        ParsedQuery pq2 = p.parseQuery(index1, null);
        

        List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
                pq2.getTupleExpr());
               
        Assert.assertEquals(1, normalize.size());
        
        for(TupleExpr te: normalize) {
            Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), te));
        }        
    }
    
    
    
    /**
     * @throws Exception
     *             Tests QueryVariableNormalizerContext on the large query q9
     *             with with a smaller, potential index q10 to see if the
     *             correct number of outputs are produced.
     */
    @Test
    public void querConstNodeFourMatch() throws Exception {

        SPARQLParser parser1 = new SPARQLParser();
        SPARQLParser parser2 = new SPARQLParser();

        ParsedQuery pq1 = parser1.parseQuery(q9, null);
        ParsedQuery pq2 = parser2.parseQuery(q10, null);

        List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
                pq2.getTupleExpr());

        
        //System.out.println(normalize);
        
        Assert.assertEquals(4, normalize.size());
        
        for(TupleExpr te: normalize) {
            Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), te));
        }
        

        

    }
    
    
    @Test
    public void queryConstNodeSixMatch() throws Exception {

        SPARQLParser parser1 = new SPARQLParser();
        SPARQLParser parser2 = new SPARQLParser();

        ParsedQuery pq1 = parser1.parseQuery(q9, null);
        ParsedQuery pq2 = parser2.parseQuery(q18, null);

        List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
                pq2.getTupleExpr());

        
        Assert.assertEquals(6, normalize.size());
        
        //System.out.println("tuple expr is " +pq1.getTupleExpr() + " and normalized tuples are " + normalize);
        
        for(TupleExpr te: normalize) {
            Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), te));
        }
        

    }
    
    
    
    @Test
    public void queryConstGeoFilter() throws Exception {

        SPARQLParser parser1 = new SPARQLParser();
        SPARQLParser parser2 = new SPARQLParser();

        ParsedQuery pq1 = parser1.parseQuery(q32, null);
        ParsedQuery pq2 = parser2.parseQuery(q34, null);

        
        List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
                pq2.getTupleExpr());

        
    
        Assert.assertEquals(1, normalize.size());
        
        for(TupleExpr te: normalize) {
            Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), te));
        }
        
        
        
        FilterCollector fc1 = new FilterCollector();
        pq1.getTupleExpr().visit(fc1);
        List<QueryModelNode> fList1 = fc1.getFilters();
        
        for(TupleExpr te: normalize) {
            FilterCollector fc2 = new FilterCollector();
            te.visit(fc2);
            List<QueryModelNode> fList2 = fc2.getFilters();
            
            for(QueryModelNode q: fList2) {
                Assert.assertTrue(fList1.contains(q));
            }
        }
        

    }
    
    
    @Test
    public void queryConstFreeTextFilter() throws Exception {

        SPARQLParser parser1 = new SPARQLParser();
        SPARQLParser parser2 = new SPARQLParser();

        ParsedQuery pq1 = parser1.parseQuery(q33, null);
        ParsedQuery pq2 = parser2.parseQuery(q35, null);
        
        System.out.println(pq1.getTupleExpr());
        
        List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
                pq2.getTupleExpr());
        
        
        
        Assert.assertEquals(2, normalize.size());
        
        for(TupleExpr te: normalize) {
            Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), te));
        }
        
        
        
        FilterCollector fc1 = new FilterCollector();
        pq1.getTupleExpr().visit(fc1);
        List<QueryModelNode> fList1 = fc1.getFilters();
        
        for(TupleExpr te: normalize) {
            FilterCollector fc2 = new FilterCollector();
            te.visit(fc2);
            List<QueryModelNode> fList2 = fc2.getFilters();
            
            for(QueryModelNode q: fList2) {
                Assert.assertTrue(fList1.contains(q));
            }
        }
        
        
        

    }
    
    
    
    
    
    @Test
    public void queryConstNodeTwoMatch() throws Exception {

        SPARQLParser parser1 = new SPARQLParser();
        SPARQLParser parser2 = new SPARQLParser();

        ParsedQuery pq1 = parser1.parseQuery(q7, null);
        ParsedQuery pq2 = parser2.parseQuery(q4, null);

        List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
                pq2.getTupleExpr());

        
        Assert.assertEquals(2, normalize.size());
        
        for(TupleExpr te: normalize) {
            Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), te));
        }
        




    }
    
   
    
    
    
    
    
    
    @Test
    public void queryNAryListMatch() throws Exception {

        
        
        String q1 = ""//
                + "SELECT ?a ?b ?c ?d ?e ?f ?q ?g ?h " //
                + "{" //
                + " GRAPH ?x { " //
                + "  ?a a ?b ."//
                + "  ?b <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
                + "  ?d <uri:talksTo> ?e . "//
                + "  FILTER(bound(?f) && sameTerm(?a,?b)&&bound(?q)). " //
                + "  FILTER ( ?e < ?f && (?a > ?b || ?c = ?d) ). " //
                + "  FILTER(?g IN (1,2,3) && ?h NOT IN(5,6,7)). " //
                + "  ?x <http://www.w3.org/2000/01/rdf-schema#label> ?g. "//
                + "  ?b a ?q ."//
                + "     }"//
                + "}";//
        
        
        String q2 = ""//
                + "SELECT ?m ?n ?r ?y " //
                + "{" //
                + " GRAPH ?q { " //
                + "  FILTER(?m IN (1,?y,3) && ?n NOT IN(?r,6,7)). " //
                + "  ?q <http://www.w3.org/2000/01/rdf-schema#label> ?m. "//
                + "     }"//
                + "}";//
        
        
        SPARQLParser parser1 = new SPARQLParser();
        SPARQLParser parser2 = new SPARQLParser();

        ParsedQuery pq1 = parser1.parseQuery(q1, null);
        ParsedQuery pq2 = parser2.parseQuery(q2, null);

        List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
                pq2.getTupleExpr());

        
        Assert.assertEquals(1, normalize.size());
        
        for(TupleExpr te: normalize) {
            Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), te));
        }
        
        FilterCollector fc1 = new FilterCollector();
        pq1.getTupleExpr().visit(fc1);
        List<QueryModelNode> fList1 = fc1.getFilters();
        
        for(TupleExpr te: normalize) {
            FilterCollector fc2 = new FilterCollector();
            te.visit(fc2);
            List<QueryModelNode> fList2 = fc2.getFilters();
            
            for(QueryModelNode q: fList2) {
                Assert.assertTrue(fList1.contains(q));
            }
        }



    }
    
    
   
    
    
    
    @Test
    public void queryCompoundFilterMatch() throws Exception {

        
        
        String q17 = ""//
                + "SELECT ?j ?k ?l ?m ?n ?o " //
                + "{" //
                + " GRAPH ?z { " //
                + "  ?j <uri:talksTo> ?k . "//
                + "  FILTER ( ?k < ?l && (?m > ?n || ?o = ?j) ). " //
                + "     }"//
                + "}";//
        
//        String q18 = ""//
//                + "SELECT ?r ?s ?t ?u " //
//                + "{" //
//                + " GRAPH ?q { " //
//                + "  FILTER(bound(?r) && sameTerm(?s,?t)&&bound(?u)). " //
//                + "  ?t a ?u ."//
//                + "     }"//
//                + "}";//
        
        
        
        String q19 = ""//
                + "SELECT ?a ?b ?c ?d ?f ?q ?g ?h " //
                + "{" //
                + " GRAPH ?x { " //
                + "  ?a a ?b ."//
                + "  ?b <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
                + "  ?d <uri:talksTo> \"5\" . "//
                + "  FILTER ( \"5\" < ?f && (?a > ?b || ?c = ?d) ). " //
                + "  FILTER(bound(?f) && sameTerm(?a,?b)&&bound(?q)). " //
                + "  FILTER(?g IN (1,2,3) && ?h NOT IN(5,6,7)). " //
                + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?g. "//
                + "  ?b a ?q ."//
                + "     }"//
                + "}";//
        
        
//        String q20 = ""//
//                + "SELECT ?m ?n ?o " //
//                + "{" //
//                + " GRAPH ?q { " //
//                + "  FILTER(?m IN (1,?o,3) && ?n NOT IN(5,6,7)). " //
//                + "  ?n <http://www.w3.org/2000/01/rdf-schema#label> ?m. "//
//                + "     }"//
//                + "}";//
        
        
        
        
        SPARQLParser parser1 = new SPARQLParser();
        SPARQLParser parser2 = new SPARQLParser();

        ParsedQuery pq1 = parser1.parseQuery(q19, null);
        ParsedQuery pq2 = parser2.parseQuery(q17, null);

        List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
                pq2.getTupleExpr());

        
        
        System.out.println(normalize);
        
        Assert.assertEquals(1, normalize.size());
        
        for(TupleExpr te: normalize) {
            Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), te));
        }
        
        FilterCollector fc1 = new FilterCollector();
        pq1.getTupleExpr().visit(fc1);
        List<QueryModelNode> fList1 = fc1.getFilters();
        
        for(TupleExpr te: normalize) {
            FilterCollector fc2 = new FilterCollector();
            te.visit(fc2);
            List<QueryModelNode> fList2 = fc2.getFilters();
            
            for(QueryModelNode q: fList2) {
                Assert.assertTrue(fList1.contains(q));
            }
        }



    }
    
    
    
    
    
//    @Test
//    public void queryCompoundFilterMatch2() throws Exception {
//
//        
//        
//     
//        
//        
//        String q19 = ""//
//                + "SELECT ?a ?b ?c ?d ?f ?q ?g ?h " //
//                + "{" //
//                + " GRAPH ?x { " //
//                + "  ?a a ?b ."//
//                + "  ?b <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
//                + "  ?d <uri:talksTo> \"5\" . "//
//                + "  FILTER ( \"5\" < ?f && (?a > ?b || ?c = ?d) ). " //
//                + "  FILTER(bound(?f) && sameTerm(?a,?b)&&bound(?q)). " //
//                + "  FILTER(?g IN (1,5,3) && ?h NOT IN(5,6,7)). " //
//                + "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?g. "//
//                + "  ?b a ?q ."//
//                + "     }"//
//                + "}";//
//        
//        
//        String q20 = ""//
//                + "SELECT ?m ?n ?o ?f ?a ?b ?c ?d " //
//                + "{" //
//                + " GRAPH ?q { " //
//                + "  ?d <uri:talksTo> ?o . "//
//                + "  FILTER ( ?o < ?f && (?a > ?b || ?c = ?d) ). " //
//                + "  FILTER(?m IN (1,?o,3) && ?n NOT IN(5,6,7)). " //
//                + "  ?n <http://www.w3.org/2000/01/rdf-schema#label> ?m. "//
//                + "     }"//
//                + "}";//
//        
//        
//        
//        
//        SPARQLParser parser1 = new SPARQLParser();
//        SPARQLParser parser2 = new SPARQLParser();
//
//        ParsedQuery pq1 = parser1.parseQuery(q19, null);
//        ParsedQuery pq2 = parser2.parseQuery(q20, null);
//
//        List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
//                pq2.getTupleExpr());
//
//        
//        
//        System.out.println(normalize);
//        
//        Assert.assertEquals(1, normalize.size());
//        
//        for(TupleExpr te: normalize) {
//            Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), te));
//        }
//        
//        FilterCollector fc1 = new FilterCollector();
//        pq1.getTupleExpr().visit(fc1);
//        List<QueryModelNode> fList1 = fc1.getFilters();
//        
//        for(TupleExpr te: normalize) {
//            FilterCollector fc2 = new FilterCollector();
//            te.visit(fc2);
//            List<QueryModelNode> fList2 = fc2.getFilters();
//            
//            for(QueryModelNode q: fList2) {
//                Assert.assertTrue(fList1.contains(q));
//            }
//        }
//
//
//
//    }
//    
//    
    
    
    
    
    
    
    
    
    
    
    
    private static class FilterCollector extends QueryModelVisitorBase<RuntimeException> {

        private List<QueryModelNode> filterList = Lists.newArrayList();

        public List<QueryModelNode> getFilters() {
            return filterList;
        }

        @Override
        public void meet(Filter node) {
            filterList.add(node.getCondition());
            super.meet(node);
        }

    }

    
    
    


}
