package org.apache.rya.indexing.external;

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
import java.util.List;
import java.util.Set;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.SimpleExternalTupleSet;
import org.apache.rya.indexing.pcj.matching.PCJOptimizer;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Sets;

public class PrecompJoinOptimizerVarToConstTest {




    String q15 = ""//
            + "SELECT ?a ?b ?c ?d ?e ?f ?q " //
            + "{" //
            + " GRAPH ?x { " //
            + "  ?a a ?b ."//
            + "  ?b <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
            + "  ?d <uri:talksTo> ?e . "//
            + "  FILTER ( ?e < ?f && (?a > ?b || ?c = ?d) ). " //
            + "  FILTER(bound(?f) && sameTerm(?a,?b)&&bound(?q)). " //
            + "  ?b a ?q ."//
            + "     }"//
            + "}";//




    String q17 = ""//
            + "SELECT ?j ?k ?l ?m ?n ?o " //
            + "{" //
            + " GRAPH ?z { " //
            + "  ?l a ?m. " //
            + "  ?n a ?o. " //
            + "  ?j <uri:talksTo> ?k . "//
            + "  FILTER ( ?k < ?l && (?m > ?n || ?o = ?j) ). " //
            + "     }"//
            + "}";//

    String q18 = ""//
            + "SELECT ?r ?s ?t ?u " //
            + "{" //
            + " GRAPH ?q { " //
            + "  FILTER(bound(?r) && sameTerm(?s,?t)&&bound(?u)). " //
            + "  ?t a ?u ."//
            + "  ?s a ?r ."//
            + "     }"//
            + "}";//



    String q19 = ""//
            + "SELECT ?a ?c ?d ?f ?q " //
            + "{" //
            + " GRAPH ?x { " //
            + "  ?f a ?a ."//
            + " \"3\" a ?c . "//
            + "  ?d <uri:talksTo> \"5\" . "//
            + "  FILTER ( \"5\" < ?f && (?a > \"3\" || ?c = ?d) ). " //
            + "  FILTER(bound(?f) && sameTerm(?a,\"3\") && bound(?q)). " //
            + "  \"3\" a ?q ."//
            + "  ?a a ?f ."//
            + "     }"//
            + "}";//






    String q21 = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
            + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
            + "SELECT ?feature ?point " //
            + "{" //
            + "  ?feature a geo:Feature . "//
            + "  ?feature geo:hasGeometry ?point . "//
            + "  ?point a geo:Point . "//
            + "  ?point geo:asWKT \"wkt\" . "//
            + "  FILTER(geof:sfWithin(\"wkt\", \"Polygon\")) " //
            + "}";//


     String q22 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
             + "SELECT ?person " //
             + "{" //
             + "  ?person a <http://example.org/ontology/Person> . "//
             + "  ?person <http://www.w3.org/2000/01/rdf-schema#label> \"sally\" . "//
             + "  ?person <http://www.w3.org/2000/01/rdf-schema#label> \"john\" . "//
             + "  FILTER(fts:text(\"sally\", \"bob\")) . " //
             + "  FILTER(fts:text(\"john\", \"harry\"))  " //
             + "  ?person <uri:hasName> \"bob\". "//
             + "  ?person <uri:hasName> \"harry\". "//
             + "}";//


     String q23 = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
                + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
                + "SELECT ?a ?b ?c " //
                + "{" //
                + "  ?a a geo:Feature . "//
                + "  ?b a geo:Point . "//
                + "  ?b geo:asWKT ?c . "//
                + "  FILTER(geof:sfWithin(?c, \"Polygon\")) " //
                + "}";//


     String q24 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
             + "SELECT ?f ?g ?h" //
             + "{" //
             + "  ?f <http://www.w3.org/2000/01/rdf-schema#label> ?g . "//
             + "  FILTER(fts:text(?g,?h)).  " //
             + " ?f <uri:hasName> ?h. " //
             + "}";//


     String q25 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
             + "SELECT ?person ?point" //
             + "{" //
             + "  ?person <http://www.w3.org/2000/01/rdf-schema#label> \"label\" . "//
             + "  FILTER(fts:text(\"label\", \"bob\")) . " //
             + "  ?person <uri:hasName> \"bob\" . " //
             + "  ?person a ?point. " //
             + "  \"bob\" a <http://example.org/ontology/Person> . "//
             + "  ?person <http://www.w3.org/2000/01/rdf-schema#commentmatch> \"comment\" . "//
             + "  FILTER((?person > ?point) || (?person = \"comment\")). "
             + "  FILTER(fts:text(\"comment\", \"bob\"))  " //
             + "}";//


     String q26 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
             + "SELECT ?a ?b ?c ?d " //
             + "{" //
             + "  ?a a ?c. " //
             + "  ?d a <http://example.org/ontology/Person> . "//
             + "  ?a <http://www.w3.org/2000/01/rdf-schema#commentmatch> ?b . "//
             + "  FILTER((?a > ?c) || (?a = ?b)). "
             + "  FILTER(fts:text(?b, ?d)) . " //
             + "}";//



     String q27 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
             + "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
             + "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
             + "SELECT ?person ?feature ?point " //
             + "{" //
             + "  ?person <http://www.w3.org/2000/01/rdf-schema#label> \"label\" . "//
             + "  FILTER(fts:text(\"label\", \"bob\")) . " //
             + "  ?person <uri:hasName> \"bob\" . " //
             + "  ?person a ?point. " //
             + "  \"bob\" a <http://example.org/ontology/Person> . "//
             + "  ?person <http://www.w3.org/2000/01/rdf-schema#commentmatch> \"comment\" . "//
             + "  FILTER((?person > ?point) || (?person = \"comment\")). "
             + "  FILTER(fts:text(\"comment\", \"bob\"))  " //
             + "  ?feature a geo:Feature . "//
             + "  ?point a geo:Point . "//
             + "  ?point geo:asWKT \"wkt\" . "//
             + "  FILTER(geof:sfWithin(\"wkt\", \"Polygon\")) " //
             + "}";//

     String q28 = ""//
             + "SELECT ?m ?n " //
             + "{" //
             + "  FILTER(?m IN (1,2,3) && ?n NOT IN(5,6,7)). " //
             + "  ?n <http://www.w3.org/2000/01/rdf-schema#label> ?m. "//
             + "}";//


    @Test
    public void testContextFilterFourIndex() throws Exception {

        final SPARQLParser parser1 = new SPARQLParser();
        final SPARQLParser parser3 = new SPARQLParser();
        final SPARQLParser parser4 = new SPARQLParser();
        final ParsedQuery pq1 = parser1.parseQuery(q19, null);
        final ParsedQuery pq3 = parser3.parseQuery(q17, null);
        final ParsedQuery pq4 = parser4.parseQuery(q18, null);

        System.out.println("Query is " + pq1.getTupleExpr());
        System.out.println("Indexes are " + pq3.getTupleExpr()+ " , " +pq4.getTupleExpr());

        final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(new Projection(pq3.getTupleExpr()));
        final SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(new Projection(pq4.getTupleExpr()));

        final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

        list.add(extTup3);
        list.add(extTup2);

        final TupleExpr tup = pq1.getTupleExpr().clone();

		final PCJOptimizer pcj = new PCJOptimizer(list, false);
        pcj.optimize(tup, null, null);

        System.out.println("Processed query is " + tup);

        final Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector.process(pq1.getTupleExpr()));

        final Set<QueryModelNode> eTupSet =  PcjIntegrationTestingUtil.getTupleSets(tup);
        Assert.assertEquals(2, eTupSet.size());

        final Set<StatementPattern> set = Sets.newHashSet();

        for (final QueryModelNode s : eTupSet) {
            final Set<StatementPattern> tempSet = Sets.newHashSet(StatementPatternCollector.process(((ExternalTupleSet) s)
                    .getTupleExpr()));
            set.addAll(tempSet);
        }
        Assert.assertTrue(qSet.containsAll(set));
    }




    @Test
    public void testGeoIndexFunction() throws Exception {

        final SPARQLParser parser1 = new SPARQLParser();
        final SPARQLParser parser2 = new SPARQLParser();

        final ParsedQuery pq1 = parser1.parseQuery(q21, null);
        final ParsedQuery pq2 = parser2.parseQuery(q23, null);
        System.out.println("Query is " + pq1.getTupleExpr());
        System.out.println("Index is " + pq2.getTupleExpr());

        final SimpleExternalTupleSet extTup = new SimpleExternalTupleSet(new Projection(pq2.getTupleExpr()));

        final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
        list.add(extTup);

        final TupleExpr tup = pq1.getTupleExpr().clone();
		final PCJOptimizer pcj = new PCJOptimizer(list, false);
        pcj.optimize(tup, null, null);

        System.out.println("Processed query is " + tup);

        final Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector.process(pq1.getTupleExpr()));
        final Set<QueryModelNode> eTupSet =  PcjIntegrationTestingUtil.getTupleSets(tup);
        final Set<StatementPattern> set = Sets.newHashSet();

        Assert.assertEquals(1, eTupSet.size());

        for (final QueryModelNode s : eTupSet) {
            final Set<StatementPattern> tempSet = Sets.newHashSet(StatementPatternCollector.process(((ExternalTupleSet) s)
                    .getTupleExpr()));
            set.addAll(tempSet);

        }

        Assert.assertTrue(qSet.containsAll(set));
    }



    @Test
    public void testFreeTestIndexFunction() throws Exception {

        final SPARQLParser parser1 = new SPARQLParser();
        final SPARQLParser parser2 = new SPARQLParser();

        final ParsedQuery pq1 = parser1.parseQuery(q22, null);
        final ParsedQuery pq2 = parser2.parseQuery(q24, null);

        System.out.println("Query is " + pq1.getTupleExpr());
        System.out.println("Index is " + pq2.getTupleExpr());

        final SimpleExternalTupleSet extTup = new SimpleExternalTupleSet(new Projection(pq2.getTupleExpr()));

        final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
        list.add(extTup);

        final TupleExpr tup = pq1.getTupleExpr().clone();
		final PCJOptimizer pcj = new PCJOptimizer(list, false);
        pcj.optimize(tup, null, null);

        System.out.println("Processed query is " + tup);

        final Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector.process(pq1.getTupleExpr()));

        final Set<QueryModelNode> eTupSet =  PcjIntegrationTestingUtil.getTupleSets(tup);
        final Set<StatementPattern> set = Sets.newHashSet();

        Assert.assertEquals(2, eTupSet.size());

        for (final QueryModelNode s : eTupSet) {
            final Set<StatementPattern> tempSet = Sets.newHashSet(StatementPatternCollector.process(((ExternalTupleSet) s)
                    .getTupleExpr()));
            set.addAll(tempSet);

        }
        Assert.assertTrue(qSet.containsAll(set));
    }


    @Test
    public void testThreeIndexGeoFreeCompareFilterMix() throws Exception {

        final SPARQLParser parser1 = new SPARQLParser();
        final SPARQLParser parser2 = new SPARQLParser();
        final SPARQLParser parser3 = new SPARQLParser();

        final ParsedQuery pq1 = parser1.parseQuery(q25, null);
        final ParsedQuery pq2 = parser2.parseQuery(q24, null);
        final ParsedQuery pq3 = parser3.parseQuery(q26, null);

        System.out.println("Query is " + pq1.getTupleExpr());
        System.out.println("Indexes are " + pq2.getTupleExpr() + " and " + pq3.getTupleExpr());


        final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(new Projection(pq2.getTupleExpr()));
        final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(new Projection(pq3.getTupleExpr()));

        final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
        list.add(extTup1);
        list.add(extTup2);

        final TupleExpr tup = pq1.getTupleExpr().clone();
		final PCJOptimizer pcj = new PCJOptimizer(list, false);
        pcj.optimize(tup, null, null);

        System.out.println("Processed query is " + tup);

        final Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector.process(pq1.getTupleExpr()));
        final Set<QueryModelNode> eTupSet =  PcjIntegrationTestingUtil.getTupleSets(tup);
        final Set<StatementPattern> set = Sets.newHashSet();

        Assert.assertEquals(2, eTupSet.size());

        for (final QueryModelNode s : eTupSet) {
            final Set<StatementPattern> tempSet = Sets.newHashSet(StatementPatternCollector.process(((ExternalTupleSet) s)
                    .getTupleExpr()));
            set.addAll(tempSet);

        }
        Assert.assertTrue(qSet.containsAll(set));

    }

    @Test
    public void testFourIndexGeoFreeCompareFilterMix() throws Exception {

        final SPARQLParser parser1 = new SPARQLParser();
        final SPARQLParser parser2 = new SPARQLParser();
        final SPARQLParser parser3 = new SPARQLParser();
        final SPARQLParser parser4 = new SPARQLParser();


        final ParsedQuery pq1 = parser1.parseQuery(q27, null);
        final ParsedQuery pq2 = parser2.parseQuery(q23, null);
        final ParsedQuery pq3 = parser3.parseQuery(q26, null);
        final ParsedQuery pq4 = parser4.parseQuery(q24, null);

        System.out.println("Query is " + pq1.getTupleExpr());
        System.out.println("Indexes are " + pq2.getTupleExpr() + " , " + pq3.getTupleExpr() + " , " + pq4.getTupleExpr());

        final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(new Projection(pq2.getTupleExpr()));
        final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(new Projection(pq3.getTupleExpr()));
        final SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(new Projection(pq4.getTupleExpr()));

        final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

        list.add(extTup1);
        list.add(extTup2);
        list.add(extTup3);

        final TupleExpr tup = pq1.getTupleExpr().clone();
		final PCJOptimizer pcj = new PCJOptimizer(list, false);
        pcj.optimize(tup, null, null);

        System.out.println("Processed query is " + tup);

        final Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector.process(pq1.getTupleExpr()));
        final Set<QueryModelNode> eTupSet =  PcjIntegrationTestingUtil.getTupleSets(tup);
        final Set<StatementPattern> set = Sets.newHashSet();

        Assert.assertEquals(3, eTupSet.size());

        for (final QueryModelNode s : eTupSet) {
            final Set<StatementPattern> tempSet = Sets.newHashSet(StatementPatternCollector.process(((ExternalTupleSet) s)
                    .getTupleExpr()));
            set.addAll(tempSet);

        }

        Assert.assertTrue(qSet.containsAll(set));

    }







}
