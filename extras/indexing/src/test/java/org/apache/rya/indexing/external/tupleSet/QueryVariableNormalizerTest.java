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
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Sets;



public class QueryVariableNormalizerTest {

	private String q1 = ""//
			+ "SELECT ?e ?l ?c " //
			+ "{" //
			+ "  ?e a ?c . "//
			+ "  ?c <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
			+ "  ?l <uri:talksTo> ?e . "//
			+ "}";//

	private String q2 = ""//
			+ "SELECT ?a ?t ?v  " //
			+ "{" //
			+ "  ?a a ?t . "//
			+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?v . "//
			+ "  ?v <uri:talksTo> ?a . "//
			+ "}";//

	private String q3 = ""//
			+ "SELECT ?f ?m ?d " //
			+ "{" //
			+ "  ?f a ?d . "//
			+ "  ?f <http://www.w3.org/2000/01/rdf-schema#label> ?m "//
			+ "}";//

	private String q4 = ""//
			+ "SELECT ?s ?t ?u " //
			+ "{" //
			+ "  ?s a ?t . "//
			+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u "//
			+ "}";//

	private String q5 = ""//
			+ "SELECT ?f ?m ?d ?s " //
			+ "{" //
			+ "  ?m a ?d . "//
			+ "  ?f a ?m . "//
			+ "  ?f a ?s . "//
			+ "  ?m <uri:talksTo> ?f . "//
			+ "  ?s <http://www.w3.org/2000/01/rdf-schema#label> ?m "//
			+ "}";//

	private String q6 = ""//
			+ "SELECT ?q ?r ?s ?t ?u " //
			+ "{" //
			+ "  ?q a ?r ."//
			+ "  ?r a ?s ."//
			+ "  ?t a ?u ."//
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

	private String q10 = ""//
			+ "SELECT ?f ?m ?d " //
			+ "{" //
			+ "  ?f a ?m ."//
			+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
			+ "  ?d <uri:talksTo> ?f . "//
			+ "}";//

	private String q11 = ""//
			+ "SELECT ?f ?m ?d ?e ?l ?c ?x ?y ?z" //
			+ "{" //
			+ "  ?f a ?m ."//
			+ "  ?m a ?d ."//
			+ "  ?d a ?e ."//
			+ "  ?e a ?l ."//
			+ "  ?l a ?c ."//
			+ "  ?x a ?y ."//
			+ "  ?y a ?z ."//
			+ "  ?z a ?x ."//
			+ "}";//

	private String q12 = ""//
			+ "SELECT ?s ?t ?u ?v " //
			+ "{" //
			+ " \"hello\" ?s ?t ."//
			+ "  ?t a ?u ."//
			+ "  ?u ?v \"m\" . "//
			+ "}";//

	private String q13 = ""//
			+ "SELECT ?x ?y ?z ?w " //
			+ "{" //
			+ "  \"hello\" ?x ?y ."//
			+ "  ?y a ?z ."//
			+ "  ?z ?w \"m\" . "//
			+ "}";//

	private String q14 = ""//
			+ "SELECT ?e ?l ?c " //
			+ "{" //
			+ "  ?c a ?l . "//
			+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?e. "//
			+ "  ?e <uri:talksTo> ?c . "//
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
			+ "SELECT ?f ?m ?d ?e ?l ?c ?n ?o ?p ?a ?h ?r " //
			+ "{" //
			+ "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?r ."//
			+ "  ?f a ?m ."//
			+ "  ?p <uri:talksTo> ?n . "//
			+ "  ?e a ?l ."//
			+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?p ."//
			+ "  ?d <uri:talksTo> ?f . "//
			+ "  ?c <uri:talksTo> ?e . "//
			+ "  ?n a ?o ."//
			+ "  ?a a ?h ."//
			+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
			+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
			+ "  ?r <uri:talksTo> ?a . "//
			+ "}";//

	String q23 = ""//
			+ "SELECT ?f ?m ?d " //
			+ "{" //
			+ " GRAPH ?x { " //
			+ "  ?f a ?m ."//
			+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
			+ "  ?d <uri:talksTo> ?f . "//
			+ "  ?x a ?f. "//
			+ "		}"//
			+ "}";//

	String q22 = ""//
			+ "SELECT ?f ?m ?d " //
			+ "{" //
			+ " GRAPH ?y { " //
			+ "  ?f a ?m ."//
			+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
			+ "  ?d <uri:talksTo> ?f . "//
			+ "  ?y a ?f . "//
			+ "		}"//
			+ "}";//

	String q19 = ""//
			+ "SELECT ?r ?s ?t " //
			+ "{" //
			+ " GRAPH ?u { " //
			+ "  ?r a ?s ."//
			+ "  ?s <http://www.w3.org/2000/01/rdf-schema#label> ?t ."//
			+ "  ?t <uri:talksTo> ?r . "//
			+ "  ?u a ?r . "//
			+ "		}"//
			+ "}";//

	String q20 = ""//
			+ "SELECT ?f ?m ?d " //
			+ "{" //
			+ " GRAPH <https://non-constant> { " //
			+ "  ?f a ?m ."//
			+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
			+ "  ?d <uri:talksTo> ?f . "//
			+ "		}"//
			+ "}";//

	String q21 = ""//
			+ "SELECT ?r ?s ?t " //
			+ "{" //
			+ " GRAPH <https://non-constant> { " //
			+ "  ?r a ?s ."//
			+ "  ?s <http://www.w3.org/2000/01/rdf-schema#label> ?t ."//
			+ "  ?t <uri:talksTo> ?r . "//
			+ "		}"//
			+ "}";//

	private String q24 = ""//
			+ "SELECT ?e ?l ?c ?x ?y ?z " //
			+ "{" //
			+ " GRAPH ?d { " //
			+ "  ?c a ?l . "//
			+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?e. "//
			+ "  ?e <uri:talksTo> ?c . "//
			+ "  ?x a ?y . "//
			+ "  ?y <http://www.w3.org/2000/01/rdf-schema#label> ?z. "//
			+ "  ?z <uri:talksTo> ?x . "//
			+ "}" //
			+ "}";//

	String q25 = ""//
			+ "SELECT ?f ?m ?d " //
			+ "{" //
			+ " GRAPH ?w { " //
			+ "  ?f a ?m ."//
			+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
			+ "  ?d <uri:talksTo> ?f . "//
			+ "		}"//
			+ "}";//

	private String q26 = ""//
			+ "SELECT ?f ?m ?d ?e ?l ?c ?x ?y ?z" //
			+ "{" //
			+ " GRAPH ?w { " //
			+ "  ?f a ?m ."//
			+ "  ?m a ?d ."//
			+ "  ?d a ?e ."//
			+ "  ?e a ?l ."//
			+ "  ?l a ?c ."//
			+ "  ?x a ?y ."//
			+ "  ?y a ?z ."//
			+ "  ?z a ?x ."//
			+ "		}"//
			+ "}";//

	private String q27 = ""//
			+ "SELECT ?q ?r ?s ?t ?u " //
			+ "{" //
			+ " GRAPH ?n { " //
			+ "  ?q a ?r ."//
			+ "  ?r a ?s ."//
			+ "  ?t a ?u ."//
			+ "		}"//
			+ "}";//

	
	
	
	String q30 = ""//
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
			+ "		}"//
			+ "}";//
	
	
	String q31 = ""//
			+ "SELECT ?m ?n " //
			+ "{" //
			+ " GRAPH ?q { " //
			+ "  FILTER(?m IN (1,2,3) && ?n NOT IN(5,6,7)). " //
			+ "  ?q <http://www.w3.org/2000/01/rdf-schema#label> ?m. "//
			+ "		}"//
			+ "}";//
	
	
	String q32 = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
			+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
			+ "SELECT ?feature ?point ?wkt " //
			+ "{" //
			+ "  ?feature a geo:Feature . "//
			+ "  ?feature geo:hasGeometry ?point . "//
			+ "  ?point a geo:Point . "//
			+ "  ?point geo:asWKT ?wkt . "//
			+ "  FILTER(geof:sfWithin(?wkt, \"Polygon\")) " //
			+ "}";//
	
	
	 String q33 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
             + "SELECT ?person ?commentmatch ?labelmatch" //
             + "{" //
             + "  ?person a <http://example.org/ontology/Person> . "//
             + "  ?person <http://www.w3.org/2000/01/rdf-schema#label> ?labelmatch . "//
             + "  ?person <http://www.w3.org/2000/01/rdf-schema#comment> ?commentmatch . "//
             + "  FILTER(fts:text(?labelmatch, \"bob\")) . " //
             + "  FILTER(fts:text(?commentmatch, \"bob\"))  " //
             + "}";//
	 
	 
	 String q34 = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?a ?b ?c " //
				+ "{" //
				+ "  ?a a geo:Feature . "//
				+ "  ?b a geo:Point . "//
				+ "  ?b geo:asWKT ?c . "//
				+ "  FILTER(geof:sfWithin(?c, \"Polygon\")) " //
				+ "}";//
	 
	 
	 String q35 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
             + "SELECT ?a ?b " //
             + "{" //
             + "  ?a <http://www.w3.org/2000/01/rdf-schema#comment> ?b . "//
             + "  FILTER(fts:text(?b, \"bob\"))  " //
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
	 *             Tests QueryVariableNormalizerContext on the queries q1,q2
	 *             which are the same up to a relabeling of variables.
	 */
	@Test
	public void testEqThreeDiffVars() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q1, null);
		ParsedQuery pq2 = parser2.parseQuery(q2, null);
	
		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertEquals(1, normalize.size());

		for (TupleExpr s : normalize) {
			Assert.assertTrue(tupleEquals(s, pq1.getTupleExpr()));

		}

	}

	/**
	 * @throws Exception
	 *             Tests QueryVariableNormalizerContext on queries q1 and q14
	 *             which are the same up to the permutation of their variables.
	 */
	@Test
	public void testEqThreePermuteVars() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q1, null);
		ParsedQuery pq2 = parser2.parseQuery(q14, null);
	

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertEquals(1, normalize.size());

		for (TupleExpr s : normalize) {
			Assert.assertTrue(tupleEquals(s, pq1.getTupleExpr()));
		}

	}

	/**
	 * @throws Exception
	 *             Tests QueryVariableNormalizerContext on the queries q12 and
	 *             q13, which are the same up to a relabeling of the variables,
	 *             but have StatementPatterns whose constants are not
	 *             predicates.
	 */
	@Test
	public void testEqPredNotConst() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q12, null);
		ParsedQuery pq2 = parser2.parseQuery(q13, null);
		
		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertEquals(1, normalize.size());

		for (TupleExpr s : normalize) {
			// System.out.println(s);
			Assert.assertTrue(tupleEquals(s, pq1.getTupleExpr()));
		}

	}

	/**
	 * @throws Exception
	 *             Tests QueryVariableNormalizerContext on the large query q9
	 *             with with a smaller, potential index q10 to see if the
	 *             correct number of outputs are produced.
	 */
	@Test
	public void testEqLargeEx() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q9, null);
		ParsedQuery pq2 = parser2.parseQuery(q10, null);
		

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertEquals(4, normalize.size());

		for (TupleExpr s : normalize) {
			List<TupleExpr> testList = QueryVariableNormalizer.getNormalizedIndex(pq2.getTupleExpr(), s);
			Assert.assertEquals(1, testList.size());
			for (TupleExpr t : testList) {
				Assert.assertTrue(t.equals(pq2.getTupleExpr()));
			}
		}

		SPARQLParser parser3 = new SPARQLParser();
		ParsedQuery pq3 = parser3.parseQuery(q7, null);
		List<TupleExpr> normalize2 = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq3.getTupleExpr());

		Assert.assertEquals(12, normalize2.size());
		for (TupleExpr s : normalize2) {
			Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), s));
		}

	}

	/**
	 * @throws Exception
	 *             Tests QueryVariableNormalizerContext to see if it recognizes
	 *             that no substitution exists for two moderate, similar queries
	 *             q5 and q1 that are structurally different
	 */
	@Test
	public void testEqNEQ() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q1, null);
		ParsedQuery pq2 = parser2.parseQuery(q5, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 0);

		pq1 = parser1.parseQuery(q5, null);
		pq2 = parser2.parseQuery(q1, null);

		List<TupleExpr> normalize2 = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertEquals(1, normalize2.size());

		for (TupleExpr s : normalize2) {
			List<TupleExpr> testList = QueryVariableNormalizer.getNormalizedIndex(pq2.getTupleExpr(), s);
			Assert.assertEquals(1, testList.size());
			for (TupleExpr t : testList) {
				Assert.assertTrue(t.equals(pq2.getTupleExpr()));
			}
		}

	}

	/**
	 * @throws Exception
	 *             Tests QueryVariableNormalizerContext to see if it recognizes
	 *             that no substitution exists for two small, similar queries q3
	 *             and q4 that are structurally different
	 */
	@Test
	public void testNeq1() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q3, null);
		ParsedQuery pq2 = parser2.parseQuery(q4, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 0);

	}

	/**
	 * @throws Exception
	 *             Tests QueryVariableNormalizerContext to see if it recognizes
	 *             that no substitution exists for the variables of q8 given
	 *             that it has more variables than q1
	 */
	@Test
	public void testNeq2() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q1, null);
		ParsedQuery pq2 = parser2.parseQuery(q8, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 0);

	}

	/**
	 * @throws Exception
	 *             Tests QueryVariableNormalizerContext to see if it recognizes
	 *             that no substitution exists for the large queries q8 and q9
	 *             which contain the same number of variables and are similar.
	 */
	@Test
	public void testNeq() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q9, null);
		ParsedQuery pq2 = parser2.parseQuery(q8, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 0);

	}

	/**
	 * @throws Exception
	 *             Tests QueryVariableNormalizerContext on the large query q11
	 *             and q6, which have many similar nodes, to see if the correct
	 *             number of outputs are produced.
	 */
	@Test
	public void testLargeNeq() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q11, null);
		ParsedQuery pq2 = parser2.parseQuery(q6, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 33);
		for (TupleExpr s : normalize) {
			Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), s));
		}

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

		Assert.assertTrue(normalize.size() == 2);
		for (TupleExpr s : normalize) {
			Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(), s));
		}

		pq1 = parser1.parseQuery(q16, null);
		pq2 = parser2.parseQuery(q17, null);
		normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(), pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 0);

	}

	/**
	 * @throws Exception
	 *             Tests QueryVariableNormalizerContext with same query passed
	 *             in as query and index. Tests that only one index is produced
	 *             and that it equals original query.
	 */
	@Test
	public void testSameTuples() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q11, null);
		ParsedQuery pq2 = parser2.parseQuery(q11, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 1);
		Assert.assertTrue(normalize.get(0).equals(pq1.getTupleExpr()) && normalize.get(0).equals(pq2.getTupleExpr()));

	}

	

	/**
	 * @throws Exception
	 *             Tests QueryVariable normalizer on queries q9 and q18, where
	 *             q18 is obtained from q9 by reordering lines.
	 */
	@Test
	public void testOrderEq() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q9, null);
		ParsedQuery pq2 = parser2.parseQuery(q18, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 24);
		for (TupleExpr s : normalize) {
			Assert.assertTrue(isTupleSubset(s, pq1.getTupleExpr())&&isTupleSubset(pq1.getTupleExpr(),s));
		}

	}

	@Test
	public void testSimpleVarGraph() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q22, null);
		ParsedQuery pq2 = parser2.parseQuery(q23, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 1);
		for (TupleExpr s : normalize) {
			Assert.assertTrue(tupleEquals(s, pq1.getTupleExpr()));
		}

	}

	@Test
	public void testVarGraph() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q19, null);
		ParsedQuery pq2 = parser2.parseQuery(q22, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 1);
		for (TupleExpr s : normalize) {
			Assert.assertTrue(tupleEquals(s, pq1.getTupleExpr()));
		}

	}

	@Test
	public void tesVarConstantGraph() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q19, null);
		ParsedQuery pq2 = parser2.parseQuery(q20, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 0);

	}

	@Test
	public void testConstantGraph() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q20, null);
		ParsedQuery pq2 = parser2.parseQuery(q21, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 1);
		for (TupleExpr s : normalize) {
			Assert.assertTrue(tupleEquals(s, pq1.getTupleExpr()));
		}

	}

	
	@Test
	public void testMedVarGraph() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q24, null);
		ParsedQuery pq2 = parser2.parseQuery(q25, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 2);
		for (TupleExpr s : normalize) {
			Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(),s));
		}

	}

	
	@Test
	public void tesGraphVarInBody() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q19, null);
		ParsedQuery pq2 = parser2.parseQuery(q25, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 1);

	}
	
	
	@Test
	public void tesLargeVarGraph() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q26, null);
		ParsedQuery pq2 = parser2.parseQuery(q27, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 33);
		for (TupleExpr s : normalize) {
			Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(),s));
		}
		
		
	}
	
	
	@Test
	public void testFilters1() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q30, null);
		ParsedQuery pq2 = parser2.parseQuery(q31, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 1);
		for (TupleExpr s : normalize) {
			Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(),s));
		}
		
		
	}

	
	
	@Test
	public void testFilters2() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q32, null);
		ParsedQuery pq2 = parser2.parseQuery(q34, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 1);
		for (TupleExpr s : normalize) {
			Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(),s));
		}
		
		
	}

	
	
	@Test
	public void testFilters3() throws Exception {

		SPARQLParser parser1 = new SPARQLParser();
		SPARQLParser parser2 = new SPARQLParser();

		ParsedQuery pq1 = parser1.parseQuery(q33, null);
		ParsedQuery pq2 = parser2.parseQuery(q35, null);

		List<TupleExpr> normalize = QueryVariableNormalizer.getNormalizedIndex(pq1.getTupleExpr(),
				pq2.getTupleExpr());

		Assert.assertTrue(normalize.size() == 1);
		for (TupleExpr s : normalize) {
			Assert.assertTrue(isTupleSubset(pq1.getTupleExpr(),s));
		}
		
		
	}

	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
