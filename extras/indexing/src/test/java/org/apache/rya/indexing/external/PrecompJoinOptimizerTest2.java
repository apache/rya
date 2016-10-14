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

import org.apache.rya.indexing.external.PcjIntegrationTestingUtil.BindingSetAssignmentCollector;
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

public class PrecompJoinOptimizerTest2 {

	private final String queryString = ""//
			+ "SELECT ?e ?c ?l ?o " //
			+ "{" //
			+ "  ?e a ?c . "//
			+ "  ?c a ?l . "//
			+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
			+ "  ?e <uri:talksTo> ?o  "//
			+ "}";//

	private final String indexSparqlString = ""//
			+ "SELECT ?x ?y ?z " //
			+ "{" //
			+ "  ?x <http://www.w3.org/2000/01/rdf-schema#label> ?z. "//
			+ "  ?x a ?y . "//
			+ "  ?y a ?z  "//
			+ "}";//

	private final String q1 = ""//
			+ "SELECT ?e ?l ?c " //
			+ "{" //
			+ "  ?e a ?c . "//
			+ "  ?c <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
			+ "  ?l <uri:talksTo> ?e . "//
			+ "}";//

	private final String q2 = ""//
			+ "SELECT ?a ?t ?v  " //
			+ "{" //
			+ "  ?a a ?t . "//
			+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?v . "//
			+ "  ?v <uri:talksTo> ?a . "//
			+ "}";//

	private final String q5 = ""//
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

	private final String q7 = ""//
			+ "SELECT ?s ?t ?u " //
			+ "{" //
			+ "  ?s a ?t ."//
			+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
			+ "  ?u <uri:talksTo> ?s . "//
			+ "}";//

	private final String q8 = ""//
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

	private final String q11 = ""//
			+ "SELECT ?f ?m ?d ?e ?l ?c ?n ?o ?p ?a ?h ?r ?x ?y ?w ?t ?duck ?chicken ?pig ?rabbit " //
			+ "{" //
			+ "  ?w a ?t ."//
			+ "  ?x a ?y ."//
			+ "  ?duck a ?chicken ."//
			+ "  ?pig a ?rabbit ."//
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

	private final String q12 = ""//
			+ "SELECT ?b ?p ?dog ?cat " //
			+ "{" //
			+ "  ?b a ?p ."//
			+ "  ?dog a ?cat. "//
			+ "}";//

	private final String q13 = ""//
			+ "SELECT ?f ?m ?d ?e ?l ?c ?n ?o ?p ?a ?h ?r ?x ?y ?w ?t ?duck ?chicken ?pig ?rabbit ?dick ?jane ?betty " //
			+ "{" //
			+ "  ?w a ?t ."//
			+ "  ?x a ?y ."//
			+ "  ?duck a ?chicken ."//
			+ "  ?pig a ?rabbit ."//
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
			+ "  ?dick <uri:talksTo> ?jane . "//
			+ "  ?jane <uri:talksTo> ?betty . "//
			+ "}";//

	private final String q14 = ""//
			+ "SELECT ?harry ?susan ?mary " //
			+ "{" //
			+ "  ?harry <uri:talksTo> ?susan . "//
			+ "  ?susan <uri:talksTo> ?mary . "//
			+ "}";//

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
			+ "		}"//
			+ "}";//

	String q16 = ""//
			+ "SELECT ?g ?h ?i " //
			+ "{" //
			+ " GRAPH ?y { " //
			+ "  ?g a ?h ."//
			+ "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?i ."//
			+ "		}"//
			+ "}";//

	String q17 = ""//
			+ "SELECT ?j ?k ?l ?m ?n ?o " //
			+ "{" //
			+ " GRAPH ?z { " //
			+ "  ?j <uri:talksTo> ?k . "//
			+ "  FILTER ( ?k < ?l && (?m > ?n || ?o = ?j) ). " //
			+ "		}"//
			+ "}";//

	String q18 = ""//
			+ "SELECT ?r ?s ?t ?u " //
			+ "{" //
			+ " GRAPH ?q { " //
			+ "  FILTER(bound(?r) && sameTerm(?s,?t)&&bound(?u)). " //
			+ "  ?t a ?u ."//
			+ "		}"//
			+ "}";//

	String q19 = ""//
			+ "SELECT ?a ?b ?c ?d ?e ?f ?q ?g ?h " //
			+ "{" //
			+ " GRAPH ?x { " //
			+ "  ?a a ?b ."//
			+ "  ?b <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
			+ "  ?d <uri:talksTo> ?e . "//
			+ "  FILTER ( ?e < ?f && (?a > ?b || ?c = ?d) ). " //
			+ "  FILTER(bound(?f) && sameTerm(?a,?b)&&bound(?q)). " //
			+ "  FILTER(?g IN (1,2,3) && ?h NOT IN(5,6,7)). " //
			+ "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?g. "//
			+ "  ?b a ?q ."//
			+ "		}"//
			+ "}";//

	String q20 = ""//
			+ "SELECT ?m ?n " //
			+ "{" //
			+ " GRAPH ?q { " //
			+ "  FILTER(?m IN (1,2,3) && ?n NOT IN(5,6,7)). " //
			+ "  ?n <http://www.w3.org/2000/01/rdf-schema#label> ?m. "//
			+ "		}"//
			+ "}";//

	String q21 = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
			+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
			+ "SELECT ?feature ?point ?wkt " //
			+ "{" //
			+ "  ?feature a geo:Feature . "//
			+ "  ?feature geo:hasGeometry ?point . "//
			+ "  ?point a geo:Point . "//
			+ "  ?point geo:asWKT ?wkt . "//
			+ "  FILTER(geof:sfWithin(?wkt, \"Polygon\")) " //
			+ "}";//

	String q22 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
			+ "SELECT ?person ?commentmatch ?labelmatch" //
			+ "{" //
			+ "  ?person a <http://example.org/ontology/Person> . "//
			+ "  ?person <http://www.w3.org/2000/01/rdf-schema#label> ?labelmatch . "//
			+ "  ?person <http://www.w3.org/2000/01/rdf-schema#comment> ?commentmatch . "//
			+ "  FILTER(fts:text(?labelmatch, \"bob\")) . " //
			+ "  FILTER(fts:text(?commentmatch, \"bob\"))  " //
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
			+ "SELECT ?f ?g " //
			+ "{" //
			+ "  ?f <http://www.w3.org/2000/01/rdf-schema#comment> ?g . "//
			+ "  FILTER(fts:text(?g, \"bob\"))  " //
			+ "}";//

	String q25 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
			+ "SELECT ?person ?commentmatch ?labelmatch ?point" //
			+ "{" //
			+ "  ?person a ?point. " //
			+ "  ?person a <http://example.org/ontology/Person> . "//
			+ "  ?person <http://www.w3.org/2000/01/rdf-schema#label> ?labelmatch . "//
			+ "  ?person <http://www.w3.org/2000/01/rdf-schema#comment> ?commentmatch . "//
			+ "  FILTER((?person > ?point) || (?person = ?labelmatch)). "
			+ "  FILTER(fts:text(?labelmatch, \"bob\")) . " //
			+ "  FILTER(fts:text(?commentmatch, \"bob\"))  " //
			+ "}";//

	String q26 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
			+ "SELECT ?a ?b ?c  " //
			+ "{" //
			+ "  ?a a ?c. " //
			+ "  ?a a <http://example.org/ontology/Person> . "//
			+ "  ?a <http://www.w3.org/2000/01/rdf-schema#label> ?b . "//
			+ "  FILTER((?a > ?c) || (?a = ?b)). "
			+ "  FILTER(fts:text(?b, \"bob\")) . " //
			+ "}";//

	String q27 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
			+ "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
			+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
			+ "SELECT ?person ?commentmatch ?labelmatch ?other ?feature ?point ?wkt ?g ?h" //
			+ "{" //
			+ "  ?person a <http://example.org/ontology/Person> . "//
			+ "  ?person <http://www.w3.org/2000/01/rdf-schema#label> ?labelmatch . "//
			+ "  ?person <http://www.w3.org/2000/01/rdf-schema#comment> ?commentmatch . "//
			+ "  FILTER((?person > ?other) || (?person = ?labelmatch)). "
			+ "  ?person a ?other. "//
			+ "  FILTER(fts:text(?labelmatch, \"bob\")) . " //
			+ "  FILTER(fts:text(?commentmatch, \"bob\"))  " //
			+ " ?feature a geo:Feature . "//
			+ "  ?point a geo:Point . "//
			+ "  ?point geo:asWKT ?wkt . "//
			+ "  FILTER(geof:sfWithin(?wkt, \"Polygon\")) " //
			+ "  FILTER(?g IN (1,2,3) && ?h NOT IN(5,6,7)). " //
			+ "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?g. "//
			+ "}";//

	String q28 = ""//
			+ "SELECT ?m ?n " //
			+ "{" //
			+ "  FILTER(?m IN (1,2,3) && ?n NOT IN(5,6,7)). " //
			+ "  ?n <http://www.w3.org/2000/01/rdf-schema#label> ?m. "//
			+ "}";//

	String q29 = ""//
			+ "SELECT ?m ?n ?o" //
			+ "{" //
			+ "  FILTER(?m IN (1,2,3) && ?n NOT IN(5,6,7)). " //
			+ "  ?n <http://www.w3.org/2000/01/rdf-schema#label> ?m. "//
			+ "  ?m a ?o." //
			+ "  FILTER(ISNUMERIC(?o))." + "}";//

	String q30 = ""//
			+ "SELECT ?pig ?dog ?owl" //
			+ "{" //
			+ "  FILTER(?pig IN (1,2,3) && ?dog NOT IN(5,6,7)). " //
			+ "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig. "//
			+ "  ?pig a ?owl. " //
			+ "  FILTER(ISNUMERIC(?owl))." + "}";//

	String q31 = ""//
			+ "SELECT ?q ?r ?s " //
			+ "{" //
			+ "  {?q a ?r} UNION {?r a ?s} ."//
			+ "  ?r a ?s ."//
			+ "}";//

	String q33 = ""//
			+ "SELECT ?q ?r ?s ?t " //
			+ "{" //
			+ "  OPTIONAL {?q a ?r} ."//
			+ "  ?s a ?t ."//
			+ "}";//

	String q34 = ""//
			+ "SELECT ?q ?r  " //
			+ "{" //
			+ "  FILTER(?q > ?r) ."//
			+ "  ?q a ?r ."//
			+ "}";//

	String q35 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
			+ "SELECT ?s ?t ?u ?v ?w ?x ?y ?z " //
			+ "{" //
			+ "  FILTER(?s > ?t)."//
			+ "  ?s a ?t ."//
			+ "  FILTER(?u > ?v)."//
			+ "  ?u a ?v ."//
			+ "  ?w <http://www.w3.org/2000/01/rdf-schema#label> ?x ."//
			+ "  FILTER(fts:text(?x, \"bob\")) . " //
			+ "  ?y <http://www.w3.org/2000/01/rdf-schema#label> ?z ."//
			+ "  FILTER(fts:text(?z, \"bob\")) . " //
			+ "}";//

	String q36 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
			+ "SELECT ?dog ?cat  " //
			+ "{" //
			+ "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?cat ."//
			+ "  FILTER(fts:text(?cat, \"bob\")) . " //
			+ "}";//

	String q37 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
			+ "SELECT ?s ?t " //
			+ "{" //
			+ "  FILTER(?s > ?t)."//
			+ "  ?s a ?t ."//
			+ "  FILTER(?s > ?t)."//
			+ "  ?s a ?t ."//
			+ "  FILTER(?s > ?t)."//
			+ "  ?s a ?t ."//
			+ "}";//

	String q38 = "PREFIX fts: <http://rdf.useekm.com/fts#>  "//
			+ "SELECT ?s ?t " //
			+ "{" //
			+ "  FILTER(?s > ?t)."//
			+ "  ?s a ?t ."//
			+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?s ."//
			+ "  FILTER(?s > ?t)."//
			+ "}";//

	String q39 = "PREFIX fts: <http://rdf.useekm.com/fts#> "//
			+ "SELECT ?s ?t " //
			+ "{" //
			+ " VALUES(?s) { (<ub:poodle>)(<ub:pitbull>)} ." //
			+ " ?t <ub:peesOn> <ub:rug> ." //
			+ " ?t <http://www.w3.org/2000/01/rdf-schema#label> ?s ."//
			+ "}";//

	String q40 = "PREFIX fts: <http://rdf.useekm.com/fts#> "//
			+ "SELECT ?u ?v " //
			+ "{" //
			+ " ?v <ub:peesOn> <ub:rug> ." //
			+ " ?v <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
			+ "}";//

	String q41 = "PREFIX fts: <http://rdf.useekm.com/fts#> "//
			+ "SELECT ?s ?t ?w ?x" //
			+ "{" //
			+ " FILTER(?s > ?t)."//
			+ " VALUES(?s) { (<ub:poodle>)(<ub:pitbull>)} ." //
			+ " VALUES(?w) { (<ub:persian>) (<ub:siamese>) } ." //
			+ " ?t <ub:peesOn> <ub:rug> ." //
			+ " ?t <http://www.w3.org/2000/01/rdf-schema#label> ?s ."//
			+ " ?w <ub:peesOn> <ub:rug> ." //
			+ " ?w <http://www.w3.org/2000/01/rdf-schema#label> ?x ."//
			+ "}";//

	String q42 = "PREFIX fts: <http://rdf.useekm.com/fts#> "//
			+ "SELECT ?u ?v " //
			+ "{" //
			+ " FILTER(?u > ?v)."//
			+ " ?v <ub:peesOn> <ub:rug> ." //
			+ " ?v <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
			+ "}";//

	String q43 = "PREFIX fts: <http://rdf.useekm.com/fts#> "//
			+ "SELECT ?a ?b " //
			+ "{" //
			+ " ?b <ub:peesOn> <ub:rug> ." //
			+ " ?b <http://www.w3.org/2000/01/rdf-schema#label> ?a ."//
			+ "}";//

	@Test
	public void testVarRelableIndexSmaller() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(queryString, null);
		final ParsedQuery pq2 = parser2.parseQuery(indexSparqlString, null);

		System.out.println("Query is " + pq1.getTupleExpr());
		System.out.println("Index is " + pq2.getTupleExpr());

		final SimpleExternalTupleSet extTup = new SimpleExternalTupleSet(
				new Projection(pq2.getTupleExpr()));

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup);

		final TupleExpr tup = pq1.getTupleExpr().clone();
		final PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		final Set<StatementPattern> qSet = Sets
				.newHashSet(StatementPatternCollector.process(pq1
						.getTupleExpr()));
		final Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		final Set<StatementPattern> set = Sets.newHashSet();
		for (final QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(qSet.containsAll(set) && set.size() != 0);
	}

	@Test
	public void testVarRelableIndexSameSize() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(q1, null);
		final ParsedQuery pq2 = parser2.parseQuery(q2, null);

		final SimpleExternalTupleSet extTup = new SimpleExternalTupleSet(
				new Projection(pq2.getTupleExpr()));

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup);

		final TupleExpr tup = pq1.getTupleExpr().clone();
		final PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		final Set<StatementPattern> qSet = Sets
				.newHashSet(StatementPatternCollector.process(pq1
						.getTupleExpr()));
		final Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		final Set<StatementPattern> set = Sets.newHashSet();
		for (final QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(set.equals(qSet));

	}

	@Test
	public void testTwoIndexLargeQuery() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();
		final SPARQLParser parser3 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(q11, null);
		final ParsedQuery pq2 = parser2.parseQuery(q7, null);
		final ParsedQuery pq3 = parser3.parseQuery(q12, null);

		System.out.println("Query is " + pq1.getTupleExpr());
		System.out.println("Indexes are " + pq2.getTupleExpr() + " and "
				+ pq3.getTupleExpr());

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup1);
		list.add(extTup2);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(set.equals(qSet));

	}

	@Test
	public void testThreeIndexLargeQuery() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();
		final SPARQLParser parser3 = new SPARQLParser();
		final SPARQLParser parser4 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(q13, null);
		final ParsedQuery pq2 = parser2.parseQuery(q5, null);
		final ParsedQuery pq3 = parser3.parseQuery(q12, null);
		final ParsedQuery pq4 = parser4.parseQuery(q14, null);

		System.out.println("Query is " + pq1.getTupleExpr());
		System.out.println("Indexes are " + pq2.getTupleExpr() + " , "
				+ pq3.getTupleExpr() + " , " + pq4.getTupleExpr());

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());
		final SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				(Projection) pq4.getTupleExpr());

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup1);
		list.add(extTup2);
		list.add(extTup3);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(set.equals(qSet));

	}

	@Test
	public void testSingleIndexLargeQuery() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(q8, null);
		final ParsedQuery pq2 = parser2.parseQuery(q7, null);

		final SimpleExternalTupleSet extTup = new SimpleExternalTupleSet(
				new Projection(pq2.getTupleExpr()));

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(set.equals(qSet));

	}

	@Test
	public void testContextFilter1() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();
		final SPARQLParser parser3 = new SPARQLParser();
		final SPARQLParser parser4 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(q15, null);
		final ParsedQuery pq2 = parser2.parseQuery(q16, null);
		final ParsedQuery pq3 = parser3.parseQuery(q17, null);
		final ParsedQuery pq4 = parser4.parseQuery(q18, null);

		System.out.println("Query is " + pq1.getTupleExpr());
		System.out.println("Indexes are " + pq2.getTupleExpr() + " , "
				+ pq3.getTupleExpr() + " , " + pq4.getTupleExpr());

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());
		final SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				(Projection) pq4.getTupleExpr());

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup1);
		list.add(extTup2);
		list.add(extTup3);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(qSet.containsAll(set) && eTupSet.size() == 1);
	}

	@Test
	public void testGeoFilter() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();

		String query1 = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "PREFIX geof: <http://www.opengis.net/def/function/geosparql/>  "//
				+ "SELECT ?a ?b ?c " //
				+ "{" //
				+ "  ?a a geo:Feature . "//
				+ "  ?b a geo:Point . "//
				+ "  ?b geo:asWKT ?c . "//
				+ "  FILTER(geof:sfWithin(?b, \"Polygon\")) " //
				+ "}";//

		String query2 = "PREFIX geo: <http://www.opengis.net/ont/geosparql#>  "//
				+ "SELECT ?f ?g " //
				+ "{" //
				+ "  ?f a geo:Feature . "//
				+ "  ?g a geo:Point . "//
				+ "}";//

		final ParsedQuery pq1 = parser1.parseQuery(query1, null);
		final ParsedQuery pq2 = parser2.parseQuery(query2, null);

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup1);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(qSet.containsAll(set) && eTupSet.size() == 1);
	}

	@Test
	public void testContextFilter2() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();

		String query1 = ""//
				+ "SELECT ?k ?l ?m ?n " //
				+ "{" //
				+ " GRAPH ?z { " //
				+ " ?l <uri:talksTo> ?n . "//
				+ " ?l a ?n."//
				+ " ?k a ?m."//
				+ "  FILTER ((?k < ?l) && (?m < ?n)). " //
				+ "		}"//
				+ "}";//

		String query2 = ""//
				+ "SELECT ?s ?t " //
				+ "{" //
				+ " GRAPH ?r { " //
				+ " ?s <uri:talksTo> ?t . "//
				+ " ?s a ?t."//
				+ "	}"//
				+ "}";//

		final ParsedQuery pq1 = parser1.parseQuery(query1, null);
		final ParsedQuery pq2 = parser2.parseQuery(query2, null);

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup1);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(qSet.containsAll(set) && eTupSet.size() == 1);
	}

	@Test
	public void testGeoIndexFunction() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(q21, null);
		final ParsedQuery pq2 = parser2.parseQuery(q23, null);

		System.out.println("Query is " + pq1.getTupleExpr());
		System.out.println("Index is " + pq2.getTupleExpr());

		final SimpleExternalTupleSet extTup = new SimpleExternalTupleSet(
				new Projection(pq2.getTupleExpr()));

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(qSet.containsAll(set) && set.size() != 0);

	}

	@Test
	public void testFreeTextIndexFunction() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(q22, null);
		final ParsedQuery pq2 = parser2.parseQuery(q24, null);

		System.out.println("Query is " + pq1.getTupleExpr());
		System.out.println("Index is " + pq2.getTupleExpr());

		final SimpleExternalTupleSet extTup = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(qSet.containsAll(set) && set.size() != 0);

	}

	@Test
	public void testThreeIndexGeoFreeCompareFilterMix() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();
		final SPARQLParser parser3 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(q25, null);
		final ParsedQuery pq2 = parser2.parseQuery(q24, null);
		final ParsedQuery pq3 = parser3.parseQuery(q26, null);

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				new Projection(pq2.getTupleExpr()));
		final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				new Projection(pq3.getTupleExpr()));

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup1);
		list.add(extTup2);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(set.equals(qSet) && eTupSet.size() == 2);

	}

	@Test
	public void testFourIndexGeoFreeCompareFilterMix() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();
		final SPARQLParser parser3 = new SPARQLParser();
		final SPARQLParser parser4 = new SPARQLParser();
		final SPARQLParser parser5 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(q27, null);
		final ParsedQuery pq2 = parser2.parseQuery(q23, null);
		final ParsedQuery pq3 = parser3.parseQuery(q26, null);
		final ParsedQuery pq4 = parser4.parseQuery(q24, null);
		final ParsedQuery pq5 = parser5.parseQuery(q28, null);

		System.out.println("Query is " + pq1.getTupleExpr());
		System.out.println("Indexes are " + pq2.getTupleExpr() + " , "
				+ pq3.getTupleExpr() + " , " + pq4.getTupleExpr() + " and "
				+ pq5.getTupleExpr());

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				new Projection(pq2.getTupleExpr()));
		final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				new Projection(pq3.getTupleExpr()));
		final SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				new Projection(pq4.getTupleExpr()));
		final SimpleExternalTupleSet extTup4 = new SimpleExternalTupleSet(
				new Projection(pq5.getTupleExpr()));

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup4);
		list.add(extTup1);
		list.add(extTup2);
		list.add(extTup3);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(set.equals(qSet));

	}

	@Test
	public void testThreeIndexGeoFreeCompareFilterMix2() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();
		final SPARQLParser parser3 = new SPARQLParser();
		final SPARQLParser parser4 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(q27, null);
		final ParsedQuery pq2 = parser2.parseQuery(q23, null);
		final ParsedQuery pq3 = parser3.parseQuery(q26, null);
		final ParsedQuery pq4 = parser4.parseQuery(q28, null);

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				new Projection(pq2.getTupleExpr()));
		final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				new Projection(pq3.getTupleExpr()));
		final SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				new Projection(pq4.getTupleExpr()));

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup1);
		list.add(extTup3);
		list.add(extTup2);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(qSet.containsAll(set));

	}

	@Test
	public void testISNUMERIC() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(q29, null);
		final ParsedQuery pq2 = parser2.parseQuery(q30, null);

		final SimpleExternalTupleSet extTup = new SimpleExternalTupleSet(
				new Projection(pq2.getTupleExpr()));

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertTrue(set.equals(qSet) && eTupSet.size() == 1);

	}

	@Test
	public void testTwoRepeatedIndex() throws Exception {

		final SPARQLParser parser1 = new SPARQLParser();
		final SPARQLParser parser2 = new SPARQLParser();
		final SPARQLParser parser3 = new SPARQLParser();

		final ParsedQuery pq1 = parser1.parseQuery(q35, null);
		final ParsedQuery pq2 = parser2.parseQuery(q34, null);
		final ParsedQuery pq3 = parser3.parseQuery(q36, null);

		System.out.println("Query is " + pq1.getTupleExpr());
		System.out.println("Indexes are " + pq2.getTupleExpr() + " and "
				+ pq3.getTupleExpr());

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				new Projection(pq2.getTupleExpr()));
		final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				new Projection(pq3.getTupleExpr()));

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup1);
		list.add(extTup2);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertEquals(4, eTupSet.size());
		Assert.assertEquals(qSet, set);

	}

	@Test
	public void testBindingSetAssignment2() throws Exception {

		final SPARQLParser parser = new SPARQLParser();

		final ParsedQuery pq1 = parser.parseQuery(q41, null);
		final ParsedQuery pq2 = parser.parseQuery(q42, null);
		final ParsedQuery pq3 = parser.parseQuery(q43, null);

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				new Projection(pq2.getTupleExpr()));
		final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				new Projection(pq3.getTupleExpr()));

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup1);
		list.add(extTup2);

		TupleExpr tup = pq1.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(list, false);
		pcj.optimize(tup, null, null);

		Set<StatementPattern> qSet = Sets.newHashSet(StatementPatternCollector
				.process(pq1.getTupleExpr()));
		Set<QueryModelNode> eTupSet = PcjIntegrationTestingUtil
				.getTupleSets(tup);

		Set<StatementPattern> set = Sets.newHashSet();
		for (QueryModelNode s : eTupSet) {
			set.addAll(StatementPatternCollector.process(((ExternalTupleSet) s)
					.getTupleExpr()));
		}

		Assert.assertEquals(2, eTupSet.size());
		Assert.assertEquals(qSet, set);

		BindingSetAssignmentCollector bsac1 = new BindingSetAssignmentCollector();
		BindingSetAssignmentCollector bsac2 = new BindingSetAssignmentCollector();
		pq1.getTupleExpr().visit(bsac1);
		tup.visit(bsac2);

		Assert.assertEquals(bsac1.getBindingSetAssignments(),
				bsac2.getBindingSetAssignments());

	}

}
