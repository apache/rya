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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.SimpleExternalTupleSet;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Sets;

public class VarConstantIndexListPrunerTest {

	private String q7 = ""//
			+ "SELECT ?s ?t ?u " //
			+ "{" //
			+ "  ?s a ?t ."//
			+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
			+ "  ?u <uri:talksTo> ?s . "//
			+ "}";//

	private String q8 = ""//
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

	private String q11 = ""//
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

	private String q12 = ""//
			+ "SELECT ?b ?p ?dog ?cat " //
			+ "{" //
			+ "  ?b a ?p ."//
			+ "  ?dog a ?cat. "//
			+ "}";//

	private String q13 = ""//
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

	private String q14 = ""//
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
			+ "  ?d <uri:talksTo> ?a . "//
			+ "}";//

	private String q15 = ""//
			+ "SELECT ?s ?t ?u " //
			+ "{" //
			+ "  Filter(?s > 1)."//
			+ "  ?s a ?t ."//
			+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
			+ "  ?u <uri:talksTo> ?s . "//
			+ "}";//

	private String q16 = ""//
			+ "SELECT ?s ?t ?u " //
			+ "{" //
			+ "  Filter(?s > 2)."//
			+ "  ?s a ?t ."//
			+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
			+ "  ?u <uri:talksTo> ?s . "//
			+ "}";//

	private String q17 = ""//
			+ "SELECT ?s ?t ?u " //
			+ "{" //
			+ "  Filter(?t > 1)."//
			+ "  ?s a ?t ."//
			+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
			+ "  ?u <uri:talksTo> ?s . "//
			+ "}";//

	@Test
	public void testTwoIndexLargeQuery() throws Exception {

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q11, null);
		ParsedQuery pq2 = parser.parseQuery(q7, null);
		ParsedQuery pq3 = parser.parseQuery(q12, null);
		ParsedQuery pq4 = parser.parseQuery(q13, null);
		ParsedQuery pq5 = parser.parseQuery(q8, null);
		ParsedQuery pq6 = parser.parseQuery(q14, null);

		System.out.println("Query is " + pq1.getTupleExpr());

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());
		SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				(Projection) pq4.getTupleExpr());
		SimpleExternalTupleSet extTup4 = new SimpleExternalTupleSet(
				(Projection) pq5.getTupleExpr());
		SimpleExternalTupleSet extTup5 = new SimpleExternalTupleSet(
				(Projection) pq6.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup1);
		list.add(extTup2);
		list.add(extTup3);
		list.add(extTup4);
		list.add(extTup5);

		VarConstantIndexListPruner vci = new VarConstantIndexListPruner(
				pq1.getTupleExpr());
		List<ExternalTupleSet> processedIndexSet = vci.getRelevantIndices(list);

		System.out.println("Relevant indexes are: ");
		for (ExternalTupleSet e : processedIndexSet) {
			System.out.println(e);
		}

		Set<ExternalTupleSet> indexSet = Sets.newHashSet();
		indexSet.add(extTup1);
		indexSet.add(extTup2);
		indexSet.add(extTup4);

		Assert.assertTrue(Sets.intersection(indexSet, Sets.<ExternalTupleSet> newHashSet(processedIndexSet))
				.equals(Sets.<ExternalTupleSet> newHashSet(processedIndexSet)));

	}

	@Test
	public void testTwoIndexFilter1() throws Exception {

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q15, null);
		ParsedQuery pq2 = parser.parseQuery(q16, null);
		ParsedQuery pq3 = parser.parseQuery(q17, null);

		System.out.println("Query is " + pq1.getTupleExpr());

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup1);
		list.add(extTup2);

		VarConstantIndexListPruner vci = new VarConstantIndexListPruner(
				pq1.getTupleExpr());
		List<ExternalTupleSet> processedIndexSet = vci.getRelevantIndices(list);

		System.out.println("Relevant indexes are: ");
		for (ExternalTupleSet e : processedIndexSet) {
			System.out.println(e);
		}

		Set<ExternalTupleSet> indexSet = Sets.newHashSet();
		indexSet.add(extTup2);

		Assert.assertTrue(Sets.intersection(indexSet,
				Sets.<ExternalTupleSet> newHashSet(processedIndexSet)).equals(
						Sets.<ExternalTupleSet> newHashSet(processedIndexSet)));

	}

	@Test
	public void testTwoIndexFilter2() throws Exception {

		String q18 = ""//
				+ "SELECT ?s ?t ?u " //
				+ "{" //
				+ "  Filter(?s > 1 && ?t > 8)." //
				+ "  ?s a ?t ."//
				+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
				+ "  ?u <uri:talksTo> ?s . "//
				+ "}";//

		String q19 = ""//
				+ "SELECT ?s ?t ?u " //
				+ "{" //
				+ "  Filter(?s > 1)." //
				+ "  Filter(?t > 8)." //
				+ "  ?s a ?t ."//
				+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
				+ "  ?u <uri:talksTo> ?s . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q18, null);
		ParsedQuery pq2 = parser.parseQuery(q19, null);

		System.out.println("Query is " + pq1.getTupleExpr());

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();
		list.add(extTup1);

		VarConstantIndexListPruner vci = new VarConstantIndexListPruner(
				pq1.getTupleExpr());
		List<ExternalTupleSet> processedIndexSet = vci.getRelevantIndices(list);

		Assert.assertTrue(processedIndexSet.isEmpty());

	}

}
