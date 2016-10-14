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
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class GeneralizedExternalProcessorTest {

	private String q7 = ""//
			+ "SELECT ?s ?t ?u " //
			+ "{" //
			+ "  ?s a ?t ."//
			+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
			+ "  ?u <uri:talksTo> ?s . "//
			+ "}";//

	private String q12 = ""//
			+ "SELECT ?b ?p ?dog ?cat " //
			+ "{" //
			+ "  ?b a ?p ."//
			+ "  ?dog a ?cat. "//
			+ "}";//

	private String q15 = ""//
			+ "SELECT ?f ?m ?d ?e ?l ?c " //
			+ "{" //
			+ "  ?f a ?m ."//
			+ "  ?e a ?l ."//
			+ "  ?d <uri:talksTo> ?f . "//
			+ "  ?c <uri:talksTo> ?e . "//
			+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
			+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
			+ "}";//

	private String q16 = ""//
			+ "SELECT ?f ?m ?d ?e ?l ?c " //
			+ "{" //
			+ "  ?d <uri:talksTo> ?f . "//
			+ "  ?c <uri:talksTo> ?e . "//
			+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
			+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
			+ "}";//

	private String q17 = ""//
			+ "SELECT ?dog ?cat ?chicken " //
			+ "{" //
			+ "  ?chicken <uri:talksTo> ?dog . "//
			+ "  ?cat <http://www.w3.org/2000/01/rdf-schema#label> ?chicken ."//
			+ "}";//

	private String q18 = ""//
			+ "SELECT ?dog ?chicken " //
			+ "{" //
			+ "  ?chicken <uri:talksTo> ?dog . "//
			+ "}";//

	private String q19 = ""//
			+ "SELECT ?cat ?chicken " //
			+ "{" //
			+ "  ?cat <http://www.w3.org/2000/01/rdf-schema#label> ?chicken ."//
			+ "}";//

	@Test
	public void testTwoIndexLargeQuery() throws Exception {

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q15, null);
		ParsedQuery pq2 = parser.parseQuery(q7, null);
		ParsedQuery pq3 = parser.parseQuery(q12, null);

		System.out.println("Query is " + pq1.getTupleExpr());

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				new Projection(pq2.getTupleExpr()));
		SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				new Projection(pq3.getTupleExpr()));

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup2);
		list.add(extTup1);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);
		List<ExternalTupleSet> indexSet = iep.getNormalizedIndices();
		Assert.assertEquals(4, indexSet.size());

		Set<TupleExpr> processedTups = Sets.newHashSet(iep.getIndexedTuples());
		Assert.assertEquals(5, processedTups.size());

	}

	@Test
	public void testThreeIndexQuery() throws Exception {

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q16, null);
		ParsedQuery pq2 = parser.parseQuery(q17, null);
		ParsedQuery pq3 = parser.parseQuery(q18, null);
		ParsedQuery pq4 = parser.parseQuery(q19, null);

		System.out.println("Query is " + pq1.getTupleExpr());

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				new Projection(pq2.getTupleExpr()));
		SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				new Projection(pq3.getTupleExpr()));
		SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				new Projection(pq4.getTupleExpr()));

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup2);
		list.add(extTup3);
		list.add(extTup1);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);
		List<ExternalTupleSet> indexSet = iep.getNormalizedIndices();
		Assert.assertEquals(6, indexSet.size());

		Set<TupleExpr> processedTups = Sets.newHashSet(iep.getIndexedTuples());

		Assert.assertEquals(17, processedTups.size());

		TupleExecutionPlanGenerator tep = new TupleExecutionPlanGenerator();
		List<TupleExpr> plans = Lists.newArrayList(tep.getPlans(processedTups
				.iterator()));

		System.out.println("Size is " + plans.size());

		System.out.println("Possible indexed tuple plans are :");
		for (TupleExpr te : plans) {
			System.out.println(te);
		}

	}

}
