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
import java.util.Iterator;
import java.util.List;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.SimpleExternalTupleSet;
import org.apache.rya.indexing.pcj.matching.PCJOptimizer;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.beust.jcommander.internal.Lists;

public class ThreshholdPlanSelectorTest {

	private String q7 = ""//
			+ "SELECT ?s ?t ?u " //
			+ "{" //
			+ "  ?s a ?t ."//
			+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
			+ "  ?u <uri:talksTo> ?s . "//
			+ "}";//

	private String q8 = ""//
			+ "SELECT ?e ?l ?c " //
			+ "{" //
			+ "  ?e a ?l ."//
			+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
			+ "  ?c <uri:talksTo> ?e . "//
			+ "}";//

	private String q9 = ""//
			+ "SELECT ?f ?m ?d " //
			+ "{" //
			+ "  ?f a ?m ."//
			+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
			+ "  ?d <uri:talksTo> ?f . "//
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

	@Test
	public void testSingleIndex() throws Exception {

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q15, null);
		ParsedQuery pq2 = parser.parseQuery(q7, null);
		ParsedQuery pq3 = parser.parseQuery(q8, null);
		ParsedQuery pq4 = parser.parseQuery(q9, null);

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());
		SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				(Projection) pq4.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup1);

		List<QueryModelNode> optTupNodes = Lists.newArrayList();
		optTupNodes.add(extTup2);
		optTupNodes.add(extTup3);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);

		Iterator<TupleExpr> plans = new TupleExecutionPlanGenerator()
				.getPlans(iep.getIndexedTuples());

		IndexPlanValidator ipv = new IndexPlanValidator(false);

		Iterator<TupleExpr> validPlans = ipv.getValidTuples(plans);

		ThreshholdPlanSelector tps = new ThreshholdPlanSelector(
				pq1.getTupleExpr());

		TupleExpr optimalTup = tps.getThreshholdQueryPlan(validPlans, .1, 1, 0,
				0);

		NodeCollector nc = new NodeCollector();
		optimalTup.visit(nc);

		List<QueryModelNode> qNodes = nc.getNodes();

		Assert.assertEquals(qNodes.size(), optTupNodes.size());
		for (QueryModelNode node : qNodes) {
			Assert.assertTrue(optTupNodes.contains(node));
		}

	}

	@Test
	public void testSingleIndex2() throws Exception {

		String q1 = ""//
				+ "SELECT ?f ?m ?d ?e ?l ?c " //
				+ "{" //
				+ "  ?f a ?m ."//
				+ "  ?c a ?l ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "  ?e <uri:talksTo> ?c . "//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?e ."//
				+ "  ?m <uri:talksTo> ?e . "//
				+ "}";//

		String q2 = ""//
				+ "SELECT ?u ?s ?t " //
				+ "{" //
				+ "  ?s a ?t ."//
				+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
				+ "  ?u <uri:talksTo> ?s . "//
				+ "}";//

		String q3 = ""//
				+ "SELECT ?e ?c ?l " //
				+ "{" //
				+ "  ?c a ?l ."//
				+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?e ."//
				+ "  ?e <uri:talksTo> ?c . "//
				+ "}";//

		String q4 = ""//
				+ "SELECT ?d ?f ?m " //
				+ "{" //
				+ "  ?f a ?m ."//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q1, null);
		ParsedQuery pq2 = parser.parseQuery(q2, null);
		ParsedQuery pq3 = parser.parseQuery(q3, null);
		ParsedQuery pq4 = parser.parseQuery(q4, null);

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());
		SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				(Projection) pq4.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup1);

		List<StatementPattern> spList = StatementPatternCollector.process(pq1
				.getTupleExpr());
		List<QueryModelNode> optTupNodes = Lists.newArrayList();
		optTupNodes.add(extTup3);
		optTupNodes.add(spList.get(6));
		optTupNodes.add(extTup2);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);

		Iterator<TupleExpr> plans = new TupleExecutionPlanGenerator()
				.getPlans(iep.getIndexedTuples());

		IndexPlanValidator ipv = new IndexPlanValidator(true);
		Iterator<TupleExpr> validPlans = ipv.getValidTuples(plans);

		ThreshholdPlanSelector tps = new ThreshholdPlanSelector(
				pq1.getTupleExpr());

		TupleExpr optimalTup = tps.getThreshholdQueryPlan(validPlans, .4, .7,
				.1, .2);

		NodeCollector nc = new NodeCollector();
		optimalTup.visit(nc);

		List<QueryModelNode> qNodes = nc.getNodes();

		Assert.assertTrue(qNodes.equals(optTupNodes));

	}

	@Test
	public void testTwoIndex() throws Exception {

		String q1 = ""//
				+ "SELECT ?f ?m ?d ?h ?i " //
				+ "{" //
				+ "  ?f a ?m ."//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "  ?d <uri:hangOutWith> ?f ." //
				+ "  ?f <uri:hangOutWith> ?h ." //
				+ "  ?f <uri:associatesWith> ?i ." //
				+ "  ?i <uri:associatesWith> ?h ." //
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
				+ "SELECT ?m ?f ?d " //
				+ "{" //
				+ "  ?f a ?m ."//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "}";//

		String q6 = ""//
				+ "SELECT ?d ?f ?h " //
				+ "{" //
				+ "  ?d <uri:hangOutWith> ?f ." //
				+ "  ?f <uri:hangOutWith> ?h ." //
				+ "}";//

		String q7 = ""//
				+ "SELECT ?f ?i ?h " //
				+ "{" //
				+ "  ?f <uri:associatesWith> ?i ." //
				+ "  ?i <uri:associatesWith> ?h ." //
				+ "}";//

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q1, null);
		ParsedQuery pq2 = parser.parseQuery(q2, null);
		ParsedQuery pq3 = parser.parseQuery(q3, null);
		ParsedQuery pq4 = parser.parseQuery(q4, null);
		ParsedQuery pq5 = parser.parseQuery(q5, null);
		ParsedQuery pq6 = parser.parseQuery(q6, null);
		ParsedQuery pq7 = parser.parseQuery(q7, null);

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
		SimpleExternalTupleSet extTup6 = new SimpleExternalTupleSet(
				(Projection) pq7.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup2);
		list.add(extTup1);
		list.add(extTup3);

		List<QueryModelNode> optTupNodes = Lists.newArrayList();
		optTupNodes.add(extTup4);
		optTupNodes.add(extTup6);
		optTupNodes.add(extTup5);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);

		Iterator<TupleExpr> plans = new TupleExecutionPlanGenerator()
				.getPlans(iep.getIndexedTuples());
		IndexPlanValidator ipv = new IndexPlanValidator(true);
		Iterator<TupleExpr> validPlans = ipv.getValidTuples(plans);

		ThreshholdPlanSelector tps = new ThreshholdPlanSelector(
				pq1.getTupleExpr());
		TupleExpr optimalTup = tps.getThreshholdQueryPlan(validPlans, .2, .6,
				.4, 0);

		NodeCollector nc = new NodeCollector();
		optimalTup.visit(nc);

		List<QueryModelNode> qNodes = nc.getNodes();

		Assert.assertTrue(qNodes.equals(optTupNodes));

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

		String q5 = ""//
				+ "SELECT  ?f ?m ?d ?a ?h ?r " //
				+ "{" //
				+ "  ?f a ?m ."//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "  ?a a ?h ."//
				+ "  ?h <http://www.w3.org/2000/01/rdf-schema#label> ?r ."//
				+ "  ?r <uri:talksTo> ?a . "//
				+ "}";//

		String q6 = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ "  ?e a ?l ."//
				+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
				+ "  ?c <uri:talksTo> ?e . "//
				+ "}";//

		String q7 = ""//
				+ "SELECT ?n ?o ?p " //
				+ "{" //
				+ "  ?n a ?o ."//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?p ."//
				+ "  ?p <uri:talksTo> ?n . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = null;
		ParsedQuery pq2 = null;
		ParsedQuery pq3 = null;
		ParsedQuery pq4 = null;
		ParsedQuery pq5 = null;
		ParsedQuery pq6 = null;
		ParsedQuery pq7 = null;

		try {
			pq1 = parser.parseQuery(q1, null);
			pq2 = parser.parseQuery(q2, null);
			pq3 = parser.parseQuery(q3, null);
			pq4 = parser.parseQuery(q4, null);
			pq5 = parser.parseQuery(q5, null);
			pq6 = parser.parseQuery(q6, null);
			pq7 = parser.parseQuery(q7, null);

		} catch (Exception e) {
			e.printStackTrace();
		}

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
		SimpleExternalTupleSet extTup6 = new SimpleExternalTupleSet(
				(Projection) pq7.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup2);
		list.add(extTup1);
		list.add(extTup3);

		List<ExternalTupleSet> list2 = new ArrayList<ExternalTupleSet>();

		list2.add(extTup4);
		list2.add(extTup5);
		list2.add(extTup6);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);

		Iterator<TupleExpr> plans = new TupleExecutionPlanGenerator()
				.getPlans(iep.getIndexedTuples());
		IndexPlanValidator ipv = new IndexPlanValidator(false);
		Iterator<TupleExpr> validPlans = ipv.getValidTuples(plans);

		ThreshholdPlanSelector tps = new ThreshholdPlanSelector(
				pq1.getTupleExpr());
		TupleExpr optimalTup = tps.getThreshholdQueryPlan(validPlans, .4, .8,
				.1, .1);

		NodeCollector nc = new NodeCollector();
		optimalTup.visit(nc);

	}

	@Test
	public void twoIndexFilterTest() {

		String q1 = ""//
				+ "SELECT ?f ?m ?d ?e ?l ?c " //
				+ "{" //
				+ "  Filter(?f > \"5\")." //
				+ "  Filter(?e > \"5\")." //
				+ "  ?f a ?m ."//
				+ "  ?e a ?l ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "  ?c <uri:talksTo> ?e . "//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
				+ "}";//

		String q2 = ""//
				+ "SELECT ?s ?t ?u " //
				+ "{" //
				+ "  ?s a ?t ."//
				+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
				+ "  ?u <uri:talksTo> ?s . "//
				+ "}";//

		String q3 = ""//
				+ "SELECT ?s ?t ?u " //
				+ "{" //
				+ " Filter(?s > \"5\") ."//
				+ "  ?s a ?t ."//
				+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
				+ "  ?u <uri:talksTo> ?s . "//
				+ "}";//

		String q4 = ""//
				+ "SELECT ?f ?m ?d " //
				+ "{" //
				+ " Filter(?f > \"5\") ."//
				+ "  ?f a ?m ."//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "}";//

		String q5 = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ " Filter(?e > \"5\") ."//
				+ "  ?e a ?l ."//
				+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
				+ "  ?c <uri:talksTo> ?e . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = null;
		ParsedQuery pq2 = null;
		ParsedQuery pq3 = null;
		ParsedQuery pq4 = null;
		ParsedQuery pq5 = null;

		try {
			pq1 = parser.parseQuery(q1, null);
			pq2 = parser.parseQuery(q2, null);
			pq3 = parser.parseQuery(q3, null);
			pq4 = parser.parseQuery(q4, null);
			pq5 = parser.parseQuery(q5, null);

		} catch (Exception e) {
			e.printStackTrace();
		}

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());
		SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				(Projection) pq4.getTupleExpr());
		SimpleExternalTupleSet extTup4 = new SimpleExternalTupleSet(
				(Projection) pq5.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup2);
		list.add(extTup1);

		List<ExternalTupleSet> list2 = new ArrayList<ExternalTupleSet>();

		list2.add(extTup3);
		list2.add(extTup4);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);

		Iterator<TupleExpr> plans = new TupleExecutionPlanGenerator()
				.getPlans(iep.getIndexedTuples());
		IndexPlanValidator ipv = new IndexPlanValidator(false);
		Iterator<TupleExpr> validPlans = ipv.getValidTuples(plans);

		ThreshholdPlanSelector tps = new ThreshholdPlanSelector(
				pq1.getTupleExpr());
		TupleExpr optimalTup = tps.getThreshholdQueryPlan(validPlans, .4, .8,
				.1, .1);

		NodeCollector nc = new NodeCollector();
		optimalTup.visit(nc);

		Assert.assertEquals(nc.getNodes().size(), list2.size());

		for (QueryModelNode e : nc.getNodes()) {
			Assert.assertTrue(list2.contains(e));
		}

	}

	@Test
	public void testCost1() throws Exception {

		String q1 = ""//
				+ "SELECT ?f ?m ?d ?h ?i " //
				+ "{" //
				+ "  ?f a ?m ."//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "  ?d <uri:hangOutWith> ?f ." //
				+ "  ?f <uri:hangOutWith> ?h ." //
				+ "  ?f <uri:associatesWith> ?i ." //
				+ "  ?i <uri:associatesWith> ?h ." //
				+ "}";//

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q1, null);

		ThreshholdPlanSelector tps = new ThreshholdPlanSelector(
				pq1.getTupleExpr());
		double cost = tps.getCost(pq1.getTupleExpr(), .6, .4, 0);
		Assert.assertEquals(.7, cost, .01);

	}

	@Test
	public void testCost2() throws Exception {

		String q1 = ""//
				+ "SELECT ?f ?m ?d ?e ?l ?c " //
				+ "{" //
				+ "  ?f a ?m ."//
				+ "  ?e a ?l ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "  ?c <uri:talksTo> ?e . "//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q1, null);

		ThreshholdPlanSelector tps = new ThreshholdPlanSelector(
				pq1.getTupleExpr());
		double cost = tps.getCost(pq1.getTupleExpr(), .4, .3, .3);
		Assert.assertEquals(.58, cost, .000000001);

	}

	@Test
	public void testCost3() throws Exception {

		String q1 = ""//
				+ "SELECT ?f ?m ?d ?e ?l ?c " //
				+ "{" //
				+ "  Filter(?f > \"5\")." //
				+ "  Filter(?e > \"6\")." //
				+ "  ?f a ?m ."//
				+ "  ?e a ?l ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "  ?c <uri:talksTo> ?e . "//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
				+ "}";//

		String q2 = ""//
				+ "SELECT ?s ?t ?u " //
				+ "{" //
				+ "  ?s a ?t ."//
				+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
				+ "  ?u <uri:talksTo> ?s . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q1, null);
		ParsedQuery pq2 = parser.parseQuery(q2, null);

		SimpleExternalTupleSet sep = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		List<ExternalTupleSet> eList = Lists.newArrayList();
		eList.add(sep);

		final TupleExpr te = pq1.getTupleExpr().clone();
		final PCJOptimizer pcj = new PCJOptimizer(eList, false);
        pcj.optimize(te, null, null);

		ThreshholdPlanSelector tps = new ThreshholdPlanSelector(
				pq1.getTupleExpr());
		double cost = tps.getCost(te, .4, .3, .3);
		Assert.assertEquals(.575, cost, .0001);

	}

	public static class NodeCollector extends
			QueryModelVisitorBase<RuntimeException> {

		List<QueryModelNode> qNodes = Lists.newArrayList();

		public List<QueryModelNode> getNodes() {
			return qNodes;
		}

		@Override
		public void meetNode(QueryModelNode node) {
			if (node instanceof StatementPattern
					|| node instanceof ExternalTupleSet) {
				qNodes.add(node);
			}
			super.meetNode(node);

		}

	}

}
