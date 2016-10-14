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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import com.google.common.collect.Sets;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.SimpleExternalTupleSet;
import org.apache.rya.indexing.pcj.matching.PCJOptimizer;

public class PrecompJoinOptimizerTest {

	@Test
	public void testThreeIndex() throws Exception {

		final String q7 = ""//
				+ "SELECT ?s ?t ?u " //
				+ "{" //
				+ "  ?s a ?t ."//
				+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
				+ "  ?u <uri:talksTo> ?s . "//
				+ "}";//

		final String q8 = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ "  ?e a ?l ."//
				+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
				+ "  ?c <uri:talksTo> ?e . "//
				+ "}";//

		final String q9 = ""//
				+ "SELECT ?f ?m ?d " //
				+ "{" //
				+ "  ?f a ?m ."//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "}";//

		final String q15 = ""//
				+ "SELECT ?f ?m ?d ?e ?l ?c " //
				+ "{" //
				+ "  ?f a ?m ."//
				+ "  ?e a ?l ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "  ?c <uri:talksTo> ?e . "//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
				+ "}";//

		final SPARQLParser parser = new SPARQLParser();

		final ParsedQuery pq1 = parser.parseQuery(q15, null);
		final ParsedQuery pq2 = parser.parseQuery(q7, null);
		final ParsedQuery pq3 = parser.parseQuery(q8, null);
		final ParsedQuery pq4 = parser.parseQuery(q9, null);

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());
		final SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				(Projection) pq4.getTupleExpr());

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup1);

		final List<QueryModelNode> optTupNodes = Lists.newArrayList();
		optTupNodes.add(extTup2);
		optTupNodes.add(extTup3);

		final PCJOptimizer pcj = new PCJOptimizer(list, true);
		final TupleExpr te = pq1.getTupleExpr();
		pcj.optimize(te, null, null);

		final NodeCollector nc = new NodeCollector();
		te.visit(nc);

		Assert.assertEquals(nc.qNodes.size(), optTupNodes.size());
		for (final QueryModelNode node : nc.qNodes) {
			Assert.assertTrue(optTupNodes.contains(node));
		}

	}

	@Test
	public void testThreeIndex2() throws Exception {

		final String q1 = ""//
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

		final String q2 = ""//
				+ "SELECT ?u ?s ?t " //
				+ "{" //
				+ "  ?s a ?t ."//
				+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
				+ "  ?u <uri:talksTo> ?s . "//
				+ "}";//

		final String q3 = ""//
				+ "SELECT ?e ?c ?l " //
				+ "{" //
				+ "  ?c a ?l ."//
				+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?e ."//
				+ "  ?e <uri:talksTo> ?c . "//
				+ "}";//

		final String q4 = ""//
				+ "SELECT ?d ?f ?m " //
				+ "{" //
				+ "  ?f a ?m ."//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "}";//

		final SPARQLParser parser = new SPARQLParser();

		final ParsedQuery pq1 = parser.parseQuery(q1, null);
		final ParsedQuery pq2 = parser.parseQuery(q2, null);
		final ParsedQuery pq3 = parser.parseQuery(q3, null);
		final ParsedQuery pq4 = parser.parseQuery(q4, null);

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());
		final SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				(Projection) pq4.getTupleExpr());

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup1);

		final List<StatementPattern> spList = StatementPatternCollector
				.process(pq1.getTupleExpr());
		final List<QueryModelNode> optTupNodes = Lists.newArrayList();
		optTupNodes.add(extTup3);
		optTupNodes.add(spList.get(6));
		optTupNodes.add(extTup2);

		final PCJOptimizer pcj = new PCJOptimizer(list, true);
		final TupleExpr te = pq1.getTupleExpr();
		pcj.optimize(te, null, null);

		final NodeCollector nc = new NodeCollector();
		te.visit(nc);

		Assert.assertEquals(nc.qNodes, Sets.newHashSet(optTupNodes));

	}

	@Test
	public void testSixIndex() throws Exception {

		final String q1 = ""//
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

		final String q2 = ""//
				+ "SELECT ?t ?s ?u " //
				+ "{" //
				+ "  ?s a ?t ."//
				+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
				+ "  ?u <uri:talksTo> ?s . "//
				+ "}";//

		final String q3 = ""//
				+ "SELECT ?s ?t ?u " //
				+ "{" //
				+ "  ?s <uri:hangOutWith> ?t ." //
				+ "  ?t <uri:hangOutWith> ?u ." //
				+ "}";//

		final String q4 = ""//
				+ "SELECT ?s ?t ?u " //
				+ "{" //
				+ "  ?s <uri:associatesWith> ?t ." //
				+ "  ?t <uri:associatesWith> ?u ." //
				+ "}";//

		final String q5 = ""//
				+ "SELECT ?m ?f ?d " //
				+ "{" //
				+ "  ?f a ?m ."//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "}";//

		final String q6 = ""//
				+ "SELECT ?d ?f ?h " //
				+ "{" //
				+ "  ?d <uri:hangOutWith> ?f ." //
				+ "  ?f <uri:hangOutWith> ?h ." //
				+ "}";//

		final String q7 = ""//
				+ "SELECT ?f ?i ?h " //
				+ "{" //
				+ "  ?f <uri:associatesWith> ?i ." //
				+ "  ?i <uri:associatesWith> ?h ." //
				+ "}";//

		final SPARQLParser parser = new SPARQLParser();

		final ParsedQuery pq1 = parser.parseQuery(q1, null);
		final ParsedQuery pq2 = parser.parseQuery(q2, null);
		final ParsedQuery pq3 = parser.parseQuery(q3, null);
		final ParsedQuery pq4 = parser.parseQuery(q4, null);
		final ParsedQuery pq5 = parser.parseQuery(q5, null);
		final ParsedQuery pq6 = parser.parseQuery(q6, null);
		final ParsedQuery pq7 = parser.parseQuery(q7, null);

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());
		final SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				(Projection) pq4.getTupleExpr());
		final SimpleExternalTupleSet extTup4 = new SimpleExternalTupleSet(
				(Projection) pq5.getTupleExpr());
		final SimpleExternalTupleSet extTup5 = new SimpleExternalTupleSet(
				(Projection) pq6.getTupleExpr());
		final SimpleExternalTupleSet extTup6 = new SimpleExternalTupleSet(
				(Projection) pq7.getTupleExpr());

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup2);
		list.add(extTup1);
		list.add(extTup3);

		final List<QueryModelNode> optTupNodes = Lists.newArrayList();
		optTupNodes.add(extTup4);
		optTupNodes.add(extTup6);
		optTupNodes.add(extTup5);

		final PCJOptimizer pcj = new PCJOptimizer(list, true);
		final TupleExpr te = pq1.getTupleExpr();
		pcj.optimize(te, null, null);

		System.out.println(te);

		final NodeCollector nc = new NodeCollector();
		te.visit(nc);

		Assert.assertEquals(nc.qNodes, Sets.newHashSet(optTupNodes));

	}

	@Test
	public void twoFourIndexWithFilterTest() {

		final String q1 = ""//
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

		final String q2 = ""//
				+ "SELECT ?s ?t ?u " //
				+ "{" //
				+ "  ?s a ?t ."//
				+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
				+ "  ?u <uri:talksTo> ?s . "//
				+ "}";//

		final String q3 = ""//
				+ "SELECT ?s ?t ?u " //
				+ "{" //
				+ " Filter(?s > \"5\") ."//
				+ "  ?s a ?t ."//
				+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
				+ "  ?u <uri:talksTo> ?s . "//
				+ "}";//

		final String q4 = ""//
				+ "SELECT ?f ?m ?d " //
				+ "{" //
				+ " Filter(?f > \"5\") ."//
				+ "  ?f a ?m ."//
				+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
				+ "  ?d <uri:talksTo> ?f . "//
				+ "}";//

		final String q5 = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ " Filter(?e > \"5\") ."//
				+ "  ?e a ?l ."//
				+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
				+ "  ?c <uri:talksTo> ?e . "//
				+ "}";//

		final SPARQLParser parser = new SPARQLParser();

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

		} catch (final Exception e) {
			e.printStackTrace();
		}

		final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());
		final SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				(Projection) pq4.getTupleExpr());
		final SimpleExternalTupleSet extTup4 = new SimpleExternalTupleSet(
				(Projection) pq5.getTupleExpr());

		final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup2);
		list.add(extTup1);

		final List<ExternalTupleSet> list2 = new ArrayList<ExternalTupleSet>();

		list2.add(extTup3);
		list2.add(extTup4);

		final PCJOptimizer pcj = new PCJOptimizer(list, true);
		final TupleExpr te = pq1.getTupleExpr();
		pcj.optimize(te, null, null);

		System.out.println(te);

		final NodeCollector nc = new NodeCollector();
		te.visit(nc);

		Assert.assertEquals(nc.qNodes.size(), list2.size());

		for (final QueryModelNode e : nc.qNodes) {
			Assert.assertTrue(list2.contains(e));
		}

	}

	public static class NodeCollector extends
			QueryModelVisitorBase<RuntimeException> {

		Set<QueryModelNode> qNodes = new HashSet<>();

		@Override
		public void meetNode(final QueryModelNode node) {
			if (node instanceof StatementPattern
					|| node instanceof ExternalTupleSet) {
				qNodes.add(node);
			}
			super.meetNode(node);

		}

        public List<QueryModelNode> getNodes() {
            return Lists.newArrayList(qNodes);
        }

	}

}
