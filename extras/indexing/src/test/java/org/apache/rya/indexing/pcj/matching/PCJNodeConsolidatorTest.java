package org.apache.rya.indexing.pcj.matching;
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

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class PCJNodeConsolidatorTest {


	@Test
	public void testBasicOptionalWithFilter() throws Exception {

		String query1 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ "  Filter(?e = <uri:Bob>)" //
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  OPTIONAL{?e <uri:talksTo> ?l } . "//
				+ "  OPTIONAL{?c <uri:worksAt> <uri:Apple>} . " //
				+ "  ?e a ?c . "//
				+ "  ?e <uri:livesIn> <uri:Virginia>"
				+ "}";//

		String query2 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ " Filter(?e = <uri:Bob>)" //
				+ "  ?e a ?c . "//
				+ "  OPTIONAL{?e <uri:talksTo> ?l } . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		TopOfQueryFilterRelocator.moveFiltersToTop(te1);
		TopOfQueryFilterRelocator.moveFiltersToTop(te2);
		Filter filter1 = (Filter) ((Projection) te1).getArg();
		Filter filter2 = (Filter) ((Projection) te2).getArg();

		OptionalJoinSegment seg1 = new OptionalJoinSegment(filter1);
		OptionalJoinSegment seg2 = new OptionalJoinSegment(filter2);

		PCJNodeConsolidator consolidator = new PCJNodeConsolidator(seg1.getOrderedNodes(), seg2.getOrderedNodes());

		List<QueryModelNode> queryNodes = new ArrayList<>(seg1.getOrderedNodes());
		QueryModelNode node = queryNodes.remove(0);
		queryNodes.add(1,node);
		node = queryNodes.remove(4);
		queryNodes.add(2, node);

		Assert.assertTrue(consolidator.consolidateNodes());
		Assert.assertEquals(consolidator.getQueryNodes(), queryNodes);

	}



	@Test
	public void testUpperLowerBoundOptional() throws Exception {

		String query1 = ""//
				+ "SELECT ?a ?b ?c ?d" //
				+ "{" //
				+ "  ?a <uri:p5> <uri:const3>" //
				+ "  OPTIONAL{<uri:const2> <uri:p4> ?d } . "//
				+ "  ?a <uri:p4> ?b . "//
				+ "  OPTIONAL{?a <uri:p3> ?c} . " //
				+ "  OPTIONAL{<uri:const1> <uri:p2> ?b} . "//
				+ "  ?c <uri:p1> ?d "
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b ?c ?d" //
				+ "{" //
				+ "  ?a <uri:p4> ?b . "//
				+ "  OPTIONAL{<uri:const2> <uri:p4> ?d } . "//
				+ "  ?c <uri:p1> ?d "
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		Join join1 = (Join) ((Projection) te1).getArg();
		Join join2 = (Join) ((Projection) te2).getArg();

		OptionalJoinSegment seg1 = new OptionalJoinSegment(join1);
		OptionalJoinSegment seg2 = new OptionalJoinSegment(join2);

		PCJNodeConsolidator consolidator = new PCJNodeConsolidator(seg1.getOrderedNodes(), seg2.getOrderedNodes());

		List<QueryModelNode> queryNodes = new ArrayList<>(seg1.getOrderedNodes());
		QueryModelNode node = queryNodes.remove(0);
		queryNodes.add(1,node);
		node = queryNodes.remove(3);
		queryNodes.add(2, node);
		node = queryNodes.remove(4);
		queryNodes.add(2, node);

		Assert.assertTrue(consolidator.consolidateNodes());
		Assert.assertEquals(consolidator.getQueryNodes(), queryNodes);

	}


	@Test
	public void testAlreadyInOrder() throws Exception {

		String query1 = ""//
				+ "SELECT ?a ?b ?c ?d" //
				+ "{" //
				+ "  ?a <uri:p5> <uri:const3>" //
				+ "  OPTIONAL{?a <uri:p3> ?c} . " //
				+ "  ?a <uri:p4> ?b . "//
				+ "  OPTIONAL{<uri:const2> <uri:p4> ?d } . "//
				+ "  ?c <uri:p1> ?d "
				+ "  OPTIONAL{<uri:const1> <uri:p2> ?b} . "//
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b ?c ?d" //
				+ "{" //
				+ "  ?a <uri:p4> ?b . "//
				+ "  OPTIONAL{<uri:const2> <uri:p4> ?d } . "//
				+ "  ?c <uri:p1> ?d "
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		LeftJoin join1 = (LeftJoin) ((Projection) te1).getArg();
		Join join2 = (Join) ((Projection) te2).getArg();

		OptionalJoinSegment seg1 = new OptionalJoinSegment(join1);
		OptionalJoinSegment seg2 = new OptionalJoinSegment(join2);

		PCJNodeConsolidator consolidator = new PCJNodeConsolidator(seg1.getOrderedNodes(), seg2.getOrderedNodes());
		List<QueryModelNode> queryNodes = new ArrayList<>(seg1.getOrderedNodes());

		Assert.assertTrue(consolidator.consolidateNodes());
		Assert.assertEquals(consolidator.getQueryNodes(), queryNodes);

	}


	@Test
	public void testInvalidOrder() throws Exception {

		String query1 = ""//
				+ "SELECT ?a ?b ?c ?d" //
				+ "{" //
				+ "  ?a <uri:p5> <uri:const3>" //
				+ "  OPTIONAL{<uri:const2> <uri:p4> ?d } . "//
				+ "  ?a <uri:p4> ?b . "//
				+ "  OPTIONAL{?a <uri:p3> ?c} . " //
				+ "  OPTIONAL{<uri:const1> <uri:p2> ?b} . "//
				+ "  ?c <uri:p1> ?d "
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b ?c ?d" //
				+ "{" //
				+ "  ?a <uri:p4> ?b . "//
				+ "  ?c <uri:p1> ?d "
				+ "  OPTIONAL{<uri:const2> <uri:p4> ?d } . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		Join join1 = (Join) ((Projection) te1).getArg();
		LeftJoin join2 = (LeftJoin) ((Projection) te2).getArg();

		OptionalJoinSegment seg1 = new OptionalJoinSegment(join1);
		OptionalJoinSegment seg2 = new OptionalJoinSegment(join2);

		PCJNodeConsolidator consolidator = new PCJNodeConsolidator(seg1.getOrderedNodes(), seg2.getOrderedNodes());

		Assert.assertTrue(!consolidator.consolidateNodes());
	}



	@Test
	public void testCantConsolidate() throws Exception {

		String query1 = ""//
				+ "SELECT ?a ?b ?c" //
				+ "{" //
				+ "  ?c <uri:p5> <uri:o2> ." //
				+ "  ?a <uri:p4> <uri:o1> . "//
				+ "  OPTIONAL{?a <uri:p3> ?b} . " //
				+ "  OPTIONAL{<uri:s2> <uri:p2> ?b} . "//
				+ "  <uri:s1> <uri:p1> ?b "
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b" //
				+ "{" //
				+ "  <uri:s1> <uri:p1> ?b . "//
				+ "  ?a <uri:p4> <uri:o1> "
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		Join join1 = (Join) ((Projection) te1).getArg();
		Join join2 = (Join) ((Projection) te2).getArg();

		OptionalJoinSegment seg1 = new OptionalJoinSegment(join1);
		OptionalJoinSegment seg2 = new OptionalJoinSegment(join2);

		PCJNodeConsolidator consolidator = new PCJNodeConsolidator(seg1.getOrderedNodes(), seg2.getOrderedNodes());

		Assert.assertTrue(!consolidator.consolidateNodes());
	}


	@Test
	public void testMoveAcrossMultipleLeftJoins() throws Exception {

		String query1 = ""//
				+ "SELECT ?a ?b ?c ?e ?f" //
				+ "{" //
				+ "  ?c <uri:p5> <uri:o2> ." //
				+ "  ?a <uri:p4> <uri:o1> . "//
				+ "  OPTIONAL{?a <uri:p3> ?b} . " //
				+ "  OPTIONAL{<uri:s2> <uri:p2> ?e} . "//
				+ "  OPTIONAL{<uri:s2> <uri:p2> ?f} . "//
				+ "  <uri:s1> <uri:p1> ?f "
				+ "}";//

		String query2 = ""//
				+ "SELECT ?f ?c" //
				+ "{" //
				+ "  ?c <uri:p5> <uri:o2> . "//
				+ "  <uri:s1> <uri:p1> ?f "
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		Join join1 = (Join) ((Projection) te1).getArg();
		Join join2 = (Join) ((Projection) te2).getArg();

		OptionalJoinSegment seg1 = new OptionalJoinSegment(join1);
		OptionalJoinSegment seg2 = new OptionalJoinSegment(join2);

		PCJNodeConsolidator consolidator = new PCJNodeConsolidator(seg1.getOrderedNodes(), seg2.getOrderedNodes());

		List<QueryModelNode> queryNodes = new ArrayList<>(seg1.getOrderedNodes());
		QueryModelNode node = queryNodes.remove(5);
		queryNodes.add(1,node);

		Assert.assertTrue(consolidator.consolidateNodes());
		Assert.assertEquals(consolidator.getQueryNodes(), queryNodes);
	}



	@Test
	public void testExactMatchReOrdered() throws Exception {

		String query1 = ""//
				+ "SELECT ?a ?b ?c ?d ?e ?f ?g ?h" //
				+ "{" //
				+ "  ?a <uri:p0> ?b ." //
				+ "  OPTIONAL{?b <uri:p2> ?c. ?c <uri:p1> ?d} . " //
				+ "  OPTIONAL{?b <uri:p3> ?e. ?e <uri:p1> ?f} . "//
				+ "  OPTIONAL{?b <uri:p4> ?g. ?g <uri:p1> ?h} . "//
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b ?c ?d ?e ?f ?g ?h" //
				+ "{" //
				+ "  ?a <uri:p0> ?b ." //
				+ "  OPTIONAL{?b <uri:p2> ?c. ?c <uri:p1> ?d} . " //
				+ "  OPTIONAL{?b <uri:p4> ?g. ?g <uri:p1> ?h} . "//
				+ "  OPTIONAL{?b <uri:p3> ?e. ?e <uri:p1> ?f} . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		LeftJoin join1 = (LeftJoin) ((Projection) te1).getArg();
		LeftJoin join2 = (LeftJoin) ((Projection) te2).getArg();

		OptionalJoinSegment seg1 = new OptionalJoinSegment(join1);
		OptionalJoinSegment seg2 = new OptionalJoinSegment(join2);

		PCJNodeConsolidator consolidator = new PCJNodeConsolidator(seg1.getOrderedNodes(), seg2.getOrderedNodes());
		List<QueryModelNode> queryNodes = new ArrayList<>(seg2.getOrderedNodes());

		Assert.assertTrue(consolidator.consolidateNodes());
		Assert.assertEquals(consolidator.getQueryNodes(), queryNodes);
	}


	@Test
	public void testExactMatchLargeReOrdered() throws Exception {

		String query1 = ""//
				+ "SELECT ?a ?b ?c ?d ?e ?f ?g ?h" //
				+ "{" //
				+ "  ?a <uri:p0> ?b ." //
				+ "  OPTIONAL{?b <uri:p2> ?c. ?c <uri:p1> ?d} . " //
				+ "  OPTIONAL{?b <uri:p3> ?e. ?e <uri:p1> ?f} . "//
				+ "  OPTIONAL{?b <uri:p4> ?g. ?g <uri:p1> ?h} . "//
				+ "  OPTIONAL{?b <uri:p4> ?i. ?i <uri:p1> ?j} . "//
				+ "  OPTIONAL{?b <uri:p4> ?k. ?k <uri:p1> ?l} . "//
				+ "  OPTIONAL{?b <uri:p4> ?m. ?m <uri:p1> ?n} . "//
				+ "  OPTIONAL{?b <uri:p4> ?o. ?o <uri:p1> ?p} . "//
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b ?c ?d ?e ?f ?g ?h" //
				+ "{" //
				+ "  ?a <uri:p0> ?b ." //
				+ "  OPTIONAL{?b <uri:p4> ?o. ?o <uri:p1> ?p} . "//
				+ "  OPTIONAL{?b <uri:p4> ?g. ?g <uri:p1> ?h} . "//
				+ "  OPTIONAL{?b <uri:p2> ?c. ?c <uri:p1> ?d} . " //
				+ "  OPTIONAL{?b <uri:p4> ?i. ?i <uri:p1> ?j} . "//
				+ "  OPTIONAL{?b <uri:p4> ?m. ?m <uri:p1> ?n} . "//
				+ "  OPTIONAL{?b <uri:p4> ?k. ?k <uri:p1> ?l} . "//
				+ "  OPTIONAL{?b <uri:p3> ?e. ?e <uri:p1> ?f} . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		LeftJoin join1 = (LeftJoin) ((Projection) te1).getArg();
		LeftJoin join2 = (LeftJoin) ((Projection) te2).getArg();

		OptionalJoinSegment seg1 = new OptionalJoinSegment(join1);
		OptionalJoinSegment seg2 = new OptionalJoinSegment(join2);

		PCJNodeConsolidator consolidator = new PCJNodeConsolidator(seg1.getOrderedNodes(), seg2.getOrderedNodes());
		List<QueryModelNode> queryNodes = new ArrayList<>(seg2.getOrderedNodes());

		Assert.assertTrue(consolidator.consolidateNodes());
		Assert.assertEquals(consolidator.getQueryNodes(), queryNodes);
	}


	@Test
	public void testSwitchBoundVars() throws Exception {

		String query1 = ""//
				+ "SELECT ?a ?b " //
				+ "{" //
				+ "  ?a <uri:p0> ?b ." //
				+ " OPTIONAL{ ?a <uri:p1> <uri:o1> } ." //
				+ " ?a <uri:p2> <uri:o2> " //
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b " //
				+ "{" //
				+ " ?a <uri:p2> <uri:o2> " //
				+ " OPTIONAL{ ?a <uri:p1> <uri:o1> } ." //
				+ "  ?a <uri:p0> ?b ." //
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		Join join1 = (Join) ((Projection) te1).getArg();
		Join join2 = (Join) ((Projection) te2).getArg();

		OptionalJoinSegment seg1 = new OptionalJoinSegment(join1);
		OptionalJoinSegment seg2 = new OptionalJoinSegment(join2);

		PCJNodeConsolidator consolidator = new PCJNodeConsolidator(seg1.getOrderedNodes(), seg2.getOrderedNodes());
		List<QueryModelNode> queryNodes = new ArrayList<>(seg2.getOrderedNodes());

		Assert.assertTrue(consolidator.consolidateNodes());
		Assert.assertEquals(consolidator.getQueryNodes(), queryNodes);
	}



	@Test
	public void testSwitchTwoBoundVars() throws Exception {

		String query1 = ""//
				+ "SELECT ?a ?b ?c " //
				+ "{" //
				+ "  ?a <uri:p0> ?c ." //
				+ "  ?b<uri:p1> ?c ." //
				+ " OPTIONAL{ ?a <uri:p1> ?b } ." //
				+ " ?a <uri:p2> <uri:o2>. " //
				+ " ?b <uri:p3> <uri:o3> " //
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b ?c" //
				+ "{" //
				+ " ?a <uri:p2> <uri:o2>. " //
				+ " ?b <uri:p3> <uri:o3>. " //
				+ " OPTIONAL{ ?a <uri:p1> ?b } ." //
				+ "  ?a <uri:p0> ?c ." //
				+ "  ?b<uri:p1> ?c " //
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		Join join1 = (Join) ((Projection) te1).getArg();
		Join join2 = (Join) ((Projection) te2).getArg();

		OptionalJoinSegment seg1 = new OptionalJoinSegment(join1);
		OptionalJoinSegment seg2 = new OptionalJoinSegment(join2);

		PCJNodeConsolidator consolidator = new PCJNodeConsolidator(seg1.getOrderedNodes(), seg2.getOrderedNodes());
		List<QueryModelNode> queryNodes = new ArrayList<>(seg2.getOrderedNodes());

		Assert.assertTrue(consolidator.consolidateNodes());
		Assert.assertEquals(consolidator.getQueryNodes(), queryNodes);
	}










}
