package mvm.rya.indexing.pcj.matching;
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

import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;
import mvm.rya.indexing.external.tupleSet.SimpleExternalTupleSet;

import org.junit.Test;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class PCJOptimizerTest {

	@Test
	public void testBasicSegment() throws MalformedQueryException {

		String query1 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  OPTIONAL {?e <uri:talksTo> ?l}  . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b ?m" //
				+ "{" //
				+ "  ?a a ?b . "//
				+ "  OPTIONAL {?a <uri:talksTo> ?m}  . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();

		SimpleExternalTupleSet pcj = new SimpleExternalTupleSet(
				(Projection) te2);
		List<ExternalTupleSet> externalList = new ArrayList<>();
		externalList.add(pcj);

		PCJOptimizer optimizer = new PCJOptimizer(externalList, false);
		optimizer.optimize(te1, null, null);
		System.out.println(te1);

		// Assert.assertEquals(true, jsm.matchPCJ(pcj));
		// TupleExpr te = jsm.getQuery();
		// Assert.assertEquals(new HashSet<QueryModelNode>(),
		// jsm.getUnmatchedArgs());
		//
		// Set<QueryModelNode> qNodes = LeftJoinQueryNodeGatherer.getNodes(te);
		// List<QueryModelNode> nodes = jsm.getOrderedNodes();
		// Set<QueryModelNode> nodeSet = new HashSet<>();
		// nodeSet.add(nodes.get(0));
		// nodeSet.add(pcj);
		//
		// Assert.assertEquals(nodeSet, new HashSet<QueryModelNode>(nodes));
		// Assert.assertEquals(nodeSet, qNodes);

	}

	@Test
	public void testSegmentWithUnion() throws MalformedQueryException {

		String query1 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ " {?e <uri:p1> <uri:o1>. } UNION { ?e a ?c. OPTIONAL {?e <uri:talksTo> ?l}. ?e <uri:p5> <uri:o4>. ?e <uri:p4> <uri:o3> }  . "//
				+ "  ?e <uri:p2> ?c . "//
				+ "  ?e <uri:p3> <uri:o2>  . "//
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b ?m" //
				+ "{" //
				+ " ?a <uri:p5> <uri:o4> ." //
				+ " ?a <uri:p4> <uri:o3> ." //
				+ "  OPTIONAL {?a <uri:talksTo> ?m} . "//
				+ "  ?a a ?b . "//
				+ "}";//

		String query3 = ""//
				+ "SELECT ?h ?i" //
				+ "{" //
				+ "  ?h <uri:p2> ?i . "//
				+ "  ?h <uri:p3> <uri:o2>  . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		ParsedQuery pq3 = parser.parseQuery(query3, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		TupleExpr te3 = pq3.getTupleExpr();

		SimpleExternalTupleSet pcj1 = new SimpleExternalTupleSet(
				(Projection) te2);
		SimpleExternalTupleSet pcj2 = new SimpleExternalTupleSet(
				(Projection) te3);
		List<ExternalTupleSet> externalList = new ArrayList<>();
		externalList.add(pcj1);
		externalList.add(pcj2);

		PCJOptimizer optimizer = new PCJOptimizer(externalList, false);
		optimizer.optimize(te1, null, null);
		System.out.println(te1);

		// Assert.assertEquals(true, jsm.matchPCJ(pcj));
		// TupleExpr te = jsm.getQuery();
		// Assert.assertEquals(new HashSet<QueryModelNode>(),
		// jsm.getUnmatchedArgs());
		//
		// Set<QueryModelNode> qNodes = LeftJoinQueryNodeGatherer.getNodes(te);
		// List<QueryModelNode> nodes = jsm.getOrderedNodes();
		// Set<QueryModelNode> nodeSet = new HashSet<>();
		// nodeSet.add(nodes.get(0));
		// nodeSet.add(pcj);
		//
		// Assert.assertEquals(nodeSet, new HashSet<QueryModelNode>(nodes));
		// Assert.assertEquals(nodeSet, qNodes);

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

		SimpleExternalTupleSet pcj = new SimpleExternalTupleSet(
				(Projection) te2);
		List<ExternalTupleSet> externalList = new ArrayList<>();
		externalList.add(pcj);

		PCJOptimizer optimizer = new PCJOptimizer(externalList, false);
		optimizer.optimize(te1, null, null);
		System.out.println(te1);
	}

	@Test
	public void testSubsetMatchLargeReOrdered() throws Exception {

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
				+ "  OPTIONAL{?b <uri:p3> ?e. ?e <uri:p1> ?f} . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();

		SimpleExternalTupleSet pcj = new SimpleExternalTupleSet(
				(Projection) te2);
		List<ExternalTupleSet> externalList = new ArrayList<>();
		externalList.add(pcj);

		PCJOptimizer optimizer = new PCJOptimizer(externalList, false);
		optimizer.optimize(te1, null, null);
		System.out.println(te1);
	}

	@Test
	public void testSwitchTwoBoundVars() throws Exception {

		String query1 = ""//
				+ "SELECT ?a ?b ?c " //
				+ "{" //
				+ "  ?a <uri:p0> ?c ." //
				+ "  ?c <uri:p5> <uri:o5> ." //
				+ " OPTIONAL{?c <uri:p4> <uri:o4>} ." + "  ?b<uri:p1> ?c ." //
				+ " OPTIONAL{ ?a <uri:p1> ?b } ." //
				+ " ?a <uri:p2> <uri:o2>. " //
				+ " ?b <uri:p3> <uri:o3> " //
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b ?c " //
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
		SimpleExternalTupleSet pcj = new SimpleExternalTupleSet(
				(Projection) te2);
		List<ExternalTupleSet> externalList = new ArrayList<>();
		externalList.add(pcj);

		PCJOptimizer optimizer = new PCJOptimizer(externalList, false);
		optimizer.optimize(te1, null, null);
		System.out.println(te1);
	}

	@Test
	public void testSegmentWithLargeUnion() throws MalformedQueryException {

		String query1 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ " {?e <uri:p1> <uri:o1>. } UNION { " //
				+ "  ?e <uri:p0> ?l ." //
				+ "  ?l <uri:p5> <uri:o5> ." //
				+ " OPTIONAL{?l <uri:p4> <uri:o4>} ." + "  ?c<uri:p1> ?l ." //
				+ " OPTIONAL{ ?e <uri:p1> ?c } ." //
				+ " ?e <uri:p2> <uri:o2>. " //
				+ " ?c <uri:p3> <uri:o3> " //
				+ " }  . "//
				+ "  ?e <uri:p2> ?c . "//
				+ "  ?e <uri:p3> <uri:o2>  . "//
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b ?c " //
				+ "{" //
				+ " ?a <uri:p2> <uri:o2>. " //
				+ " ?b <uri:p3> <uri:o3>. " //
				+ " OPTIONAL{ ?a <uri:p1> ?b } ." //
				+ "  ?a <uri:p0> ?c ." //
				+ "  ?b<uri:p1> ?c " //
				+ "}";//

		String query3 = ""//
				+ "SELECT ?h ?i" //
				+ "{" //
				+ "  ?h <uri:p2> ?i . "//
				+ "  ?h <uri:p3> <uri:o2>  . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		ParsedQuery pq3 = parser.parseQuery(query3, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		TupleExpr te3 = pq3.getTupleExpr();

		SimpleExternalTupleSet pcj1 = new SimpleExternalTupleSet(
				(Projection) te2);
		SimpleExternalTupleSet pcj2 = new SimpleExternalTupleSet(
				(Projection) te3);
		List<ExternalTupleSet> externalList = new ArrayList<>();
		externalList.add(pcj1);
		externalList.add(pcj2);

		PCJOptimizer optimizer = new PCJOptimizer(externalList, false);
		optimizer.optimize(te1, null, null);
		System.out.println(te1);

		// Assert.assertEquals(true, jsm.matchPCJ(pcj));
		// TupleExpr te = jsm.getQuery();
		// Assert.assertEquals(new HashSet<QueryModelNode>(),
		// jsm.getUnmatchedArgs());
		//
		// Set<QueryModelNode> qNodes = LeftJoinQueryNodeGatherer.getNodes(te);
		// List<QueryModelNode> nodes = jsm.getOrderedNodes();
		// Set<QueryModelNode> nodeSet = new HashSet<>();
		// nodeSet.add(nodes.get(0));
		// nodeSet.add(pcj);
		//
		// Assert.assertEquals(nodeSet, new HashSet<QueryModelNode>(nodes));
		// Assert.assertEquals(nodeSet, qNodes);

	}

	@Test
	public void testSegmentWithUnionAndFilters() throws MalformedQueryException {

		String query1 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ " Filter(?e = <uri:s1>) " //
				+ " Filter(?c = <uri:s2>) " //
				+ " {?e <uri:p1> <uri:o1>. } UNION { ?e a ?c. OPTIONAL {?e <uri:talksTo> ?l}. ?e <uri:p5> <uri:o4>. ?e <uri:p4> <uri:o3> }  . "//
				+ "  ?e <uri:p2> ?c . "//
				+ "  ?e <uri:p3> <uri:o2>  . "//
				+ "}";//

		String query2 = ""//
				+ "SELECT ?a ?b ?m" //
				+ "{" //
				+ " Filter(?b = <uri:s2>) " //
				+ " ?a <uri:p5> <uri:o4> ." //
				+ " ?a <uri:p4> <uri:o3> ." //
				+ "  OPTIONAL {?a <uri:talksTo> ?m} . "//
				+ "  ?a a ?b . "//
				+ "}";//

		String query3 = ""//
				+ "SELECT ?h ?i" //
				+ "{" //
				+ " Filter(?h = <uri:s1>) " //
				+ "  ?h <uri:p2> ?i . "//
				+ "  ?h <uri:p3> <uri:o2>  . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		ParsedQuery pq3 = parser.parseQuery(query3, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		TupleExpr te3 = pq3.getTupleExpr();

		SimpleExternalTupleSet pcj1 = new SimpleExternalTupleSet(
				(Projection) te2);
		SimpleExternalTupleSet pcj2 = new SimpleExternalTupleSet(
				(Projection) te3);
		List<ExternalTupleSet> externalList = new ArrayList<>();
		externalList.add(pcj1);
		externalList.add(pcj2);

		PCJOptimizer optimizer = new PCJOptimizer(externalList, false);
		optimizer.optimize(te1, null, null);
		System.out.println(te1);

		// Assert.assertEquals(true, jsm.matchPCJ(pcj));
		// TupleExpr te = jsm.getQuery();
		// Assert.assertEquals(new HashSet<QueryModelNode>(),
		// jsm.getUnmatchedArgs());
		//
		// Set<QueryModelNode> qNodes = LeftJoinQueryNodeGatherer.getNodes(te);
		// List<QueryModelNode> nodes = jsm.getOrderedNodes();
		// Set<QueryModelNode> nodeSet = new HashSet<>();
		// nodeSet.add(nodes.get(0));
		// nodeSet.add(pcj);
		//
		// Assert.assertEquals(nodeSet, new HashSet<QueryModelNode>(nodes));
		// Assert.assertEquals(nodeSet, qNodes);

	}

	@Test
	public void testSegmentWithLeftJoinsAndFilters()
			throws MalformedQueryException {

		String query1 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ " Filter(?e = <uri:s1>) " //
				+ " Filter(?c = <uri:s2>) " //
				+ " ?e <uri:p1> <uri:o1>. "
				+ " OPTIONAL {?e <uri:p2> ?l}. "
				+ " ?c <uri:p3> <uri:o3>  . "//
				+ "  ?c <uri:p4> ?e  . "//
				+ " OPTIONAL {?e <uri:p2> ?c } . "//
				+ "}";//

		String query2 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ " Filter(?c = <uri:s2>) " //
				+ " ?e <uri:p1> <uri:o1>. "
				+ " OPTIONAL {?e <uri:p2> ?l}. "
				+ " ?c <uri:p3> <uri:o3>  . "//
				+ "}";//

		String query3 = ""//
				+ "SELECT ?e ?c" //
				+ "{" //
				+ " Filter(?e = <uri:s1>) " //
				+ "  ?c <uri:p4> ?e  . "//
				+ " OPTIONAL {?e <uri:p2> ?c } . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		ParsedQuery pq3 = parser.parseQuery(query3, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		TupleExpr te3 = pq3.getTupleExpr();

		SimpleExternalTupleSet pcj1 = new SimpleExternalTupleSet(
				(Projection) te2);
		SimpleExternalTupleSet pcj2 = new SimpleExternalTupleSet(
				(Projection) te3);
		List<ExternalTupleSet> externalList = new ArrayList<>();
		externalList.add(pcj1);
		externalList.add(pcj2);

		PCJOptimizer optimizer = new PCJOptimizer(externalList, false);
		optimizer.optimize(te1, null, null);
		System.out.println(te1);

		// Assert.assertEquals(true, jsm.matchPCJ(pcj));
		// TupleExpr te = jsm.getQuery();
		// Assert.assertEquals(new HashSet<QueryModelNode>(),
		// jsm.getUnmatchedArgs());
		//
		// Set<QueryModelNode> qNodes = LeftJoinQueryNodeGatherer.getNodes(te);
		// List<QueryModelNode> nodes = jsm.getOrderedNodes();
		// Set<QueryModelNode> nodeSet = new HashSet<>();
		// nodeSet.add(nodes.get(0));
		// nodeSet.add(pcj);
		//
		// Assert.assertEquals(nodeSet, new HashSet<QueryModelNode>(nodes));
		// Assert.assertEquals(nodeSet, qNodes);

	}

}
