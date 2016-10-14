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
import java.util.NoSuchElementException;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.SimpleExternalTupleSet;

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class IndexedExecutionPlanGeneratorTest {

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
			+ "  ?l <uri:talksTo> ?c . "//
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
			+ "SELECT ?cat ?chicken ?pig ?duck " //
			+ "{" //
			+ "  ?cat <uri:talksTo> ?chicken. "//
			+ "  ?pig <uri:talksTo> ?duck . "//
			+ "}";//

	private String q19 = ""//
			+ "SELECT ?f ?m ?d ?e ?l ?c " //
			+ "{" //
			+ "  ?f <uri:talksTo> ?m . "//
			+ "  ?d <uri:talksTo> ?e . "//
			+ "  ?l <uri:talksTo> ?c . "//
			+ "}";//

	private String q20 = ""//
			+ "SELECT ?f ?m " //
			+ "{" //
			+ "  ?f <uri:talksTo> ?m . "//
			+ "}";//

	private String q21 = ""//
			+ "SELECT ?s ?t ?u " //
			+ "{" //
			+ " Filter(?s > 3). " //
			+ "  ?s a ?t ."//
			+ "  ?t <http://www.w3.org/2000/01/rdf-schema#label> ?u ."//
			+ "  ?u <uri:talksTo> ?s . "//
			+ "}";//

	private String q22 = ""//
			+ "SELECT ?f ?m ?d ?e ?l ?c " //
			+ "{" //
			+ " Filter(?f > 3) ."//
			+ " Filter(?e > 3) ."//
			+ "  ?e a ?f ." //
			+ "  ?f a ?m ."//
			+ "  ?e a ?l ."//
			+ "  ?d <uri:talksTo> ?f . "//
			+ "  ?c <uri:talksTo> ?e . "//
			+ "  ?m <http://www.w3.org/2000/01/rdf-schema#label> ?d ."//
			+ "  ?l <http://www.w3.org/2000/01/rdf-schema#label> ?c ."//
			+ "}";//

	private String q23 = ""//
			+ "SELECT ?h ?i ?j " //
			+ "{" //
			+ " Filter(?h > 3) ."//
			+ " Filter(?i > 3) ."//
			+ "  ?h a ?i ." //
			+ "  ?h a ?j ."//
			+ "}";//

	@Test
	public void testTwoIndexLargeQuery() throws Exception {

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q15, null);
		ParsedQuery pq2 = parser.parseQuery(q7, null);
		ParsedQuery pq3 = parser.parseQuery(q12, null);

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup2);
		list.add(extTup1);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);
		List<ExternalTupleSet> indexSet = iep.getNormalizedIndices();
		Assert.assertEquals(4, indexSet.size());

		Iterator<TupleExpr> processedTups = iep.getIndexedTuples();

		int size = 0;

		while (processedTups.hasNext()) {
			Assert.assertTrue(processedTups.hasNext());
			processedTups.next();
			size++;
		}

		Assert.assertTrue(!processedTups.hasNext());

		Assert.assertEquals(5, size);

	}

	@Test
	public void testThreeSingleNodeIndex() throws Exception {

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q19, null);
		ParsedQuery pq2 = parser.parseQuery(q20, null);

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup1);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);
		List<ExternalTupleSet> indexSet = iep.getNormalizedIndices();
		Assert.assertEquals(3, indexSet.size());

		Iterator<TupleExpr> processedTups = iep.getIndexedTuples();

		int size = 0;

		while (processedTups.hasNext()) {
			Assert.assertTrue(processedTups.hasNext());
			processedTups.next();
			size++;
		}
		Assert.assertTrue(!processedTups.hasNext());

		Assert.assertEquals(3, size);

	}

	@Test
	public void testThreeIndexQuery() throws Exception {

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q16, null);
		ParsedQuery pq2 = parser.parseQuery(q17, null);
		ParsedQuery pq3 = parser.parseQuery(q18, null);

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup2);
		list.add(extTup1);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);
		List<ExternalTupleSet> indexSet = iep.getNormalizedIndices();
		Assert.assertEquals(6, indexSet.size());

		Iterator<TupleExpr> processedTups = iep.getIndexedTuples();

		int size = 0;

		while (processedTups.hasNext()) {
			Assert.assertTrue(processedTups.hasNext());
			processedTups.next();
			size++;
		}

		Assert.assertTrue(!processedTups.hasNext());
		Assert.assertEquals(9, size);

	}

	@Test
	public void testThrowsException1() throws Exception {

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q16, null);
		ParsedQuery pq2 = parser.parseQuery(q17, null);
		ParsedQuery pq3 = parser.parseQuery(q18, null);

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup2);
		list.add(extTup1);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);
		List<ExternalTupleSet> indexSet = iep.getNormalizedIndices();
		Assert.assertEquals(6, indexSet.size());

		Iterator<TupleExpr> processedTups = iep.getIndexedTuples();

		boolean exceptionThrown = false;

		try {
			processedTups.remove();
		} catch (UnsupportedOperationException e) {
			exceptionThrown = true;
		}

		Assert.assertTrue(exceptionThrown);

	}

	@Test
	public void testThrowsException2() throws Exception {

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q19, null);
		ParsedQuery pq2 = parser.parseQuery(q20, null);

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup1);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);
		List<ExternalTupleSet> indexSet = iep.getNormalizedIndices();
		Assert.assertEquals(3, indexSet.size());

		Iterator<TupleExpr> processedTups = iep.getIndexedTuples();

		processedTups.next();
		processedTups.next();
		processedTups.next();

		boolean exceptionThrown = false;
		try {
			processedTups.next();
		} catch (NoSuchElementException e) {
			exceptionThrown = true;
		}

		Assert.assertTrue(exceptionThrown);

	}

	@Test
	public void testThreeIndexQueryFilter() throws Exception {

		SPARQLParser parser = new SPARQLParser();

		ParsedQuery pq1 = parser.parseQuery(q22, null);
		ParsedQuery pq2 = parser.parseQuery(q7, null);
		ParsedQuery pq3 = parser.parseQuery(q21, null);
		ParsedQuery pq4 = parser.parseQuery(q23, null);

		SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
				(Projection) pq2.getTupleExpr());
		SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
				(Projection) pq3.getTupleExpr());
		SimpleExternalTupleSet extTup3 = new SimpleExternalTupleSet(
				(Projection) pq4.getTupleExpr());

		List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

		list.add(extTup2);
		list.add(extTup1);
		list.add(extTup3);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);
		List<ExternalTupleSet> indexSet = iep.getNormalizedIndices();
		Assert.assertEquals(5, indexSet.size());

		Iterator<TupleExpr> processedTups = iep.getIndexedTuples();

		int size = 0;

		while (processedTups.hasNext()) {
			Assert.assertTrue(processedTups.hasNext());
			TupleExpr te = processedTups.next();
			System.out.println(te);
			size++;
		}

		Assert.assertTrue(!processedTups.hasNext());
		Assert.assertEquals(10, size);

	}

}
