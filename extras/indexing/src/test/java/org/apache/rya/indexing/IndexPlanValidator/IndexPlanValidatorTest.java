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
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

import com.google.common.collect.Lists;

public class IndexPlanValidatorTest {

	@Test
	public void testEvaluateTwoIndexTwoVarOrder1()
			throws MalformedQueryException {


		String indexSparqlString = ""//
				+ "SELECT ?c ?e ?l  " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String indexSparqlString2 = ""//
				+ "SELECT ?e ?o ?l " //
				+ "{" //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String queryString = ""//
				+ "SELECT ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		SPARQLParser sp = new SPARQLParser();
		ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
		ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);
		List<ExternalTupleSet> index = Lists.newArrayList();

		SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
				(Projection) index1.getTupleExpr());
		SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
				(Projection) index2.getTupleExpr());

		index.add(ais1);
		index.add(ais2);

		ParsedQuery pq = sp.parseQuery(queryString, null);
		TupleExpr tup = pq.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(index, false);
		pcj.optimize(tup, null, null);


		IndexPlanValidator ipv = new IndexPlanValidator(false);
		Assert.assertEquals(false, ipv.isValid(tup));

	}

	@Test
	public void testEvaluateTwoIndexTwoVarOrder2()
			throws MalformedQueryException {


		String indexSparqlString = ""//
				+ "SELECT ?e ?l ?c  " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String indexSparqlString2 = ""//
				+ "SELECT ?e ?o ?l " //
				+ "{" //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String queryString = ""//
				+ "SELECT ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		SPARQLParser sp = new SPARQLParser();
		ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
		ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

		List<ExternalTupleSet> index = Lists.newArrayList();

		SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
				(Projection) index1.getTupleExpr());
		SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
				(Projection) index2.getTupleExpr());

		index.add(ais1);
		index.add(ais2);

		ParsedQuery pq = sp.parseQuery(queryString, null);
		TupleExpr tup = pq.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(index, false);
		pcj.optimize(tup, null, null);


		IndexPlanValidator ipv = new IndexPlanValidator(false);
		Assert.assertEquals(true, ipv.isValid(tup));

	}

	@Test
	public void testEvaluateTwoIndexTwoVarOrder3()
			throws MalformedQueryException {


		String indexSparqlString = ""//
				+ "SELECT ?l ?e ?c  " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String indexSparqlString2 = ""//
				+ "SELECT ?e ?o ?l " //
				+ "{" //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String queryString = ""//
				+ "SELECT ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		SPARQLParser sp = new SPARQLParser();
		ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
		ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

		List<ExternalTupleSet> index = Lists.newArrayList();

		SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
				(Projection) index1.getTupleExpr());
		SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
				(Projection) index2.getTupleExpr());

		index.add(ais1);
		index.add(ais2);

		ParsedQuery pq = sp.parseQuery(queryString, null);
		TupleExpr tup = pq.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(index, false);
		pcj.optimize(tup, null, null);


		IndexPlanValidator ipv = new IndexPlanValidator(false);
		Assert.assertEquals(true, ipv.isValid(tup));

	}

	@Test
	public void testEvaluateTwoIndexTwoVarOrder4()
			throws MalformedQueryException {


		String indexSparqlString = ""//
				+ "SELECT ?e ?c ?l  " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String indexSparqlString2 = ""//
				+ "SELECT ?e ?o ?l " //
				+ "{" //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String queryString = ""//
				+ "SELECT ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		SPARQLParser sp = new SPARQLParser();
		ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
		ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

		List<ExternalTupleSet> index = Lists.newArrayList();

		SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
				(Projection) index1.getTupleExpr());
		SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
				(Projection) index2.getTupleExpr());

		index.add(ais1);
		index.add(ais2);

		ParsedQuery pq = sp.parseQuery(queryString, null);
		TupleExpr tup = pq.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(index, false);
		pcj.optimize(tup, null, null);


		IndexPlanValidator ipv = new IndexPlanValidator(false);
		Assert.assertEquals(false, ipv.isValid(tup));

	}


	@Test
	public void testEvaluateTwoIndexTwoVarOrder6()
			throws MalformedQueryException {


		String indexSparqlString = ""//
				+ "SELECT ?e ?l ?c  " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String indexSparqlString2 = ""//
				+ "SELECT ?l ?e ?o " //
				+ "{" //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String queryString = ""//
				+ "SELECT ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		SPARQLParser sp = new SPARQLParser();
		ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
		ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

		List<ExternalTupleSet> index = Lists.newArrayList();

		SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
				(Projection) index1.getTupleExpr());
		SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
				(Projection) index2.getTupleExpr());

		index.add(ais2);
		index.add(ais1);

		ParsedQuery pq = sp.parseQuery(queryString, null);
		TupleExpr tup = pq.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(index, false);
		pcj.optimize(tup, null, null);


		IndexPlanValidator ipv = new IndexPlanValidator(false);
		Assert.assertEquals(true, ipv.isValid(tup));

	}

	@Test
	public void testEvaluateTwoIndexCrossProduct1()
			throws MalformedQueryException {


		String indexSparqlString = ""//
				+ "SELECT ?e ?l ?c  " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String indexSparqlString2 = ""//
				+ "SELECT ?e ?l ?o " //
				+ "{" //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String queryString = ""//
				+ "SELECT ?e ?c ?l ?o ?f ?g " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?f <uri:talksTo> ?g . " //
				+ "}";//

		SPARQLParser sp = new SPARQLParser();
		ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
		ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);
		List<ExternalTupleSet> index = Lists.newArrayList();

		SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
				(Projection) index1.getTupleExpr());
		SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
				(Projection) index2.getTupleExpr());

		index.add(ais2);
		index.add(ais1);

		ParsedQuery pq = sp.parseQuery(queryString, null);
		TupleExpr tup = pq.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(index, false);
		pcj.optimize(tup, null, null);

		IndexPlanValidator ipv = new IndexPlanValidator(true);
		Assert.assertEquals(false, ipv.isValid(tup));

	}

	@Test
	public void testEvaluateTwoIndexCrossProduct2()
			throws MalformedQueryException {


		String indexSparqlString = ""//
				+ "SELECT ?e ?l ?c  " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String indexSparqlString2 = ""//
				+ "SELECT ?e ?l ?o " //
				+ "{" //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String queryString = ""//
				+ "SELECT ?e ?c ?l ?o ?f ?g " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?f <uri:talksTo> ?g . " //
				+ "}";//

		SPARQLParser sp = new SPARQLParser();
		ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
		ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

		List<ExternalTupleSet> index = Lists.newArrayList();

		SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
				(Projection) index1.getTupleExpr());
		SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
				(Projection) index2.getTupleExpr());

		index.add(ais1);
		index.add(ais2);

		ParsedQuery pq = sp.parseQuery(queryString, null);
		TupleExpr tup = pq.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(index, false);
		pcj.optimize(tup, null, null);

		IndexPlanValidator ipv = new IndexPlanValidator(true);
		Assert.assertEquals(false, ipv.isValid(tup));

	}

	@Test
	public void testEvaluateTwoIndexCrossProduct3()
			throws MalformedQueryException {


		String indexSparqlString = ""//
				+ "SELECT ?e ?l ?c  " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String indexSparqlString2 = ""//
				+ "SELECT ?e ?l ?o " //
				+ "{" //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String queryString = ""//
				+ "SELECT ?e ?c ?l ?o ?f ?g " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?f <uri:talksTo> ?g . " //
				+ "}";//

		SPARQLParser sp = new SPARQLParser();
		ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
		ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

		List<ExternalTupleSet> index = Lists.newArrayList();

		SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
				(Projection) index1.getTupleExpr());
		SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
				(Projection) index2.getTupleExpr());

		index.add(ais1);
		index.add(ais2);

		ParsedQuery pq = sp.parseQuery(queryString, null);
		TupleExpr tup = pq.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(index, false);
		pcj.optimize(tup, null, null);

		IndexPlanValidator ipv = new IndexPlanValidator(false);
		Assert.assertEquals(true, ipv.isValid(tup));

	}

	@Test
	public void testEvaluateTwoIndexDiffVars() throws MalformedQueryException {


		String indexSparqlString = ""//
				+ "SELECT ?chicken ?dog ?pig  " //
				+ "{" //
				+ "  ?dog a ?chicken . "//
				+ "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig "//
				+ "}";//

		String indexSparqlString2 = ""//
				+ "SELECT ?fish ?ant ?turkey " //
				+ "{" //
				+ "  ?fish <uri:talksTo> ?turkey . "//
				+ "  ?turkey <http://www.w3.org/2000/01/rdf-schema#label> ?ant "//
				+ "}";//

		String queryString = ""//
				+ "SELECT ?e ?c ?l ?o ?f ?g " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?f <uri:talksTo> ?g . " //
				+ "}";//

		SPARQLParser sp = new SPARQLParser();
		ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
		ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

		List<ExternalTupleSet> index = Lists.newArrayList();

		SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
				(Projection) index1.getTupleExpr());
		SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
				(Projection) index2.getTupleExpr());

		index.add(ais1);
		index.add(ais2);

		ParsedQuery pq = sp.parseQuery(queryString, null);
		TupleExpr tup = pq.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(index, false);
		pcj.optimize(tup, null, null);

		IndexPlanValidator ipv = new IndexPlanValidator(false);
		Assert.assertEquals(false, ipv.isValid(tup));

	}

	@Test
	public void testEvaluateTwoIndexDiffVars2() throws MalformedQueryException {


		String indexSparqlString = ""//
				+ "SELECT ?dog ?pig ?chicken  " //
				+ "{" //
				+ "  ?dog a ?chicken . "//
				+ "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig "//
				+ "}";//

		String indexSparqlString2 = ""//
				+ "SELECT ?fish ?ant ?turkey " //
				+ "{" //
				+ "  ?fish <uri:talksTo> ?turkey . "//
				+ "  ?turkey <http://www.w3.org/2000/01/rdf-schema#label> ?ant "//
				+ "}";//

		String queryString = ""//
				+ "SELECT ?e ?c ?l ?o ?f ?g " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?f <uri:talksTo> ?g . " //
				+ "}";//

		SPARQLParser sp = new SPARQLParser();
		ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
		ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

		List<ExternalTupleSet> index = Lists.newArrayList();

		SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
				(Projection) index1.getTupleExpr());
		SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
				(Projection) index2.getTupleExpr());

		index.add(ais1);
		index.add(ais2);

		ParsedQuery pq = sp.parseQuery(queryString, null);
		TupleExpr tup = pq.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(index, false);
		pcj.optimize(tup, null, null);

		IndexPlanValidator ipv = new IndexPlanValidator(false);
		Assert.assertEquals(true, ipv.isValid(tup));

	}

	@Test
	public void testEvaluateTwoIndexDiffVars3() throws MalformedQueryException {


		String indexSparqlString = ""//
				+ "SELECT ?pig ?dog ?chicken  " //
				+ "{" //
				+ "  ?dog a ?chicken . "//
				+ "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig "//
				+ "}";//

		String indexSparqlString2 = ""//
				+ "SELECT ?fish ?ant ?turkey " //
				+ "{" //
				+ "  ?fish <uri:talksTo> ?turkey . "//
				+ "  ?turkey <http://www.w3.org/2000/01/rdf-schema#label> ?ant "//
				+ "}";//

		String queryString = ""//
				+ "SELECT ?e ?c ?l ?o ?f ?g " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?f <uri:talksTo> ?g . " //
				+ "}";//

		SPARQLParser sp = new SPARQLParser();
		ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
		ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

		List<ExternalTupleSet> index = Lists.newArrayList();

		SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
				(Projection) index1.getTupleExpr());
		SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
				(Projection) index2.getTupleExpr());

		index.add(ais1);
		index.add(ais2);

		ParsedQuery pq = sp.parseQuery(queryString, null);
		TupleExpr tup = pq.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(index, false);
		pcj.optimize(tup, null, null);

		IndexPlanValidator ipv = new IndexPlanValidator(false);
		Assert.assertEquals(true, ipv.isValid(tup));

	}

	@Test
	public void testEvaluateTwoIndexDiffVarsDirProd()
			throws MalformedQueryException {


		String indexSparqlString = ""//
				+ "SELECT ?pig ?dog ?chicken  " //
				+ "{" //
				+ "  ?dog a ?chicken . "//
				+ "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig "//
				+ "}";//

		String indexSparqlString2 = ""//
				+ "SELECT ?fish ?ant ?turkey " //
				+ "{" //
				+ "  ?fish <uri:talksTo> ?turkey . "//
				+ "  ?turkey <http://www.w3.org/2000/01/rdf-schema#label> ?ant "//
				+ "}";//

		String queryString = ""//
				+ "SELECT ?e ?c ?l ?o ?f ?g " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?f <uri:talksTo> ?g . " //
				+ "}";//

		SPARQLParser sp = new SPARQLParser();
		ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
		ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

		List<ExternalTupleSet> index = Lists.newArrayList();

		SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
				(Projection) index1.getTupleExpr());
		SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
				(Projection) index2.getTupleExpr());

		index.add(ais1);
		index.add(ais2);

		ParsedQuery pq = sp.parseQuery(queryString, null);
		TupleExpr tup = pq.getTupleExpr().clone();
		PCJOptimizer pcj = new PCJOptimizer(index, false);
		pcj.optimize(tup, null, null);

		IndexPlanValidator ipv = new IndexPlanValidator(true);
		Assert.assertEquals(false, ipv.isValid(tup));

	}

	@Test
	public void testValidTupleIterator() throws Exception {

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

		list.add(extTup2);
		list.add(extTup1);
		list.add(extTup3);

		IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
				pq1.getTupleExpr(), list);

		Iterator<TupleExpr> plans = new TupleExecutionPlanGenerator()
				.getPlans(iep.getIndexedTuples());
		IndexPlanValidator ipv = new IndexPlanValidator(true);
		Iterator<TupleExpr> validPlans = ipv.getValidTuples(plans);

		int size = 0;

		while (validPlans.hasNext()) {
			Assert.assertTrue(validPlans.hasNext());
			validPlans.next();
			size++;
		}

		Assert.assertTrue(!validPlans.hasNext());
		Assert.assertEquals(732, size);

	}

}
