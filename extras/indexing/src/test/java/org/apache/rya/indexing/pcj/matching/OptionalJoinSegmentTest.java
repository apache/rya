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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rya.indexing.external.matching.QuerySegment;
import org.apache.rya.indexing.external.matching.QuerySegmentFactory;
import org.apache.rya.indexing.external.matching.TopOfQueryFilterRelocator;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.SimpleExternalTupleSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Assert;
import org.junit.Test;

public class OptionalJoinSegmentTest {

    private final QuerySegmentFactory<ExternalTupleSet> qFactory = new QuerySegmentFactory<ExternalTupleSet>();

	@Test
	public void testBasicOptional() throws MalformedQueryException {

		String query1 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  OPTIONAL{?e <uri:talksTo> ?l } . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		String query2 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  OPTIONAL{?e <uri:talksTo> ?l } . "//
				+ "}";//

		SPARQLParser parser = new SPARQLParser();
		ParsedQuery pq1 = parser.parseQuery(query1, null);
		ParsedQuery pq2 = parser.parseQuery(query2, null);
		TupleExpr te1 = pq1.getTupleExpr();
		TupleExpr te2 = pq2.getTupleExpr();
		Join join = (Join) ((Projection) te1).getArg();
		LeftJoin lj = (LeftJoin) ((Projection) te2).getArg();

		QuerySegment<ExternalTupleSet> seg1 = qFactory.getQuerySegment(join);
        QuerySegment<ExternalTupleSet> seg2 = qFactory.getQuerySegment(lj);


		Assert.assertEquals(true, seg1.containsQuerySegment(seg2));
		Assert.assertEquals(join, seg1.getQuery().getTupleExpr());

		SimpleExternalTupleSet pcj = new SimpleExternalTupleSet((Projection)te2);
		List<QueryModelNode> nodes = seg1.getOrderedNodes();
		QueryModelNode node = nodes.get(0);
		seg1.replaceWithExternalSet(seg2, pcj);

		Set<QueryModelNode> nodeSet = new HashSet<>();
		nodeSet.add(node);
		nodeSet.add(pcj);

		Assert.assertEquals(nodeSet, seg1.getUnOrderedNodes());

	}


	@Test
	public void testBasicOptionalWithFilter() throws Exception {

		String query1 = ""//
				+ "SELECT ?e ?c ?l" //
				+ "{" //
				+ " Filter(?e = <uri:Bob>)" //
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e a ?c . "//
				+ "  OPTIONAL{?e <uri:talksTo> ?l } . "//
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
		Filter filter1 = (Filter) ((Projection) te1).getArg();
		Filter filter2 = (Filter) ((Projection) te2).getArg();

		QuerySegment<ExternalTupleSet> seg1 = qFactory.getQuerySegment(filter1);
        QuerySegment<ExternalTupleSet> seg2 = qFactory.getQuerySegment(filter2);

		Assert.assertEquals(filter1, seg1.getQuery().getTupleExpr());
		Assert.assertEquals(true, seg1.containsQuerySegment(seg2));

		SimpleExternalTupleSet pcj = new SimpleExternalTupleSet((Projection)te2);
		List<QueryModelNode> nodes = seg1.getOrderedNodes();
		QueryModelNode node = nodes.get(3);
		seg1.replaceWithExternalSet(seg2, pcj);

		Set<QueryModelNode> nodeSet = new HashSet<>();
		nodeSet.add(node);
		nodeSet.add(pcj);

		Assert.assertEquals(nodeSet, seg1.getUnOrderedNodes());

	}




}
