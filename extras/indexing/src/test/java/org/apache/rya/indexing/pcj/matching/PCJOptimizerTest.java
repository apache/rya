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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.indexPlanValidator.IndexedExecutionPlanGenerator;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.SimpleExternalTupleSet;
import org.apache.rya.indexing.mongodb.pcj.MongoPcjIndexSetProvider;
import org.apache.rya.indexing.pcj.matching.provider.AbstractPcjIndexSetProvider;
import org.apache.rya.indexing.pcj.matching.provider.AccumuloIndexSetProvider;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.apache.rya.test.mongo.EmbeddedMongoSingleton;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@RunWith(Parameterized.class)
public class PCJOptimizerTest {
    private final AbstractPcjIndexSetProvider provider;

    @Parameterized.Parameters
    public static Collection providers() throws Exception {
        final StatefulMongoDBRdfConfiguration conf = new StatefulMongoDBRdfConfiguration(new Configuration(), EmbeddedMongoSingleton.getNewMongoClient());
        return Lists.<AbstractPcjIndexSetProvider> newArrayList(
                new AccumuloIndexSetProvider(new Configuration()),
                new MongoPcjIndexSetProvider(conf)
                );
    }

    public PCJOptimizerTest(final AbstractPcjIndexSetProvider provider) {
        this.provider = provider;
    }

    @Test
    public void testBasicSegment() throws Exception {

        final String query1 = ""//
                + "SELECT ?e ?c ?l" //
                + "{" //
                + "  ?e a ?c . "//
                + "  OPTIONAL {?e <uri:talksTo> ?l}  . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String query2 = ""//
                + "SELECT ?a ?b ?m" //
                + "{" //
                + "  ?a a ?b . "//
                + "  OPTIONAL {?a <uri:talksTo> ?m}  . "//
                + "}";//

        final SPARQLParser parser = new SPARQLParser();
        final ParsedQuery pq1 = parser.parseQuery(query1, null);
        final ParsedQuery pq2 = parser.parseQuery(query2, null);
        final TupleExpr te1 = pq1.getTupleExpr();
        final TupleExpr te2 = pq2.getTupleExpr();

        final TupleExpr unOpt = te1.clone();
        final List<QueryModelNode> remainingNodes = getNodes(te1);
        final Set<QueryModelNode> unMatchedNodes = new HashSet<>();
        unMatchedNodes.add(remainingNodes.get(2));

        final SimpleExternalTupleSet pcj = new SimpleExternalTupleSet((Projection) te2);
        final List<ExternalTupleSet> externalList = new ArrayList<>();
        externalList.add(pcj);

        provider.setIndices(externalList);
        final PCJOptimizer optimizer = new PCJOptimizer(externalList, false, provider);
        optimizer.optimize(te1, null, null);

        Assert.assertEquals(true, validatePcj(te1, unOpt, externalList, unMatchedNodes));


    }

    @Test
    public void testSegmentWithUnion() throws Exception {

        final String query1 = ""//
                + "SELECT ?e ?c ?l" //
                + "{" //
                + " {?e <uri:p1> <uri:o1>. } UNION { ?e a ?c. OPTIONAL {?e <uri:talksTo> ?l}. ?e <uri:p5> <uri:o4>. ?e <uri:p4> <uri:o3> }  . "//
                + "  ?e <uri:p2> ?c . "//
                + "  ?e <uri:p3> <uri:o2>  . "//
                + "}";//

        final String query2 = ""//
                + "SELECT ?a ?b ?m" //
                + "{" //
                + " ?a <uri:p5> <uri:o4> ." //
                + " ?a <uri:p4> <uri:o3> ." //
                + "  OPTIONAL {?a <uri:talksTo> ?m} . "//
                + "  ?a a ?b . "//
                + "}";//

        final String query3 = ""//
                + "SELECT ?h ?i" //
                + "{" //
                + "  ?h <uri:p2> ?i . "//
                + "  ?h <uri:p3> <uri:o2>  . "//
                + "}";//

        final SPARQLParser parser = new SPARQLParser();
        final ParsedQuery pq1 = parser.parseQuery(query1, null);
        final ParsedQuery pq2 = parser.parseQuery(query2, null);
        final ParsedQuery pq3 = parser.parseQuery(query3, null);
        final TupleExpr te1 = pq1.getTupleExpr();
        final TupleExpr te2 = pq2.getTupleExpr();
        final TupleExpr te3 = pq3.getTupleExpr();

        final TupleExpr unOpt = te1.clone();
        final List<QueryModelNode> remainingNodes = getNodes(te1);
        final Set<QueryModelNode> unMatchedNodes = new HashSet<>();
        unMatchedNodes.add(remainingNodes.get(0));

        final SimpleExternalTupleSet pcj1 = new SimpleExternalTupleSet((Projection) te2);
        final SimpleExternalTupleSet pcj2 = new SimpleExternalTupleSet((Projection) te3);
        final List<ExternalTupleSet> externalList = new ArrayList<>();
        externalList.add(pcj1);
        externalList.add(pcj2);

        provider.setIndices(externalList);
        final PCJOptimizer optimizer = new PCJOptimizer(externalList, false, provider);
        optimizer.optimize(te1, null, null);

        Assert.assertEquals(true, validatePcj(te1, unOpt, externalList, unMatchedNodes));



    }

    @Test
    public void testExactMatchLargeReOrdered() throws Exception {


        final String query1 = ""//
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

        final String query2 = ""//
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

        final SPARQLParser parser = new SPARQLParser();
        final ParsedQuery pq1 = parser.parseQuery(query1, null);
        final ParsedQuery pq2 = parser.parseQuery(query2, null);
        final TupleExpr te1 = pq1.getTupleExpr();
        final TupleExpr te2 = pq2.getTupleExpr();

        final TupleExpr unOpt = te1.clone();

        final SimpleExternalTupleSet pcj = new SimpleExternalTupleSet((Projection) te2);
        final List<ExternalTupleSet> externalList = new ArrayList<>();
        externalList.add(pcj);


        provider.setIndices(externalList);
        final PCJOptimizer optimizer = new PCJOptimizer(externalList, false, provider);
        optimizer.optimize(te1, null, null);

        Assert.assertEquals(true, validatePcj(te1, unOpt, externalList, new HashSet<QueryModelNode>()));
    }

    @Test
    public void testSubsetMatchLargeReOrdered() throws Exception {

        final String query1 = ""//
                + "SELECT ?a ?b ?c ?d ?e ?f ?g ?h" //
                + "{" //
                + "  ?a <uri:p0> ?b ." //
                + "  OPTIONAL{?b <uri:p2> ?c. ?c <uri:p1> ?d} . " //
                + "  OPTIONAL{?b <uri:p3> ?e. ?e <uri:p1> ?f} . "//
                + "  OPTIONAL{?b <uri:p4> ?g. ?g <uri:p1> ?h} . "//
                + "  OPTIONAL{?b <uri:p5> ?i. ?i <uri:p6> ?j} . "//
                + "  OPTIONAL{?b <uri:p5> ?k. ?k <uri:p6> ?l} . "//
                + "  OPTIONAL{?b <uri:p5> ?m. ?m <uri:p6> ?n} . "//
                + "  OPTIONAL{?b <uri:p4> ?o. ?o <uri:p1> ?p} . "//
                + "}";//

        final String query2 = ""//
                + "SELECT ?a ?b ?c ?d ?e ?f ?g ?h" //
                + "{" //
                + "  ?a <uri:p0> ?b ." //
                + "  OPTIONAL{?b <uri:p4> ?o. ?o <uri:p1> ?p} . "//
                + "  OPTIONAL{?b <uri:p4> ?g. ?g <uri:p1> ?h} . "//
                + "  OPTIONAL{?b <uri:p2> ?c. ?c <uri:p1> ?d} . " //
                + "  OPTIONAL{?b <uri:p3> ?e. ?e <uri:p1> ?f} . "//
                + "}";//

        final SPARQLParser parser = new SPARQLParser();
        final ParsedQuery pq1 = parser.parseQuery(query1, null);
        final ParsedQuery pq2 = parser.parseQuery(query2, null);
        final TupleExpr te1 = pq1.getTupleExpr();
        final TupleExpr te2 = pq2.getTupleExpr();

        final TupleExpr unOpt = te1.clone();
        final List<QueryModelNode> remainingNodes = getNodes(te1);
        final Set<QueryModelNode> unMatchedNodes = new HashSet<>();
        unMatchedNodes.add(remainingNodes.get(8));
        unMatchedNodes.add(remainingNodes.get(9));
        unMatchedNodes.add(remainingNodes.get(10));
        unMatchedNodes.add(remainingNodes.get(11));
        unMatchedNodes.add(remainingNodes.get(12));
        unMatchedNodes.add(remainingNodes.get(7));

        final SimpleExternalTupleSet pcj = new SimpleExternalTupleSet((Projection) te2);
        final List<ExternalTupleSet> externalList = new ArrayList<>();
        externalList.add(pcj);

        provider.setIndices(externalList);
        final PCJOptimizer optimizer = new PCJOptimizer(externalList, false, provider);
        optimizer.optimize(te1, null, null);

        Assert.assertEquals(true, validatePcj(te1, unOpt, externalList, unMatchedNodes));
    }

    @Test
    public void testSwitchTwoBoundVars() throws Exception {

        final String query1 = ""//
                + "SELECT ?a ?b ?c " //
                + "{" //
                + "  ?a <uri:p0> ?c ." //
                + "  ?c <uri:p5> <uri:o5> ." //
                + " OPTIONAL{?c <uri:p4> <uri:o4>} ."
                + " ?b<uri:p1> ?c ." //
                + " OPTIONAL{ ?a <uri:p1> ?b } ." //
                + " ?a <uri:p2> <uri:o2>. " //
                + " ?b <uri:p3> <uri:o3> " //
                + "}";//

        final String query2 = ""//
                + "SELECT ?a ?b ?c " //
                + "{" //
                + " ?a <uri:p2> <uri:o2>. " //
                + " ?b <uri:p3> <uri:o3>. " //
                + " OPTIONAL{ ?a <uri:p1> ?b } ." //
                + "  ?a <uri:p0> ?c ." //
                + "  ?b<uri:p1> ?c " //
                + "}";//

        final SPARQLParser parser = new SPARQLParser();
        final ParsedQuery pq1 = parser.parseQuery(query1, null);
        final ParsedQuery pq2 = parser.parseQuery(query2, null);
        final TupleExpr te1 = pq1.getTupleExpr();
        final TupleExpr te2 = pq2.getTupleExpr();

        final TupleExpr unOpt = te1.clone();
        final List<QueryModelNode> remainingNodes = getNodes(te1);
        final Set<QueryModelNode> unMatchedNodes = new HashSet<>();
        unMatchedNodes.add(remainingNodes.get(1));
        unMatchedNodes.add(remainingNodes.get(2));

        final SimpleExternalTupleSet pcj = new SimpleExternalTupleSet((Projection) te2);
        final List<ExternalTupleSet> externalList = new ArrayList<>();
        externalList.add(pcj);

        provider.setIndices(externalList);
        final PCJOptimizer optimizer = new PCJOptimizer(externalList, false, provider);
        optimizer.optimize(te1, null, null);

        Assert.assertEquals(true, validatePcj(te1, unOpt, externalList, unMatchedNodes));
    }

    @Test
    public void testSegmentWithLargeUnion() throws Exception {

        final String query1 = ""//
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

        final String query2 = ""//
                + "SELECT ?a ?b ?c " //
                + "{" //
                + " ?a <uri:p2> <uri:o2>. " //
                + " ?b <uri:p3> <uri:o3>. " //
                + " OPTIONAL{ ?a <uri:p1> ?b } ." //
                + "  ?a <uri:p0> ?c ." //
                + "  ?b<uri:p1> ?c " //
                + "}";//

        final String query3 = ""//
                + "SELECT ?h ?i" //
                + "{" //
                + "  ?h <uri:p2> ?i . "//
                + "  ?h <uri:p3> <uri:o2>  . "//
                + "}";//

        final SPARQLParser parser = new SPARQLParser();
        final ParsedQuery pq1 = parser.parseQuery(query1, null);
        final ParsedQuery pq2 = parser.parseQuery(query2, null);
        final ParsedQuery pq3 = parser.parseQuery(query3, null);
        final TupleExpr te1 = pq1.getTupleExpr();
        final TupleExpr te2 = pq2.getTupleExpr();
        final TupleExpr te3 = pq3.getTupleExpr();

        final TupleExpr unOpt = te1.clone();
        final List<QueryModelNode> remainingNodes = getNodes(te1);
        final Set<QueryModelNode> unMatchedNodes = new HashSet<>();
        unMatchedNodes.add(remainingNodes.get(0));
        unMatchedNodes.add(remainingNodes.get(2));
        unMatchedNodes.add(remainingNodes.get(3));

        final SimpleExternalTupleSet pcj1 = new SimpleExternalTupleSet((Projection) te2);
        final SimpleExternalTupleSet pcj2 = new SimpleExternalTupleSet((Projection) te3);
        final List<ExternalTupleSet> externalList = new ArrayList<>();
        externalList.add(pcj1);
        externalList.add(pcj2);

        provider.setIndices(externalList);
        final PCJOptimizer optimizer = new PCJOptimizer(externalList, false, provider);
        optimizer.optimize(te1, null, null);

        Assert.assertEquals(true, validatePcj(te1, unOpt, externalList, unMatchedNodes));

    }

    @Test
    public void testSegmentWithUnionAndFilters() throws Exception {

        final String query1 = ""//
                + "SELECT ?e ?c ?l" //
                + "{" //
                + " Filter(?e = <uri:s1>) " //
                + " Filter(?c = <uri:s2>) " //
                + " {?e <uri:p1> <uri:o1>. } UNION { ?e a ?c. OPTIONAL {?e <uri:talksTo> ?l}. ?e <uri:p5> <uri:o4>. ?e <uri:p4> <uri:o3> }  . "//
                + "  ?e <uri:p2> ?c . "//
                + "  ?e <uri:p3> <uri:o2>  . "//
                + "}";//

        final String query2 = ""//
                + "SELECT ?a ?b ?m" //
                + "{" //
                + " Filter(?b = <uri:s2>) " //
                + " ?a <uri:p5> <uri:o4> ." //
                + " ?a <uri:p4> <uri:o3> ." //
                + "  OPTIONAL {?a <uri:talksTo> ?m} . "//
                + "  ?a a ?b . "//
                + "}";//

        final String query3 = ""//
                + "SELECT ?h ?i" //
                + "{" //
                + " Filter(?h = <uri:s1>) " //
                + "  ?h <uri:p2> ?i . "//
                + "  ?h <uri:p3> <uri:o2>  . "//
                + "}";//

        final SPARQLParser parser = new SPARQLParser();
        final ParsedQuery pq1 = parser.parseQuery(query1, null);
        final ParsedQuery pq2 = parser.parseQuery(query2, null);
        final ParsedQuery pq3 = parser.parseQuery(query3, null);
        final TupleExpr te1 = pq1.getTupleExpr();
        final TupleExpr te2 = pq2.getTupleExpr();
        final TupleExpr te3 = pq3.getTupleExpr();

        final TupleExpr unOpt = te1.clone();
        final List<QueryModelNode> remainingNodes = getNodes(te1);
        final Set<QueryModelNode> unMatchedNodes = new HashSet<>();
        unMatchedNodes.add(remainingNodes.get(0));

        final SimpleExternalTupleSet pcj1 = new SimpleExternalTupleSet((Projection) te2);
        final SimpleExternalTupleSet pcj2 = new SimpleExternalTupleSet((Projection) te3);
        final List<ExternalTupleSet> externalList = new ArrayList<>();
        externalList.add(pcj1);
        externalList.add(pcj2);

        provider.setIndices(externalList);
        final PCJOptimizer optimizer = new PCJOptimizer(externalList, false, provider);
        optimizer.optimize(te1, null, null);

        Assert.assertEquals(true, validatePcj(te1, unOpt, externalList, unMatchedNodes));

    }

    @Test
    public void testSegmentWithLeftJoinsAndFilters() throws Exception {

        final String query1 = ""//
                + "SELECT ?e ?c ?l" //
                + "{" //
                + " Filter(?e = <uri:s1>) " //
                + " Filter(?c = <uri:s2>) " //
                + " ?e <uri:p1> <uri:o1>. " + " OPTIONAL {?e <uri:p2> ?l}. " + " ?c <uri:p3> <uri:o3>  . "//
                + "  ?c <uri:p4> ?e  . "//
                + " OPTIONAL {?e <uri:p2> ?c } . "//
                + "}";//

        final String query2 = ""//
                + "SELECT ?e ?c ?l" //
                + "{" //
                + " Filter(?c = <uri:s2>) " //
                + " ?e <uri:p1> <uri:o1>. " + " OPTIONAL {?e <uri:p2> ?l}. " + " ?c <uri:p3> <uri:o3>  . "//
                + "}";//

        final String query3 = ""//
                + "SELECT ?e ?c" //
                + "{" //
                + " Filter(?e = <uri:s1>) " //
                + "  ?c <uri:p4> ?e  . "//
                + " OPTIONAL {?e <uri:p2> ?c } . "//
                + "}";//

        final SPARQLParser parser = new SPARQLParser();
        final ParsedQuery pq1 = parser.parseQuery(query1, null);
        final ParsedQuery pq2 = parser.parseQuery(query2, null);
        final ParsedQuery pq3 = parser.parseQuery(query3, null);
        final TupleExpr te1 = pq1.getTupleExpr();
        final TupleExpr te2 = pq2.getTupleExpr();
        final TupleExpr te3 = pq3.getTupleExpr();

        final TupleExpr unOpt = te1.clone();

        final SimpleExternalTupleSet pcj1 = new SimpleExternalTupleSet((Projection) te2);
        final SimpleExternalTupleSet pcj2 = new SimpleExternalTupleSet((Projection) te3);
        final List<ExternalTupleSet> externalList = new ArrayList<>();
        externalList.add(pcj1);
        externalList.add(pcj2);

        provider.setIndices(externalList);
        final PCJOptimizer optimizer = new PCJOptimizer(externalList, false, provider);
        optimizer.optimize(te1, null, null);

        Assert.assertEquals(true, validatePcj(te1, unOpt, externalList, new HashSet<QueryModelNode>()));
    }

    @Test
    public void testJoinMatcherRejectsLeftJoinPcj() throws Exception {

        final String query1 = ""//
                + "SELECT ?e ?c ?l" //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <uri:talksTo> ?l . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String query2 = ""//
                + "SELECT ?a ?b ?m" //
                + "{" //
                + "  ?a a ?b . "//
                + "  ?a <uri:talksTo> ?m . "//
                + "  OPTIONAL {?a <http://www.w3.org/2000/01/rdf-schema#label> ?m}  . "//
                + "}";//

        final SPARQLParser parser = new SPARQLParser();
        final ParsedQuery pq1 = parser.parseQuery(query1, null);
        final ParsedQuery pq2 = parser.parseQuery(query2, null);
        final TupleExpr te1 = pq1.getTupleExpr();
        final TupleExpr te2 = pq2.getTupleExpr();
        final TupleExpr expected = te1.clone();

        final SimpleExternalTupleSet pcj = new SimpleExternalTupleSet((Projection) te2);
        final List<ExternalTupleSet> externalList = new ArrayList<>();
        externalList.add(pcj);

        provider.setIndices(externalList);
        final PCJOptimizer optimizer = new PCJOptimizer(externalList, false, provider);
        optimizer.optimize(te1, null, null);
        Assert.assertEquals(expected, te1);

    }


    private List<QueryModelNode> getNodes(final TupleExpr te) {
        final NodeCollector collector = new NodeCollector();
        te.visit(collector);
        return collector.getNodes();
    }

    private boolean validatePcj(final TupleExpr optTupleExp, final TupleExpr unOptTup, final List<ExternalTupleSet> pcjs, final Set<QueryModelNode> expUnmatchedNodes) {

        final IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
                unOptTup, pcjs);
        final List<ExternalTupleSet> indexList = iep.getNormalizedIndices();
        final Set<QueryModelNode> indexSet = new HashSet<>();
        for(final ExternalTupleSet etup: indexList) {
            indexSet.add(etup);
        }

        final Set<QueryModelNode> tupNodes = Sets.newHashSet(getNodes(optTupleExp));

        final Set<QueryModelNode> diff =  Sets.difference(tupNodes, indexSet);
        return diff.equals(expUnmatchedNodes);
    }


    public static class NodeCollector extends AbstractQueryModelVisitor<RuntimeException> {

        List<QueryModelNode> qNodes = new ArrayList<>();

        @Override
        public void meetNode(final QueryModelNode node) {
            if (node instanceof StatementPattern || node instanceof ExternalTupleSet) {
                qNodes.add(node);
            }
            super.meetNode(node);

        }

        public List<QueryModelNode> getNodes() {
            return qNodes;
        }

    }

}
