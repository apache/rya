/**
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
package org.apache.rya.indexing.IndexPlanValidator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.SimpleExternalTupleSet;
import org.apache.rya.indexing.mongodb.pcj.MongoPcjIndexSetProvider;
import org.apache.rya.indexing.pcj.matching.PCJOptimizer;
import org.apache.rya.indexing.pcj.matching.provider.AbstractPcjIndexSetProvider;
import org.apache.rya.indexing.pcj.matching.provider.AccumuloIndexSetProvider;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.apache.rya.test.mongo.EmbeddedMongoSingleton;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class IndexPlanValidatorTest {
    private final AbstractPcjIndexSetProvider provider;

    @Parameterized.Parameters
    public static Collection providers() throws Exception {
        final StatefulMongoDBRdfConfiguration conf = new StatefulMongoDBRdfConfiguration(new Configuration(), EmbeddedMongoSingleton.getNewMongoClient());
        return Lists.<AbstractPcjIndexSetProvider> newArrayList(
                new AccumuloIndexSetProvider(new Configuration()),
                new MongoPcjIndexSetProvider(conf)
                );
    }

    public IndexPlanValidatorTest(final AbstractPcjIndexSetProvider provider) {
        this.provider = provider;
    }

    @Test
    public void testEvaluateTwoIndexTwoVarOrder1()
            throws Exception {


        final String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String indexSparqlString2 = ""//
                + "SELECT ?e ?o ?l " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final SPARQLParser sp = new SPARQLParser();
        final ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
        final ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);
        final List<ExternalTupleSet> index = Lists.newArrayList();

        final SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
                (Projection) index1.getTupleExpr());
        final SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
                (Projection) index2.getTupleExpr());

        index.add(ais1);
        index.add(ais2);

        final ParsedQuery pq = sp.parseQuery(queryString, null);
        final TupleExpr tup = pq.getTupleExpr().clone();
        provider.setIndices(index);
        final PCJOptimizer pcj = new PCJOptimizer(index, false, provider);
        pcj.optimize(tup, null, null);


        final IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(false, ipv.isValid(tup));

    }

    @Test
    public void testEvaluateTwoIndexTwoVarOrder2()
            throws Exception {


        final String indexSparqlString = ""//
                + "SELECT ?e ?l ?c  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String indexSparqlString2 = ""//
                + "SELECT ?e ?o ?l " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final SPARQLParser sp = new SPARQLParser();
        final ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
        final ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

        final List<ExternalTupleSet> index = Lists.newArrayList();

        final SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
                (Projection) index1.getTupleExpr());
        final SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
                (Projection) index2.getTupleExpr());

        index.add(ais1);
        index.add(ais2);

        final ParsedQuery pq = sp.parseQuery(queryString, null);
        final TupleExpr tup = pq.getTupleExpr().clone();
        provider.setIndices(index);
        final PCJOptimizer pcj = new PCJOptimizer(index, false, provider);
        pcj.optimize(tup, null, null);


        final IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(true, ipv.isValid(tup));

    }

    @Test
    public void testEvaluateTwoIndexTwoVarOrder3()
            throws Exception {


        final String indexSparqlString = ""//
                + "SELECT ?l ?e ?c  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String indexSparqlString2 = ""//
                + "SELECT ?e ?o ?l " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final SPARQLParser sp = new SPARQLParser();
        final ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
        final ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

        final List<ExternalTupleSet> index = Lists.newArrayList();

        final SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
                (Projection) index1.getTupleExpr());
        final SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
                (Projection) index2.getTupleExpr());

        index.add(ais1);
        index.add(ais2);

        final ParsedQuery pq = sp.parseQuery(queryString, null);
        final TupleExpr tup = pq.getTupleExpr().clone();
        provider.setIndices(index);
        final PCJOptimizer pcj = new PCJOptimizer(index, false, provider);
        pcj.optimize(tup, null, null);


        final IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(true, ipv.isValid(tup));

    }

    @Test
    public void testEvaluateTwoIndexTwoVarOrder4()
            throws Exception {


        final String indexSparqlString = ""//
                + "SELECT ?e ?c ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String indexSparqlString2 = ""//
                + "SELECT ?e ?o ?l " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final SPARQLParser sp = new SPARQLParser();
        final ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
        final ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

        final List<ExternalTupleSet> index = Lists.newArrayList();

        final SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
                (Projection) index1.getTupleExpr());
        final SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
                (Projection) index2.getTupleExpr());

        index.add(ais1);
        index.add(ais2);

        final ParsedQuery pq = sp.parseQuery(queryString, null);
        final TupleExpr tup = pq.getTupleExpr().clone();
        provider.setIndices(index);
        final PCJOptimizer pcj = new PCJOptimizer(index, false, provider);
        pcj.optimize(tup, null, null);


        final IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(false, ipv.isValid(tup));

    }


    @Test
    public void testEvaluateTwoIndexTwoVarOrder6()
            throws Exception {


        final String indexSparqlString = ""//
                + "SELECT ?e ?l ?c  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String indexSparqlString2 = ""//
                + "SELECT ?l ?e ?o " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final SPARQLParser sp = new SPARQLParser();
        final ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
        final ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

        final List<ExternalTupleSet> index = Lists.newArrayList();

        final SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
                (Projection) index1.getTupleExpr());
        final SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
                (Projection) index2.getTupleExpr());

        index.add(ais2);
        index.add(ais1);

        final ParsedQuery pq = sp.parseQuery(queryString, null);
        final TupleExpr tup = pq.getTupleExpr().clone();
        provider.setIndices(index);
        final PCJOptimizer pcj = new PCJOptimizer(index, false, provider);
        pcj.optimize(tup, null, null);


        final IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(true, ipv.isValid(tup));

    }

    @Test
    public void testEvaluateTwoIndexCrossProduct1()
            throws Exception {


        final String indexSparqlString = ""//
                + "SELECT ?e ?l ?c  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String indexSparqlString2 = ""//
                + "SELECT ?e ?l ?o " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        final SPARQLParser sp = new SPARQLParser();
        final ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
        final ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);
        final List<ExternalTupleSet> index = Lists.newArrayList();

        final SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
                (Projection) index1.getTupleExpr());
        final SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
                (Projection) index2.getTupleExpr());

        index.add(ais2);
        index.add(ais1);

        final ParsedQuery pq = sp.parseQuery(queryString, null);
        final TupleExpr tup = pq.getTupleExpr().clone();
        provider.setIndices(index);
        final PCJOptimizer pcj = new PCJOptimizer(index, false, provider);
        pcj.optimize(tup, null, null);

        final IndexPlanValidator ipv = new IndexPlanValidator(true);
        Assert.assertEquals(false, ipv.isValid(tup));

    }

    @Test
    public void testEvaluateTwoIndexCrossProduct2()
            throws Exception {


        final String indexSparqlString = ""//
                + "SELECT ?e ?l ?c  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String indexSparqlString2 = ""//
                + "SELECT ?e ?l ?o " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        final SPARQLParser sp = new SPARQLParser();
        final ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
        final ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

        final List<ExternalTupleSet> index = Lists.newArrayList();

        final SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
                (Projection) index1.getTupleExpr());
        final SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
                (Projection) index2.getTupleExpr());

        index.add(ais1);
        index.add(ais2);

        final ParsedQuery pq = sp.parseQuery(queryString, null);
        final TupleExpr tup = pq.getTupleExpr().clone();
        provider.setIndices(index);
        final PCJOptimizer pcj = new PCJOptimizer(index, false, provider);
        pcj.optimize(tup, null, null);

        final IndexPlanValidator ipv = new IndexPlanValidator(true);
        Assert.assertEquals(false, ipv.isValid(tup));

    }

    @Test
    public void testEvaluateTwoIndexCrossProduct3()
            throws Exception {


        final String indexSparqlString = ""//
                + "SELECT ?e ?l ?c  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String indexSparqlString2 = ""//
                + "SELECT ?e ?l ?o " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        final SPARQLParser sp = new SPARQLParser();
        final ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
        final ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

        final List<ExternalTupleSet> index = Lists.newArrayList();

        final SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
                (Projection) index1.getTupleExpr());
        final SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
                (Projection) index2.getTupleExpr());

        index.add(ais1);
        index.add(ais2);

        final ParsedQuery pq = sp.parseQuery(queryString, null);
        final TupleExpr tup = pq.getTupleExpr().clone();
        provider.setIndices(index);
        final PCJOptimizer pcj = new PCJOptimizer(index, false, provider);
        pcj.optimize(tup, null, null);

        final IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(true, ipv.isValid(tup));

    }

    @Test
    public void testEvaluateTwoIndexDiffVars() throws Exception {


        final String indexSparqlString = ""//
                + "SELECT ?chicken ?dog ?pig  " //
                + "{" //
                + "  ?dog a ?chicken . "//
                + "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig "//
                + "}";//

        final String indexSparqlString2 = ""//
                + "SELECT ?fish ?ant ?turkey " //
                + "{" //
                + "  ?fish <uri:talksTo> ?turkey . "//
                + "  ?turkey <http://www.w3.org/2000/01/rdf-schema#label> ?ant "//
                + "}";//

        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        final SPARQLParser sp = new SPARQLParser();
        final ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
        final ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

        final List<ExternalTupleSet> index = Lists.newArrayList();

        final SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
                (Projection) index1.getTupleExpr());
        final SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
                (Projection) index2.getTupleExpr());

        index.add(ais1);
        index.add(ais2);

        final ParsedQuery pq = sp.parseQuery(queryString, null);
        final TupleExpr tup = pq.getTupleExpr().clone();
        provider.setIndices(index);
        final PCJOptimizer pcj = new PCJOptimizer(index, false, provider);
        pcj.optimize(tup, null, null);

        final IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(false, ipv.isValid(tup));

    }

    @Test
    public void testEvaluateTwoIndexDiffVars2() throws Exception {


        final String indexSparqlString = ""//
                + "SELECT ?dog ?pig ?chicken  " //
                + "{" //
                + "  ?dog a ?chicken . "//
                + "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig "//
                + "}";//

        final String indexSparqlString2 = ""//
                + "SELECT ?fish ?ant ?turkey " //
                + "{" //
                + "  ?fish <uri:talksTo> ?turkey . "//
                + "  ?turkey <http://www.w3.org/2000/01/rdf-schema#label> ?ant "//
                + "}";//

        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        final SPARQLParser sp = new SPARQLParser();
        final ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
        final ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

        final List<ExternalTupleSet> index = Lists.newArrayList();

        final SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
                (Projection) index1.getTupleExpr());
        final SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
                (Projection) index2.getTupleExpr());

        index.add(ais1);
        index.add(ais2);

        final ParsedQuery pq = sp.parseQuery(queryString, null);
        final TupleExpr tup = pq.getTupleExpr().clone();
        provider.setIndices(index);
        final PCJOptimizer pcj = new PCJOptimizer(index, false, provider);
        pcj.optimize(tup, null, null);

        final IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(true, ipv.isValid(tup));

    }

    @Test
    public void testEvaluateTwoIndexDiffVars3() throws Exception {


        final String indexSparqlString = ""//
                + "SELECT ?pig ?dog ?chicken  " //
                + "{" //
                + "  ?dog a ?chicken . "//
                + "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig "//
                + "}";//

        final String indexSparqlString2 = ""//
                + "SELECT ?fish ?ant ?turkey " //
                + "{" //
                + "  ?fish <uri:talksTo> ?turkey . "//
                + "  ?turkey <http://www.w3.org/2000/01/rdf-schema#label> ?ant "//
                + "}";//

        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        final SPARQLParser sp = new SPARQLParser();
        final ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
        final ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

        final List<ExternalTupleSet> index = Lists.newArrayList();

        final SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
                (Projection) index1.getTupleExpr());
        final SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
                (Projection) index2.getTupleExpr());

        index.add(ais1);
        index.add(ais2);

        final ParsedQuery pq = sp.parseQuery(queryString, null);
        final TupleExpr tup = pq.getTupleExpr().clone();
        provider.setIndices(index);
        final PCJOptimizer pcj = new PCJOptimizer(index, false, provider);
        pcj.optimize(tup, null, null);

        final IndexPlanValidator ipv = new IndexPlanValidator(false);
        Assert.assertEquals(true, ipv.isValid(tup));

    }

    @Test
    public void testEvaluateTwoIndexDiffVarsDirProd()
            throws Exception {


        final String indexSparqlString = ""//
                + "SELECT ?pig ?dog ?chicken  " //
                + "{" //
                + "  ?dog a ?chicken . "//
                + "  ?dog <http://www.w3.org/2000/01/rdf-schema#label> ?pig "//
                + "}";//

        final String indexSparqlString2 = ""//
                + "SELECT ?fish ?ant ?turkey " //
                + "{" //
                + "  ?fish <uri:talksTo> ?turkey . "//
                + "  ?turkey <http://www.w3.org/2000/01/rdf-schema#label> ?ant "//
                + "}";//

        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o ?f ?g " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  ?f <uri:talksTo> ?g . " //
                + "}";//

        final SPARQLParser sp = new SPARQLParser();
        final ParsedQuery index1 = sp.parseQuery(indexSparqlString, null);
        final ParsedQuery index2 = sp.parseQuery(indexSparqlString2, null);

        final List<ExternalTupleSet> index = Lists.newArrayList();

        final SimpleExternalTupleSet ais1 = new SimpleExternalTupleSet(
                (Projection) index1.getTupleExpr());
        final SimpleExternalTupleSet ais2 = new SimpleExternalTupleSet(
                (Projection) index2.getTupleExpr());

        index.add(ais1);
        index.add(ais2);

        final ParsedQuery pq = sp.parseQuery(queryString, null);
        final TupleExpr tup = pq.getTupleExpr().clone();
        provider.setIndices(index);
        final PCJOptimizer pcj = new PCJOptimizer(index, false, provider);
        pcj.optimize(tup, null, null);

        final IndexPlanValidator ipv = new IndexPlanValidator(true);
        Assert.assertEquals(false, ipv.isValid(tup));

    }

    @Test
    public void testValidTupleIterator() throws Exception {

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

        list.add(extTup2);
        list.add(extTup1);
        list.add(extTup3);

        final IndexedExecutionPlanGenerator iep = new IndexedExecutionPlanGenerator(
                pq1.getTupleExpr(), list);

        final Iterator<TupleExpr> plans = new TupleExecutionPlanGenerator()
                .getPlans(iep.getIndexedTuples());
        final IndexPlanValidator ipv = new IndexPlanValidator(true);
        final Iterator<TupleExpr> validPlans = ipv.getValidTuples(plans);

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
