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
package org.apache.rya.indexing.external;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.external.PrecompJoinOptimizerIT.CountingResultHandler;
import org.apache.rya.indexing.external.PrecompJoinOptimizerTest.NodeCollector;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.external.tupleSet.SimpleExternalTupleSet;
import org.apache.rya.indexing.pcj.matching.PCJOptimizer;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.algebra.Projection;
import org.eclipse.rdf4j.query.algebra.QueryModelNode;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.parser.ParsedQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLParser;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

public class PCJOptionalTestIT {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    private SailRepositoryConnection conn, pcjConn;
    private SailRepository repo, pcjRepo;
    private Connector accCon;
    String tablePrefix = "table_";
    IRI sub, sub2, obj, obj2, subclass, subclass2, talksTo, sub3, subclass3;

    @Before
    public void init() throws RepositoryException,
            TupleQueryResultHandlerException, QueryEvaluationException,
            MalformedQueryException, AccumuloException,
            AccumuloSecurityException, TableExistsException, RyaDAOException,
            TableNotFoundException, InferenceEngineException, NumberFormatException,
            UnknownHostException, SailException {

        repo = PcjIntegrationTestingUtil.getNonPcjRepo(tablePrefix, "instance");
        conn = repo.getConnection();

        pcjRepo = PcjIntegrationTestingUtil.getPcjRepo(tablePrefix, "instance");
        pcjConn = pcjRepo.getConnection();

        sub = VF.createIRI("uri:entity");
        subclass = VF.createIRI("uri:class");
        obj = VF.createIRI("uri:obj");
        talksTo = VF.createIRI("uri:talksTo");

        conn.add(sub, RDF.TYPE, subclass);
        conn.add(sub, RDFS.LABEL, VF.createLiteral("label"));
        conn.add(sub, talksTo, obj);

        sub2 = VF.createIRI("uri:entity2");
        subclass2 = VF.createIRI("uri:class2");
        obj2 = VF.createIRI("uri:obj2");
        sub3 = VF.createIRI("uri:entity3");
        subclass3 = VF.createIRI("uri:class3");


        conn.add(sub2, RDF.TYPE, subclass2);
        conn.add(sub2, RDFS.LABEL, VF.createLiteral("label2"));
        conn.add(sub2, talksTo, obj2);
        conn.add(sub3, RDF.TYPE, subclass3);
        conn.add(sub3, RDFS.LABEL, VF.createLiteral("label3"));


        accCon = new MockInstance("instance").getConnector("root",
                new PasswordToken(""));

    }

    @After
    public void close() throws RepositoryException, AccumuloException,
            AccumuloSecurityException, TableNotFoundException {

        PcjIntegrationTestingUtil.closeAndShutdown(conn, repo);
        PcjIntegrationTestingUtil.closeAndShutdown(pcjConn, pcjRepo);
        PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, tablePrefix);
        PcjIntegrationTestingUtil.deleteIndexTables(accCon, 2, tablePrefix);

    }


    @Test
    public void testEvaluateSingeIndexExactMatch()
            throws TupleQueryResultHandlerException, QueryEvaluationException,
            MalformedQueryException, RepositoryException, AccumuloException,
            AccumuloSecurityException, TableExistsException, RyaDAOException,
            SailException, TableNotFoundException, PcjException, InferenceEngineException {

        final String indexSparqlString = ""//
                + "SELECT ?e ?c ?l ?o" //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "  OPTIONAL{?e <uri:talksTo> ?o } . "//
                + "}";//

        PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablePrefix
                + "INDEX_1", indexSparqlString, new String[] { "e", "c", "l", "o" },
                Optional.absent());
        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  OPTIONAL {?e <uri:talksTo> ?o } . "//
                + "}";//
        final CountingResultHandler crh = new CountingResultHandler();
        PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, tablePrefix);
        PcjIntegrationTestingUtil.closeAndShutdown(conn, repo);
        final TupleQuery tupQuery = pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        tupQuery.evaluate(crh);

        Assert.assertEquals(3, crh.getCount());

    }



    @Test
    public void testEvaluateSingeIndex()
            throws TupleQueryResultHandlerException, QueryEvaluationException,
            MalformedQueryException, RepositoryException, AccumuloException,
            AccumuloSecurityException, TableExistsException, RyaDAOException,
            SailException, TableNotFoundException, PcjException, InferenceEngineException,
            NumberFormatException, UnknownHostException {

        final String indexSparqlString = ""//
                + "SELECT ?e ?l ?o" //
                + "{" //
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "  OPTIONAL{?e <uri:talksTo> ?o } . "//
                + "}";//

        PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablePrefix
                + "INDEX_1", indexSparqlString, new String[] { "e", "l", "o" },
                Optional.absent());
        final String queryString = ""//
                + "SELECT ?e ?c ?l ?o " //
                + "{" //
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
                + "  OPTIONAL {?e <uri:talksTo> ?o } . "//
                + "  ?e a ?c . "//
                + "}";//

        final CountingResultHandler crh = new CountingResultHandler();
        PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, tablePrefix);
        PcjIntegrationTestingUtil.closeAndShutdown(conn, repo);

        repo = PcjIntegrationTestingUtil.getPcjRepo(tablePrefix, "instance");
        conn = repo.getConnection();
        conn.add(sub, RDF.TYPE, subclass);
        conn.add(sub2, RDF.TYPE, subclass2);
        conn.add(sub3, RDF.TYPE, subclass3);


        final TupleQuery tupQuery = pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
        tupQuery.evaluate(crh);

        Assert.assertEquals(3, crh.getCount());

    }






    @Test
    public void testSimpleOptionalTest1() throws Exception {

        final String query = ""//
                + "SELECT ?u ?s ?t " //
                + "{" //
                + "  ?s a ?t ."//
                + "  OPTIONAL{?t <http://www.w3.org/2000/01/rdf-schema#label> ?u } ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "}";//

        final SPARQLParser parser = new SPARQLParser();

        final ParsedQuery pq1 = parser.parseQuery(query, null);

        final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
                (Projection) pq1.getTupleExpr().clone());

        final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

        list.add(extTup1);

        final List<QueryModelNode> optTupNodes = Lists.newArrayList();
        optTupNodes.add(extTup1);

        final PCJOptimizer pcj = new PCJOptimizer(list, true);
        final TupleExpr te = pq1.getTupleExpr();
        pcj.optimize(te, null, null);

        final NodeCollector nc = new NodeCollector();
        te.visit(nc);

        final List<QueryModelNode> qNodes = nc.getNodes();

        Assert.assertEquals(qNodes.size(), optTupNodes.size());
        for (final QueryModelNode node : qNodes) {
            Assert.assertTrue(optTupNodes.contains(node));
        }

    }


    @Test
    public void testSimpleOptionalTest2() throws Exception {

        final String query = ""//
                + "SELECT ?u ?s ?t " //
                + "{" //
                + "  ?s a ?t ."//
                + "  OPTIONAL{?t <http://www.w3.org/2000/01/rdf-schema#label> ?u } ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "  ?s a ?u ."//
                + "}";//


        final String pcj = ""//
                + "SELECT ?d ?b ?c " //
                + "{" //
                + "  ?b a ?c ."//
                + "  OPTIONAL{?c <http://www.w3.org/2000/01/rdf-schema#label> ?d } ."//
                + "  ?d <uri:talksTo> ?b . "//
                + "}";//


        final String relabel_pcj = ""//
                + "SELECT ?u ?s ?t " //
                + "{" //
                + "  ?s a ?t ."//
                + "  OPTIONAL{?t <http://www.w3.org/2000/01/rdf-schema#label> ?u } ."//
                + "  ?u <uri:talksTo> ?s . "//
                + "}";//


        final SPARQLParser parser = new SPARQLParser();

        final ParsedQuery pq1 = parser.parseQuery(query, null);
        final ParsedQuery pq2 = parser.parseQuery(pcj, null);
        final ParsedQuery pq3 = parser.parseQuery(relabel_pcj, null);

        final SimpleExternalTupleSet extTup1 = new SimpleExternalTupleSet(
                (Projection) pq2.getTupleExpr());
        final SimpleExternalTupleSet extTup2 = new SimpleExternalTupleSet(
                (Projection) pq3.getTupleExpr());

        final List<ExternalTupleSet> list = new ArrayList<ExternalTupleSet>();

        list.add(extTup1);

        final List<QueryModelNode> optTupNodes = Lists.newArrayList();
        optTupNodes.add(extTup2);

        final PCJOptimizer opt = new PCJOptimizer(list, true);
        final TupleExpr te = pq1.getTupleExpr();
        opt.optimize(te, null, null);

        final NodeCollector nc = new NodeCollector();
        te.visit(nc);

        final List<QueryModelNode> qNodes = nc.getNodes();

        Assert.assertEquals(qNodes.size(), optTupNodes.size() + 1);
        for (final QueryModelNode node : optTupNodes) {
            Assert.assertTrue(qNodes.contains(node));
        }

    }
}
