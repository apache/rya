package mvm.rya.indexing.external;

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


import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;
import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.indexing.RyaSailFactory;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.external.tupleSet.AccumuloIndexSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

public class AccumuloIndexSetTest2 {

    private SailRepositoryConnection conn;
    private SailRepository repo;
    private Connector accCon;
    String tablePrefix = "table_";
    AccumuloRdfConfiguration conf;
    URI sub, sub2, obj, obj2, subclass, subclass2, talksTo;

    @Before
    public void init() throws RepositoryException, TupleQueryResultHandlerException, QueryEvaluationException,
            MalformedQueryException, AccumuloException, AccumuloSecurityException, TableExistsException,
            RyaDAOException {

        conf = new AccumuloRdfConfiguration();
        conf.set(ConfigUtils.USE_PCJ, "true");
        conf.set(ConfigUtils.USE_MOCK_INSTANCE, "true");
        conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "instance");
        conf.setTablePrefix(tablePrefix);
        conf.setPcjTables(Arrays.asList("table1", "table2"));

        Sail sail = RyaSailFactory.getInstance(conf);
        repo = new SailRepository(sail);
        repo.initialize();
        conn = repo.getConnection();

        sub = new URIImpl("uri:entity");
        subclass = new URIImpl("uri:class");
        obj = new URIImpl("uri:obj");
        talksTo = new URIImpl("uri:talksTo");

        conn.add(sub, RDF.TYPE, subclass);
        conn.add(sub, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(sub, talksTo, obj);

        sub2 = new URIImpl("uri:entity2");
        subclass2 = new URIImpl("uri:class2");
        obj2 = new URIImpl("uri:obj2");

        conn.add(sub2, RDF.TYPE, subclass2);
        conn.add(sub2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(sub2, talksTo, obj2);

        accCon = new MockInstance("instance").getConnector("root", new PasswordToken("".getBytes()));
        accCon.tableOperations().create("table1");
        accCon.tableOperations().create("table2");

    }

    @After
    public void close() throws RepositoryException, AccumuloException, AccumuloSecurityException,
            TableNotFoundException {

        conf = null;
        conn.close();
        accCon.tableOperations().delete(tablePrefix + "spo");
        accCon.tableOperations().delete(tablePrefix + "po");
        accCon.tableOperations().delete(tablePrefix + "osp");

        if (accCon.tableOperations().exists("table1")) {
            accCon.tableOperations().delete("table1");
        }

        if (accCon.tableOperations().exists("table2")) {
            accCon.tableOperations().delete("table2");
        }

    }

    @Test
    public void testEvaluateTwoIndexTwoVarOrder2() throws RepositoryException, MalformedQueryException, SailException,
            QueryEvaluationException, MutationsRejectedException, TableNotFoundException,
            TupleQueryResultHandlerException {

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

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

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, "table1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");

        CountingResultHandler crh = new CountingResultHandler();

        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

        Assert.assertEquals(2, crh.getCount());

    }

    @Test
    public void testEvaluateTwoIndexTwoVarInvalidOrder() throws RepositoryException, MalformedQueryException,
            SailException, QueryEvaluationException, MutationsRejectedException, TableNotFoundException,
            TupleQueryResultHandlerException {

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

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

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, "table1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");

        CountingResultHandler crh = new CountingResultHandler();

        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

        Assert.assertEquals(2, crh.getCount());

    }

    @Test
    public void testEvaluateTwoIndexThreeVarOrder1() throws MalformedQueryException, SailException,
            QueryEvaluationException, MutationsRejectedException, TableNotFoundException, RepositoryException,
            TupleQueryResultHandlerException {

        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");

        conn.add(subclass, RDF.TYPE, superclass);
        conn.add(subclass2, RDF.TYPE, superclass2);
        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String indexSparqlString2 = ""//
                + "SELECT ?e ?c ?l ?f ?o" //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, "table1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");

        CountingResultHandler crh = new CountingResultHandler();

        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

        Assert.assertEquals(2, crh.getCount());

    }

    // @Test
    public void testEvaluateTwoIndexThreeVarsDiffLabel() throws RepositoryException, MalformedQueryException,
            SailException, QueryEvaluationException, MutationsRejectedException, TableNotFoundException,
            TupleQueryResultHandlerException {

        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
        conn.add(subclass, RDF.TYPE, superclass);
        conn.add(subclass2, RDF.TYPE, superclass2);
        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?dog ?pig ?owl  " //
                + "{" //
                + "  ?pig a ?dog . "//
                + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?owl "//
                + "}";//

        String indexSparqlString2 = ""//
                + "SELECT ?e ?c ?l ?f ?o" //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, "table1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");

        CountingResultHandler crh = new CountingResultHandler();

        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

        Assert.assertEquals(2, crh.getCount());

    }

    @Test
    public void testEvaluateTwoIndexThreeVarOrder2() throws RepositoryException, MalformedQueryException,
            SailException, QueryEvaluationException, MutationsRejectedException, TableNotFoundException,
            TupleQueryResultHandlerException {

        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");

        conn.add(subclass, RDF.TYPE, superclass);
        conn.add(subclass2, RDF.TYPE, superclass2);
        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?e ?c ?l  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, "table1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");

        CountingResultHandler crh = new CountingResultHandler();

        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

        Assert.assertEquals(2, crh.getCount());

    }

    @Test
    public void testEvaluateTwoIndexThreeVarOrder3ThreeBindingSet() throws TupleQueryResultHandlerException,
            QueryEvaluationException, MalformedQueryException, RepositoryException, SailException,
            MutationsRejectedException, TableNotFoundException {

        URI sub3 = new URIImpl("uri:entity3");
        URI subclass3 = new URIImpl("uri:class3");
        URI obj3 = new URIImpl("uri:obj3");

        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
        URI superclass3 = new URIImpl("uri:superclass3");

        conn.add(sub3, RDF.TYPE, subclass3);
        conn.add(sub3, RDFS.LABEL, new LiteralImpl("label3"));
        conn.add(sub3, talksTo, obj3);

        conn.add(subclass, RDF.TYPE, superclass);
        conn.add(subclass2, RDF.TYPE, superclass2);
        conn.add(subclass3, RDF.TYPE, superclass3);

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(obj3, RDFS.LABEL, new LiteralImpl("label3"));

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?l ?e ?c  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, "table1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");

        CountingResultHandler crh = new CountingResultHandler();

        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

        Assert.assertEquals(3, crh.getCount());

    }

    @Test
    public void testEvaluateTwoIndexThreeVarOrder5ThreeBindingSet() throws MalformedQueryException, SailException,
            QueryEvaluationException, MutationsRejectedException, TableNotFoundException, RepositoryException,
            TupleQueryResultHandlerException {

        URI sub3 = new URIImpl("uri:entity3");
        URI subclass3 = new URIImpl("uri:class3");
        URI obj3 = new URIImpl("uri:obj3");

        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
        URI superclass3 = new URIImpl("uri:superclass3");

        conn.add(sub3, RDF.TYPE, subclass3);
        conn.add(sub3, RDFS.LABEL, new LiteralImpl("label3"));
        conn.add(sub3, talksTo, obj3);

        conn.add(subclass, RDF.TYPE, superclass);
        conn.add(subclass2, RDF.TYPE, superclass2);
        conn.add(subclass3, RDF.TYPE, superclass3);

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(obj3, RDFS.LABEL, new LiteralImpl("label3"));

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?e ?l ?c  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, "table1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");

        CountingResultHandler crh = new CountingResultHandler();

        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

        Assert.assertEquals(3, crh.getCount());

    }

    @Test
    public void testEvaluateTwoIndexThreeVarOrder4ThreeBindingSet() throws MalformedQueryException, SailException,
            QueryEvaluationException, MutationsRejectedException, TableNotFoundException, RepositoryException,
            TupleQueryResultHandlerException {

        URI sub3 = new URIImpl("uri:entity3");
        URI subclass3 = new URIImpl("uri:class3");
        URI obj3 = new URIImpl("uri:obj3");

        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
        URI superclass3 = new URIImpl("uri:superclass3");

        conn.add(sub3, RDF.TYPE, subclass3);
        conn.add(sub3, RDFS.LABEL, new LiteralImpl("label3"));
        conn.add(sub3, talksTo, obj3);

        conn.add(subclass, RDF.TYPE, superclass);
        conn.add(subclass2, RDF.TYPE, superclass2);
        conn.add(subclass3, RDF.TYPE, superclass3);

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(obj3, RDFS.LABEL, new LiteralImpl("label3"));

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

        // TODO Auto-generated method stub
        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?c ?e ?l  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, "table1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");

        CountingResultHandler crh = new CountingResultHandler();

        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

        Assert.assertEquals(3, crh.getCount());

    }

    @Test
    public void testEvaluateTwoIndexThreeVarOrder6ThreeBindingSet() throws MalformedQueryException, SailException,
            QueryEvaluationException, MutationsRejectedException, TableNotFoundException, RepositoryException,
            TupleQueryResultHandlerException {

        URI sub3 = new URIImpl("uri:entity3");
        URI subclass3 = new URIImpl("uri:class3");
        URI obj3 = new URIImpl("uri:obj3");

        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
        URI superclass3 = new URIImpl("uri:superclass3");

        conn.add(sub3, RDF.TYPE, subclass3);
        conn.add(sub3, RDFS.LABEL, new LiteralImpl("label3"));
        conn.add(sub3, talksTo, obj3);

        conn.add(subclass, RDF.TYPE, superclass);
        conn.add(subclass2, RDF.TYPE, superclass2);
        conn.add(subclass3, RDF.TYPE, superclass3);

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(obj3, RDFS.LABEL, new LiteralImpl("label3"));

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String indexSparqlString2 = ""//
                + "SELECT ?c ?l ?e ?o ?f " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, "table1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");

        CountingResultHandler crh = new CountingResultHandler();

        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

        Assert.assertEquals(3, crh.getCount());

    }

    @Test
    public void testEvaluateTwoIndexThreeVarOrder7ThreeBindingSet() throws MalformedQueryException, SailException,
            QueryEvaluationException, MutationsRejectedException, TableNotFoundException, RepositoryException,
            TupleQueryResultHandlerException {

        URI sub3 = new URIImpl("uri:entity3");
        URI subclass3 = new URIImpl("uri:class3");
        URI obj3 = new URIImpl("uri:obj3");

        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");
        URI superclass3 = new URIImpl("uri:superclass3");

        conn.add(sub3, RDF.TYPE, subclass3);
        conn.add(sub3, RDFS.LABEL, new LiteralImpl("label3"));
        conn.add(sub3, talksTo, obj3);

        conn.add(subclass, RDF.TYPE, superclass);
        conn.add(subclass2, RDF.TYPE, superclass2);
        conn.add(subclass3, RDF.TYPE, superclass3);

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
        conn.add(obj3, RDFS.LABEL, new LiteralImpl("label3"));

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

        String indexSparqlString = ""//
                + "SELECT ?c ?e ?l  " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
                + "}";//

        String indexSparqlString2 = ""//
                + "SELECT ?o ?l ?c ?e ?f " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, "table1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");

        CountingResultHandler crh = new CountingResultHandler();

        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

        Assert.assertEquals(3, crh.getCount());

    }

    @Test
    public void testEvaluateOneIndex() throws RepositoryException, MalformedQueryException, SailException,
            QueryEvaluationException, MutationsRejectedException, TableNotFoundException,
            TupleQueryResultHandlerException {

        String indexSparqlString = ""//
                + "SELECT ?dog ?pig ?duck  " //
                + "{" //
                + "  ?pig a ?dog . "//
                + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, "table1");

        CountingResultHandler crh = new CountingResultHandler();

        conn.prepareTupleQuery(QueryLanguage.SPARQL, indexSparqlString).evaluate(crh);

        Assert.assertEquals(2, crh.getCount());

    }

    @Test
    public void testEvaluateTwoIndexThreeVarOrder3() throws RepositoryException, MalformedQueryException,
            SailException, QueryEvaluationException, MutationsRejectedException, TableNotFoundException,
            TupleQueryResultHandlerException {

        URI superclass = new URIImpl("uri:superclass");
        URI superclass2 = new URIImpl("uri:superclass2");

        conn.add(subclass, RDF.TYPE, superclass);
        conn.add(subclass2, RDF.TYPE, superclass2);
        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

        conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
        conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

        String indexSparqlString = ""//
                + "SELECT ?dog ?pig ?duck  " //
                + "{" //
                + "  ?pig a ?dog . "//
                + "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
                + "}";//

        String indexSparqlString2 = ""//
                + "SELECT ?o ?f ?e ?c ?l  " //
                + "{" //
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        String queryString = ""//
                + "SELECT ?e ?c ?l ?f ?o " //
                + "{" //
                + "  ?e a ?c . "//
                + "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?e <uri:talksTo> ?o . "//
                + "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
                + "  ?c a ?f . " //
                + "}";//

        AccumuloIndexSet ais1 = new AccumuloIndexSet(indexSparqlString, conn, accCon, "table1");
        AccumuloIndexSet ais2 = new AccumuloIndexSet(indexSparqlString2, conn, accCon, "table2");

        CountingResultHandler crh = new CountingResultHandler();

        conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

        Assert.assertEquals(2, crh.getCount());

    }

    public static class CountingResultHandler implements TupleQueryResultHandler {
        private int count = 0;

        public int getCount() {
            return count;
        }

        public void resetCount() {
            this.count = 0;
        }

        @Override
        public void startQueryResult(List<String> arg0) throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleSolution(BindingSet arg0) throws TupleQueryResultHandlerException {
            count++;
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
        }

        @Override
        public void handleBoolean(boolean arg0) throws QueryResultHandlerException {
            // TODO Auto-generated method stub

        }

        @Override
        public void handleLinks(List<String> arg0) throws QueryResultHandlerException {
            // TODO Auto-generated method stub

        }
    }

}
