package org.apache.rya.indexing.external;

import java.net.UnknownHostException;

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

import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjVarOrderFactory;
import org.junit.After;
import org.junit.Assert;
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
import org.openrdf.sail.SailException;

import com.google.common.base.Optional;

import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;

public class AccumuloConstantPcjIntegrationTest {

	private SailRepositoryConnection conn, pcjConn;
	private SailRepository repo, pcjRepo;
	private Connector accCon;
	String prefix = "table_";
	String tablename = "table_INDEX_";
	URI obj, obj2, subclass, subclass2, talksTo;

	@Before
	public void init() throws RepositoryException,
			TupleQueryResultHandlerException, QueryEvaluationException,
			MalformedQueryException, AccumuloException,
			AccumuloSecurityException, TableExistsException,
			TableNotFoundException, RyaDAOException, InferenceEngineException,
			NumberFormatException, UnknownHostException, SailException {

		repo = PcjIntegrationTestingUtil.getNonPcjRepo(prefix, "instance");
		conn = repo.getConnection();

		pcjRepo = PcjIntegrationTestingUtil.getPcjRepo(prefix, "instance");
		pcjConn = pcjRepo.getConnection();

		final URI sub = new URIImpl("uri:entity");
		subclass = new URIImpl("uri:class");
		obj = new URIImpl("uri:obj");
		talksTo = new URIImpl("uri:talksTo");

		conn.add(sub, RDF.TYPE, subclass);
		conn.add(sub, RDFS.LABEL, new LiteralImpl("label"));
		conn.add(sub, talksTo, obj);

		final URI sub2 = new URIImpl("uri:entity2");
		subclass2 = new URIImpl("uri:class2");
		obj2 = new URIImpl("uri:obj2");

		conn.add(sub2, RDF.TYPE, subclass2);
		conn.add(sub2, RDFS.LABEL, new LiteralImpl("label2"));
		conn.add(sub2, talksTo, obj2);

		accCon = new MockInstance("instance").getConnector("root",new PasswordToken(""));

	}

	@After
	public void close() throws RepositoryException, AccumuloException,
			AccumuloSecurityException, TableNotFoundException {
		PcjIntegrationTestingUtil.closeAndShutdown(conn, repo);
		PcjIntegrationTestingUtil.closeAndShutdown(pcjConn, pcjRepo);
		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, prefix);
		PcjIntegrationTestingUtil.deleteIndexTables(accCon, 2, prefix);


	}

	@Test
	public void testEvaluateTwoIndexVarInstantiate1() throws PcjException,
			RepositoryException, AccumuloException, AccumuloSecurityException,
			TableNotFoundException, TableExistsException,
			MalformedQueryException, SailException, QueryEvaluationException,
			TupleQueryResultHandlerException {

		final URI superclass = new URIImpl("uri:superclass");
		final URI superclass2 = new URIImpl("uri:superclass2");

		conn.add(subclass, RDF.TYPE, superclass);
		conn.add(subclass2, RDF.TYPE, superclass2);
		conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
		conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

		conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
		conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

		final String indexSparqlString = ""//
				+ "SELECT ?dog ?pig ?duck  " //
				+ "{" //
				+ "  ?pig a ?dog . "//
				+ "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
				+ "}";//

		final String indexSparqlString2 = ""//
				+ "SELECT ?o ?f ?e ?c ?l  " //
				+ "{" //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
				+ "  ?c a ?f . " //
				+ "}";//

		final String queryString = ""//
				+ "SELECT ?c ?l ?f ?o " //
				+ "{" //
				+ "  <uri:entity> a ?c . "//
				+ "  <uri:entity> <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
				+ "  <uri:entity> <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
				+ "  ?c a ?f . " //
				+ "}";//

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablename + 1,
				indexSparqlString, new String[] { "dog", "pig", "duck" },
				Optional.<PcjVarOrderFactory> absent());
		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablename + 2,
				indexSparqlString2, new String[] { "o", "f", "e", "c", "l" },
				Optional.<PcjVarOrderFactory> absent());

		final CountingResultHandler crh1 = new CountingResultHandler();
		final CountingResultHandler crh2 = new CountingResultHandler();

		conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString)
				.evaluate(crh1);
		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, prefix);
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);

		Assert.assertEquals(crh1.getCount(), crh2.getCount());

	}

	@Test
	public void testEvaluateThreeIndexVarInstantiate() throws PcjException,
			RepositoryException, AccumuloException, AccumuloSecurityException,
			TableNotFoundException, TableExistsException,
			MalformedQueryException, SailException, QueryEvaluationException,
			TupleQueryResultHandlerException {

		final URI superclass = new URIImpl("uri:superclass");
		final URI superclass2 = new URIImpl("uri:superclass2");

		final URI sub = new URIImpl("uri:entity");
		subclass = new URIImpl("uri:class");
		obj = new URIImpl("uri:obj");
		talksTo = new URIImpl("uri:talksTo");

		final URI howlsAt = new URIImpl("uri:howlsAt");
		final URI subType = new URIImpl("uri:subType");

		conn.add(subclass, RDF.TYPE, superclass);
		conn.add(subclass2, RDF.TYPE, superclass2);
		conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
		conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));
		conn.add(sub, howlsAt, superclass);
		conn.add(superclass, subType, obj);

		conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
		conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

		final String indexSparqlString = ""//
				+ "SELECT ?dog ?pig ?duck  " //
				+ "{" //
				+ "  ?pig a ?dog . "//
				+ "  ?pig <http://www.w3.org/2000/01/rdf-schema#label> ?duck "//
				+ "}";//

		final String indexSparqlString2 = ""//
				+ "SELECT ?o ?f ?e ?c ?l  " //
				+ "{" //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
				+ "  ?c a ?f . " //
				+ "}";//

		final String indexSparqlString3 = ""//
				+ "SELECT ?wolf ?sheep ?chicken  " //
				+ "{" //
				+ "  ?wolf <uri:howlsAt> ?sheep . "//
				+ "  ?sheep <uri:subType> ?chicken. "//
				+ "}";//

		final String queryString = ""//
				+ "SELECT ?c ?l ?f ?o " //
				+ "{" //
				+ "  <uri:entity> a ?c . "//
				+ "  <uri:entity> <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
				+ "  <uri:entity> <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l. "//
				+ "  ?c a ?f . " //
				+ "  <uri:entity> <uri:howlsAt> ?f. "//
				+ "  ?f <uri:subType> <uri:obj>. "//
				+ "}";//

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablename + 1,
				indexSparqlString, new String[] { "dog", "pig", "duck" },
				Optional.<PcjVarOrderFactory> absent());
		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablename + 2,
				indexSparqlString2, new String[] { "o", "f", "e", "c", "l" },
				Optional.<PcjVarOrderFactory> absent());
		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablename + 3,
				indexSparqlString3,
				new String[] { "wolf", "sheep", "chicken" },
				Optional.<PcjVarOrderFactory> absent());

		final CountingResultHandler crh1 = new CountingResultHandler();
		final CountingResultHandler crh2 = new CountingResultHandler();

		conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString)
				.evaluate(crh1);

		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, prefix);
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(
				crh2);

		Assert.assertEquals(crh1.getCount(), crh2.getCount());

	}

	@Test
	public void testEvaluateFilterInstantiate() throws RepositoryException,
			PcjException, MalformedQueryException, SailException,
			QueryEvaluationException, TableNotFoundException,
			TupleQueryResultHandlerException, AccumuloException,
			AccumuloSecurityException {

		final URI e1 = new URIImpl("uri:e1");
		final URI e2 = new URIImpl("uri:e2");
		final URI e3 = new URIImpl("uri:e3");
		final URI f1 = new URIImpl("uri:f1");
		final URI f2 = new URIImpl("uri:f2");
		final URI f3 = new URIImpl("uri:f3");
		final URI g1 = new URIImpl("uri:g1");
		final URI g2 = new URIImpl("uri:g2");
		final URI g3 = new URIImpl("uri:g3");

		conn.add(e1, talksTo, f1);
		conn.add(f1, talksTo, g1);
		conn.add(g1, talksTo, e1);
		conn.add(e2, talksTo, f2);
		conn.add(f2, talksTo, g2);
		conn.add(g2, talksTo, e2);
		conn.add(e3, talksTo, f3);
		conn.add(f3, talksTo, g3);
		conn.add(g3, talksTo, e3);

		final String queryString = ""//
				+ "SELECT ?x ?y ?z " //
				+ "{" //
				+ "Filter(?x = <uri:e1>) . " //
				+ " ?x <uri:talksTo> ?y. " //
				+ " ?y <uri:talksTo> ?z. " //
				+ " ?z <uri:talksTo> <uri:e1>. " //
				+ "}";//

		final String indexSparqlString = ""//
				+ "SELECT ?a ?b ?c ?d " //
				+ "{" //
				+ "Filter(?a = ?d) . " //
				+ " ?a <uri:talksTo> ?b. " //
				+ " ?b <uri:talksTo> ?c. " //
				+ " ?c <uri:talksTo> ?d. " //
				+ "}";//

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablename + 1,
				indexSparqlString, new String[] { "a", "b", "c", "d" },
				Optional.<PcjVarOrderFactory> absent());

		final CountingResultHandler crh1 = new CountingResultHandler();
		final CountingResultHandler crh2 = new CountingResultHandler();

		conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString)
				.evaluate(crh1);
		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, prefix);
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh2);

		Assert.assertEquals(crh1.getCount(), crh2.getCount());

	}

	@Test
	public void testEvaluateCompoundFilterInstantiate()
			throws RepositoryException, PcjException, MalformedQueryException,
			SailException, QueryEvaluationException,
			TableNotFoundException,
			TupleQueryResultHandlerException, AccumuloException, AccumuloSecurityException {

		final URI e1 = new URIImpl("uri:e1");
		final URI f1 = new URIImpl("uri:f1");

		conn.add(e1, talksTo, e1);
		conn.add(e1, talksTo, f1);
		conn.add(f1, talksTo, e1);

		final String queryString = ""//
				+ "SELECT ?x ?y ?z " //
				+ "{" //
				+ "Filter(?x = <uri:e1> && ?y = <uri:e1>) . " //
				+ " ?x <uri:talksTo> ?y. " //
				+ " ?y <uri:talksTo> ?z. " //
				+ " ?z <uri:talksTo> <uri:e1>. " //
				+ "}";//

		final String indexSparqlString = ""//
				+ "SELECT ?a ?b ?c ?d " //
				+ "{" //
				+ "Filter(?a = ?d && ?b = ?d) . " //
				+ " ?a <uri:talksTo> ?b. " //
				+ " ?b <uri:talksTo> ?c. " //
				+ " ?c <uri:talksTo> ?d. " //
				+ "}";//

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablename + 1,
				indexSparqlString, new String[] { "a", "b", "c", "d" },
				Optional.<PcjVarOrderFactory> absent());

		final CountingResultHandler crh1 = new CountingResultHandler();
		final CountingResultHandler crh2 = new CountingResultHandler();

		conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString)
				.evaluate(crh1);
		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, prefix);
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(
				crh2);

		Assert.assertEquals(2, crh1.getCount());
		Assert.assertEquals(crh1.getCount(), crh2.getCount());

	}

	public static class CountingResultHandler implements
			TupleQueryResultHandler {
		private int count = 0;

		public int getCount() {
			return count;
		}

		public void resetCount() {
			count = 0;
		}

		@Override
		public void startQueryResult(final List<String> arg0)
				throws TupleQueryResultHandlerException {
		}

		@Override
		public void handleSolution(final BindingSet arg0)
				throws TupleQueryResultHandlerException {
			count++;
		}

		@Override
		public void endQueryResult() throws TupleQueryResultHandlerException {
		}

		@Override
		public void handleBoolean(final boolean arg0)
				throws QueryResultHandlerException {
		}

		@Override
		public void handleLinks(final List<String> arg0)
				throws QueryResultHandlerException {
		}
	}

}
