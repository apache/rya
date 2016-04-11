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

import java.util.List;
import java.util.Map;

import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.rdftriplestore.inference.InferenceEngineException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
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

public class PrecompJoinOptimizerIntegrationTest {

	private SailRepositoryConnection conn, pcjConn;
	private SailRepository repo, pcjRepo;
	private Connector accCon;
	String tablePrefix = "table_";
	URI sub, sub2, obj, obj2, subclass, subclass2, talksTo;

	@Before
	public void init() throws RepositoryException,
			TupleQueryResultHandlerException, QueryEvaluationException,
			MalformedQueryException, AccumuloException,
			AccumuloSecurityException, TableExistsException, RyaDAOException,
			TableNotFoundException, InferenceEngineException {

		repo = PcjIntegrationTestingUtil.getNonPcjRepo(tablePrefix, "instance");
		conn = repo.getConnection();

		pcjRepo = PcjIntegrationTestingUtil.getPcjRepo(tablePrefix, "instance");
		pcjConn = pcjRepo.getConnection();

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
	public void testEvaluateSingeIndex()
			throws TupleQueryResultHandlerException, QueryEvaluationException,
			MalformedQueryException, RepositoryException, AccumuloException,
			AccumuloSecurityException, TableExistsException, RyaDAOException,
			SailException, TableNotFoundException, PcjException,
			InferenceEngineException {

		final String indexSparqlString = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon,
				tablePrefix + "INDEX_1", indexSparqlString, new String[] { "e",
						"l", "c" }, Optional.<PcjVarOrderFactory> absent());
		final String queryString = ""//
				+ "SELECT ?e ?c ?l ?o " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "}";//

		final CountingResultHandler crh = new CountingResultHandler();
		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, tablePrefix);
		PcjIntegrationTestingUtil.closeAndShutdown(conn, repo);
		repo = PcjIntegrationTestingUtil.getPcjRepo(tablePrefix, "instance");
		conn = repo.getConnection();
		conn.add(sub, talksTo, obj);
		conn.add(sub2, talksTo, obj2);
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(
				crh);

		Assert.assertEquals(2, crh.getCount());

	}

	@Test
	public void testEvaluateTwoIndexTwoVarOrder1() throws AccumuloException,
			AccumuloSecurityException, TableExistsException,
			RepositoryException, MalformedQueryException, SailException,
			QueryEvaluationException, TableNotFoundException,
			TupleQueryResultHandlerException, RyaDAOException, PcjException {

		conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));
		conn.add(obj2, RDFS.LABEL, new LiteralImpl("label2"));

		final String indexSparqlString = ""//
				+ "SELECT ?e ?l ?c " //
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

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon,
				tablePrefix + "INDEX_1", indexSparqlString, new String[] { "e",
						"l", "c" }, Optional.<PcjVarOrderFactory> absent());
		PcjIntegrationTestingUtil
				.createAndPopulatePcj(conn, accCon, tablePrefix + "INDEX_2",
						indexSparqlString2, new String[] { "e", "l", "o" },
						Optional.<PcjVarOrderFactory> absent());
		final CountingResultHandler crh = new CountingResultHandler();
		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, tablePrefix);
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(
				crh);

		Assert.assertEquals(2, crh.getCount());

	}

	@Test
	public void testEvaluateSingeFilterIndex()
			throws TupleQueryResultHandlerException, QueryEvaluationException,
			MalformedQueryException, RepositoryException, AccumuloException,
			AccumuloSecurityException, TableExistsException, RyaDAOException,
			SailException, TableNotFoundException, PcjException,
			InferenceEngineException {

		final String indexSparqlString = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ "  Filter(?e = <uri:entity>) " //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon,
				tablePrefix + "INDEX_1", indexSparqlString, new String[] { "e",
						"l", "c" }, Optional.<PcjVarOrderFactory> absent());
		final String queryString = ""//
				+ "SELECT ?e ?c ?l ?o " //
				+ "{" //
				+ "   Filter(?e = <uri:entity>) " //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "}";//

		final CountingResultHandler crh = new CountingResultHandler();
		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, tablePrefix);
		PcjIntegrationTestingUtil.closeAndShutdown(conn, repo);
		repo = PcjIntegrationTestingUtil.getPcjRepo(tablePrefix, "instance");
		conn = repo.getConnection();
		conn.add(sub, talksTo, obj);
		conn.add(sub2, talksTo, obj2);
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(
				crh);

		Assert.assertEquals(1, crh.getCount());

	}

	@Test
	public void testEvaluateSingeFilterWithUnion()
			throws TupleQueryResultHandlerException, QueryEvaluationException,
			MalformedQueryException, RepositoryException, AccumuloException,
			AccumuloSecurityException, TableExistsException, RyaDAOException,
			SailException, TableNotFoundException, PcjException,
			InferenceEngineException {

		final String indexSparqlString2 = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ "  Filter(?l = \"label2\") " //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		PcjIntegrationTestingUtil
				.createAndPopulatePcj(conn, accCon, tablePrefix + "INDEX_2",
						indexSparqlString2, new String[] { "e", "l", "c" },
						Optional.<PcjVarOrderFactory> absent());

		final String queryString = ""//
				+ "SELECT ?e ?c ?o ?m ?l" //
				+ "{" //
				+ "   Filter(?l = \"label2\") " //
				+ "  ?e <uri:talksTo> ?o . "//
				+ " { ?e a ?c .  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?m  }"//
				+ " UNION { ?e a ?c .  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l  }"//
				+ "}";//

		final CountingResultHandler crh = new CountingResultHandler();
		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, tablePrefix);
		PcjIntegrationTestingUtil.closeAndShutdown(conn, repo);
		repo = PcjIntegrationTestingUtil.getPcjRepo(tablePrefix, "instance");
		conn = repo.getConnection();
		conn.add(sub, talksTo, obj);
		conn.add(sub2, talksTo, obj2);
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(
				crh);

		Assert.assertEquals(1, crh.getCount());

	}

	@Test
	public void testEvaluateSingeFilterWithLeftJoin()
			throws TupleQueryResultHandlerException, QueryEvaluationException,
			MalformedQueryException, RepositoryException, AccumuloException,
			AccumuloSecurityException, TableExistsException, RyaDAOException,
			SailException, TableNotFoundException, PcjException,
			InferenceEngineException {

		final String indexSparqlString1 = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ "  Filter(?l = \"label3\") " //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		final URI sub3 = new URIImpl("uri:entity3");
		final URI subclass3 = new URIImpl("uri:class3");
		conn.add(sub3, RDF.TYPE, subclass3);
		conn.add(sub3, RDFS.LABEL, new LiteralImpl("label3"));

		PcjIntegrationTestingUtil
				.createAndPopulatePcj(conn, accCon, tablePrefix + "INDEX_1",
						indexSparqlString1, new String[] { "e", "l", "c" },
						Optional.<PcjVarOrderFactory> absent());
		final String queryString = ""//
				+ "SELECT ?e ?c ?o ?m ?l" //
				+ "{" //
				+ "  Filter(?l = \"label3\") " //
				+ "  ?e a ?c . " //
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l . " //
				+ "  OPTIONAL { ?e <uri:talksTo> ?o . ?e <http://www.w3.org/2000/01/rdf-schema#label> ?m }"//
				+ "}";//

		final CountingResultHandler crh = new CountingResultHandler();
		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, tablePrefix);
		PcjIntegrationTestingUtil.closeAndShutdown(conn, repo);
		repo = PcjIntegrationTestingUtil.getPcjRepo(tablePrefix, "instance");
		conn = repo.getConnection();
		conn.add(sub, talksTo, obj);
		conn.add(sub, RDFS.LABEL, new LiteralImpl("label"));
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(
				crh);

		Assert.assertEquals(1, crh.getCount());

	}


	@Test
	public void testMultipleLeftJoin() throws AccumuloException,
			AccumuloSecurityException, TableExistsException,
			RepositoryException, MalformedQueryException, SailException,
			QueryEvaluationException, TableNotFoundException,
			TupleQueryResultHandlerException, RyaDAOException, PcjException,
			InferenceEngineException {

		final URI sub3 = new URIImpl("uri:entity3");
		final URI obj3 = new URIImpl("uri:obj3");
		final URI subclass3 = new URIImpl("uri:class3");
		conn.add(sub3, RDF.TYPE, subclass3);
		conn.add(sub3, talksTo, obj3);
		conn.add(obj, RDFS.LABEL, new LiteralImpl("label"));

		final String indexSparqlString = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  OPTIONAL {?e <http://www.w3.org/2000/01/rdf-schema#label> ?l} "//
				+ "}";//

		final String indexSparqlString2 = ""//
				+ "SELECT ?e ?l ?o " //
				+ "{" //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  OPTIONAL {?o <http://www.w3.org/2000/01/rdf-schema#label> ?l} "//
				+ "}";//

		final String queryString = ""//
				+ "SELECT ?e ?l ?c ?o " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  OPTIONAL {?e <http://www.w3.org/2000/01/rdf-schema#label> ?l} "//
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  OPTIONAL {?o <http://www.w3.org/2000/01/rdf-schema#label> ?l} "//
				+ "}";//

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon,
				tablePrefix + "INDEX_1", indexSparqlString, new String[] { "e",
						"l", "c" }, Optional.<PcjVarOrderFactory> absent());
		PcjIntegrationTestingUtil
				.createAndPopulatePcj(conn, accCon, tablePrefix + "INDEX_2",
						indexSparqlString2, new String[] { "e", "l", "o" },
						Optional.<PcjVarOrderFactory> absent());

		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, tablePrefix);
		PcjIntegrationTestingUtil.closeAndShutdown(conn, repo);
		repo = PcjIntegrationTestingUtil.getPcjRepo(tablePrefix, "instance");
		conn = repo.getConnection();

		Scanner scanner = accCon.createScanner(tablePrefix + "INDEX_2",
				new Authorizations());
		for (Map.Entry<Key, org.apache.accumulo.core.data.Value> e : scanner) {
			System.out.println(e.getKey().getRow());
		}

		final CountingResultHandler crh = new CountingResultHandler();
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(
				crh);

		Assert.assertEquals(3, crh.getCount());

	}

	// @Test
	// public void leftJoinExperiment() throws AccumuloException,
	// AccumuloSecurityException, TableExistsException,
	// RepositoryException, MalformedQueryException, SailException,
	// QueryEvaluationException, TableNotFoundException,
	// TupleQueryResultHandlerException, RyaDAOException, PcjException,
	// InferenceEngineException {
	//
	// final String indexSparqlString = ""//
	// + "SELECT ?e " //
	// + "{" //
	// + "  ?e a <uri:class> . "//
	// +
	// "  OPTIONAL {?e <http://www.w3.org/2000/01/rdf-schema#label> \"label2\"} "//
	// + "}";//
	//
	//
	// PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon,
	// tablePrefix + "INDEX_1", indexSparqlString, new String[] {"e"},
	// Optional.<PcjVarOrderFactory> absent());
	//
	// Scanner scanner = accCon.createScanner(tablePrefix + "INDEX_1",
	// new Authorizations());
	// for (Map.Entry<Key, org.apache.accumulo.core.data.Value> e : scanner) {
	// System.out.println(e.getKey().getRow());
	// }
	//
	// }

	public static class CountingResultHandler implements
			TupleQueryResultHandler {
		private int count = 0;

		public int getCount() {
			return count;
		}

		public void resetCount() {
			this.count = 0;
		}

		@Override
		public void startQueryResult(List<String> arg0)
				throws TupleQueryResultHandlerException {
		}

		@Override
		public void handleSolution(BindingSet arg0)
				throws TupleQueryResultHandlerException {
			System.out.println(arg0);
			count++;
			System.out.println("Count is " + count);
		}

		@Override
		public void endQueryResult() throws TupleQueryResultHandlerException {
		}

		@Override
		public void handleBoolean(boolean arg0)
				throws QueryResultHandlerException {
			// TODO Auto-generated method stub

		}

		@Override
		public void handleLinks(List<String> arg0)
				throws QueryResultHandlerException {
			// TODO Auto-generated method stub

		}
	}

}