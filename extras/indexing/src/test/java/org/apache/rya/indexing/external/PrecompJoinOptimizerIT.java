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
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.rdftriplestore.inference.InferenceEngineException;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.repository.sail.SailRepositoryConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;

public class PrecompJoinOptimizerIT {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

	private SailRepositoryConnection conn, pcjConn;
	private SailRepository repo, pcjRepo;
	private Connector accCon;
	String tablePrefix = "table_";
	IRI sub, sub2, obj, obj2, subclass, subclass2, talksTo;

	@Before
	public void init() throws RepositoryException,
			TupleQueryResultHandlerException, QueryEvaluationException,
			MalformedQueryException, AccumuloException,
			AccumuloSecurityException, TableExistsException, RyaDAOException,
			TableNotFoundException, InferenceEngineException, NumberFormatException,
			UnknownHostException, SailException {

		repo = PcjIntegrationTestingUtil.getAccumuloNonPcjRepo(tablePrefix, "instance");
		conn = repo.getConnection();

		pcjRepo = PcjIntegrationTestingUtil.getAccumuloPcjRepo(tablePrefix, "instance");
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

		conn.add(sub2, RDF.TYPE, subclass2);
		conn.add(sub2, RDFS.LABEL, VF.createLiteral("label2"));
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
			SailException, TableNotFoundException, PcjException, InferenceEngineException,
			NumberFormatException, UnknownHostException {

		final String indexSparqlString = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablePrefix
				+ "INDEX_1", indexSparqlString, new String[] { "e", "l", "c" },
				Optional.absent());
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
		repo = PcjIntegrationTestingUtil.getAccumuloPcjRepo(tablePrefix, "instance");
		conn = repo.getConnection();
		conn.add(sub, talksTo, obj);
		conn.add(sub2, talksTo, obj2);
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(crh);

		Assert.assertEquals(2, crh.getCount());

	}

	@Test
	public void testEvaluateTwoIndexTwoVarOrder1() throws AccumuloException,
			AccumuloSecurityException, TableExistsException,
			RepositoryException, MalformedQueryException, SailException,
			QueryEvaluationException, TableNotFoundException,
			TupleQueryResultHandlerException, RyaDAOException, PcjException {

		conn.add(obj, RDFS.LABEL, VF.createLiteral("label"));
		conn.add(obj2, RDFS.LABEL, VF.createLiteral("label2"));

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

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablePrefix
				+ "INDEX_1", indexSparqlString, new String[] { "e", "l", "c" },
				Optional.absent());
		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablePrefix
				+ "INDEX_2", indexSparqlString2, new String[] { "e", "l", "o" },
				Optional.absent());
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
			SailException, TableNotFoundException, PcjException, InferenceEngineException,
			NumberFormatException, UnknownHostException {

		final String indexSparqlString = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ "  Filter(?e = <uri:entity>) " //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablePrefix
				+ "INDEX_1", indexSparqlString, new String[] { "e", "l", "c" },
				Optional.absent());
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
		repo = PcjIntegrationTestingUtil.getAccumuloPcjRepo(tablePrefix, "instance");
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
			SailException, TableNotFoundException, PcjException, InferenceEngineException,
			NumberFormatException, UnknownHostException {

		final String indexSparqlString2 = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ "  Filter(?l = \"label2\") " //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablePrefix
				+ "INDEX_2", indexSparqlString2, new String[] { "e", "l", "c" },
				Optional.absent());

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
		repo = PcjIntegrationTestingUtil.getAccumuloPcjRepo(tablePrefix, "instance");
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
			SailException, TableNotFoundException, PcjException, InferenceEngineException,
			NumberFormatException, UnknownHostException {

		final String indexSparqlString1 = ""//
				+ "SELECT ?e ?l ?c " //
				+ "{" //
				+ "  Filter(?l = \"label3\") " //
				+ "  ?e a ?c . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		final IRI sub3 = VF.createIRI("uri:entity3");
		final IRI subclass3 = VF.createIRI("uri:class3");
		conn.add(sub3, RDF.TYPE, subclass3);
		conn.add(sub3, RDFS.LABEL, VF.createLiteral("label3"));

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablePrefix
				+ "INDEX_1", indexSparqlString1, new String[] { "e", "l", "c" },
				Optional.absent());
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
		repo = PcjIntegrationTestingUtil.getAccumuloPcjRepo(tablePrefix, "instance");
		conn = repo.getConnection();
		conn.add(sub, talksTo, obj);
		conn.add(sub, RDFS.LABEL, VF.createLiteral("label"));
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(
				crh);

		Assert.assertEquals(1, crh.getCount());

	}

	@Test
	public void testEvaluateTwoIndexUnionFilter() throws AccumuloException,
			AccumuloSecurityException, TableExistsException,
			RepositoryException, MalformedQueryException, SailException,
			QueryEvaluationException, TableNotFoundException,
			TupleQueryResultHandlerException, RyaDAOException, PcjException, InferenceEngineException,
			NumberFormatException, UnknownHostException {

		conn.add(obj, RDFS.LABEL, VF.createLiteral("label"));
		conn.add(obj2, RDFS.LABEL, VF.createLiteral("label2"));
		conn.add(sub, RDF.TYPE, obj);
		conn.add(sub2, RDF.TYPE, obj2);

		final String indexSparqlString = ""//
				+ "SELECT ?e ?l ?o " //
				+ "{" //
				+ "   Filter(?l = \"label2\") " //
				+ "  ?e a ?o . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		final String indexSparqlString2 = ""//
				+ "SELECT ?e ?l ?o " //
				+ "{" //
				+ "   Filter(?l = \"label2\") " //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		final String queryString = ""//
				+ "SELECT ?c ?e ?l ?o " //
				+ "{" //
				+ "   Filter(?l = \"label2\") " //
				+ "  ?e a ?c . "//
				+ " { ?e a ?o .  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l  }"//
				+ " UNION { ?e <uri:talksTo> ?o .  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l  }"//
				+ "}";//

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablePrefix
				+ "INDEX_1", indexSparqlString, new String[] { "e", "l", "o" },
				Optional.absent());
		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablePrefix
				+ "INDEX_2", indexSparqlString2, new String[] { "e", "l", "o" },
				Optional.absent());

		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, tablePrefix);
		PcjIntegrationTestingUtil.closeAndShutdown(conn, repo);
		repo = PcjIntegrationTestingUtil.getAccumuloPcjRepo(tablePrefix, "instance");
		conn = repo.getConnection();
		conn.add(sub2, RDF.TYPE, subclass2);
		conn.add(sub2, RDF.TYPE, obj2);
		final CountingResultHandler crh = new CountingResultHandler();
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(
				crh);

		Assert.assertEquals(6, crh.getCount());

	}

	@Test
	public void testEvaluateTwoIndexLeftJoinUnionFilter()
			throws AccumuloException, AccumuloSecurityException,
			TableExistsException, RepositoryException, MalformedQueryException,
			SailException, QueryEvaluationException, TableNotFoundException,
			TupleQueryResultHandlerException, RyaDAOException, PcjException, InferenceEngineException,
			NumberFormatException, UnknownHostException {

		conn.add(obj, RDFS.LABEL, VF.createLiteral("label"));
		conn.add(obj2, RDFS.LABEL, VF.createLiteral("label2"));
		conn.add(sub, RDF.TYPE, obj);
		conn.add(sub2, RDF.TYPE, obj2);

		final IRI livesIn = VF.createIRI("uri:livesIn");
		final IRI city = VF.createIRI("uri:city");
		final IRI city2 = VF.createIRI("uri:city2");
		final IRI city3 = VF.createIRI("uri:city3");
		conn.add(sub, livesIn, city);
		conn.add(sub2, livesIn, city2);
		conn.add(sub2, livesIn, city3);
		conn.add(sub, livesIn, city3);

		final String indexSparqlString = ""//
				+ "SELECT ?e ?l ?o " //
				+ "{" //
				+ "  ?e a ?o . "//
				+ "  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		final String indexSparqlString2 = ""//
				+ "SELECT ?e ?l ?o " //
				+ "{" //
				+ "  ?e <uri:talksTo> ?o . "//
				+ "  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l "//
				+ "}";//

		final String queryString = ""//
				+ "SELECT ?c ?e ?l ?o " //
				+ "{" //
				+ " Filter(?c = <uri:city3>) " //
				+ " ?e <uri:livesIn> ?c . "//
				+ " OPTIONAL{{ ?e a ?o .  ?e <http://www.w3.org/2000/01/rdf-schema#label> ?l  }"//
				+ " UNION { ?e <uri:talksTo> ?o .  ?o <http://www.w3.org/2000/01/rdf-schema#label> ?l  }}"//
				+ "}";//

		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablePrefix
				+ "INDEX_1", indexSparqlString, new String[] { "e", "l", "o" },
				Optional.absent());
		PcjIntegrationTestingUtil.createAndPopulatePcj(conn, accCon, tablePrefix
				+ "INDEX_2", indexSparqlString2, new String[] { "e", "l", "o" },
				Optional.absent());

		PcjIntegrationTestingUtil.deleteCoreRyaTables(accCon, tablePrefix);
		PcjIntegrationTestingUtil.closeAndShutdown(conn, repo);
		repo = PcjIntegrationTestingUtil.getAccumuloPcjRepo(tablePrefix, "instance");
		conn = repo.getConnection();
		conn.add(sub2, livesIn, city3);
		conn.add(sub, livesIn, city3);

		final CountingResultHandler crh = new CountingResultHandler();
		pcjConn.prepareTupleQuery(QueryLanguage.SPARQL, queryString).evaluate(
				crh);

		Assert.assertEquals(6, crh.getCount());

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
			System.out.println(arg0);
			count++;
			System.out.println("Count is " + count);
		}

		@Override
		public void endQueryResult() throws TupleQueryResultHandlerException {
		}

		@Override
		public void handleBoolean(final boolean arg0)
				throws QueryResultHandlerException {
			// TODO Auto-generated method stub

		}

		@Override
		public void handleLinks(final List<String> arg0)
				throws QueryResultHandlerException {
			// TODO Auto-generated method stub

		}
	}

}