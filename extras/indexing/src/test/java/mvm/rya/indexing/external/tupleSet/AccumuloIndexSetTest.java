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
package mvm.rya.indexing.external.tupleSet;

import info.aduna.iteration.CloseableIteration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.resolver.RyaTypeResolverException;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.external.PcjIntegrationTestingUtil;
import mvm.rya.indexing.pcj.matching.QueryVariableNormalizer;
import mvm.rya.rdftriplestore.RyaSailRepository;
import mvm.rya.rdftriplestore.inference.InferenceEngineException;
import mvm.rya.sail.config.RyaSailFactory;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTableNameFactory;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjVarOrderFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.NumericLiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class AccumuloIndexSetTest {

	protected static Connector accumuloConn = null;
	protected RyaSailRepository ryaRepo = null;
	protected RepositoryConnection ryaConn = null;
	protected Configuration conf = getConf();
	protected String prefix = "rya_";

	@Before
	public void init() throws AccumuloException, AccumuloSecurityException,
			RyaDAOException, RepositoryException, TableNotFoundException,
			InferenceEngineException {
		accumuloConn = ConfigUtils.getConnector(conf);
		final TableOperations ops = accumuloConn.tableOperations();
		if (ops.exists(prefix + "INDEX_" + "testPcj")) {
			ops.delete(prefix + "INDEX_" + "testPcj");
		}
		if (ops.exists(prefix + "spo")) {
			ops.delete(prefix + "spo");
		}
		if (ops.exists(prefix + "po")) {
			ops.delete(prefix + "po");
		}
		if (ops.exists(prefix + "osp")) {
			ops.delete(prefix + "osp");
		}
		ryaRepo = new RyaSailRepository(RyaSailFactory.getInstance(conf));
		ryaRepo.initialize();
		ryaConn = ryaRepo.getConnection();
	}

	/**
	 * TODO doc
	 *
	 * @throws MutationsRejectedException
	 * @throws QueryEvaluationException
	 * @throws SailException
	 * @throws MalformedQueryException
	 */
	@Test
	public void accumuloIndexSetTestWithEmptyBindingSet()
			throws RepositoryException, PcjException, TableNotFoundException,
			RyaTypeResolverException, MalformedQueryException, SailException,
			QueryEvaluationException, MutationsRejectedException {
		// Load some Triples into Rya.
		final Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://hasAge"), new NumericLiteralImpl(12,
						XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));

		for (final Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		final String sparql = "SELECT ?name ?age " + "{"
				+ "FILTER(?age < 30) ." + "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> \"Soccer\" " + "}";

		final String pcjTableName = new PcjTableNameFactory().makeTableName(
				prefix, "testPcj");
		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age" },
				Optional.<PcjVarOrderFactory> absent());

		final AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn,
				pcjTableName);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(new QueryBindingSet());
		final Set<BindingSet> fetchedResults = new HashSet<BindingSet>();
		while (results.hasNext()) {
			fetchedResults.add(results.next());
		}
		// Ensure the expected results match those that were stored.
		final QueryBindingSet alice = new QueryBindingSet();
		alice.addBinding("name", new URIImpl("http://Alice"));
		alice.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));

		final QueryBindingSet bob = new QueryBindingSet();
		bob.addBinding("name", new URIImpl("http://Bob"));
		bob.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));

		final QueryBindingSet charlie = new QueryBindingSet();
		charlie.addBinding("name", new URIImpl("http://Charlie"));
		charlie.addBinding("age", new NumericLiteralImpl(12, XMLSchema.INTEGER));

		final Set<BindingSet> expectedResults = Sets.<BindingSet> newHashSet(
				alice, bob, charlie);
		Assert.assertEquals(expectedResults, fetchedResults);
	}

	/**
	 * TODO doc
	 *
	 * @throws MutationsRejectedException
	 * @throws QueryEvaluationException
	 * @throws SailException
	 * @throws MalformedQueryException
	 */
	@Test
	public void accumuloIndexSetTestWithBindingSet()
			throws RepositoryException, PcjException, TableNotFoundException,
			RyaTypeResolverException, MalformedQueryException, SailException,
			QueryEvaluationException, MutationsRejectedException {
		// Load some Triples into Rya.
		final Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://hasAge"), new NumericLiteralImpl(12,
						XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));

		for (final Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		final String sparql = "SELECT ?name ?age " + "{"
				+ "FILTER(?age < 30) ." + "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> \"Soccer\" " + "}";

		final String pcjTableName = new PcjTableNameFactory().makeTableName(
				prefix, "testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age" },
				Optional.<PcjVarOrderFactory> absent());

		final AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn,
				pcjTableName);

		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("name", new URIImpl("http://Alice"));
		bs.addBinding("location", new URIImpl("http://Virginia"));

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bs);

		bs.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		Assert.assertEquals(bs, results.next());

	}

	@Test
	public void accumuloIndexSetTestWithTwoBindingSets()
			throws RepositoryException, PcjException, TableNotFoundException,
			RyaTypeResolverException, MalformedQueryException, SailException,
			QueryEvaluationException, MutationsRejectedException {
		// Load some Triples into Rya.
		final Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://hasAge"), new NumericLiteralImpl(12,
						XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));

		for (final Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		final String sparql = "SELECT ?name ?age " + "{"
				+ "FILTER(?age < 30) ." + "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> \"Soccer\" " + "}";

		final String pcjTableName = new PcjTableNameFactory().makeTableName(
				prefix, "testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age" },
				Optional.<PcjVarOrderFactory> absent());

		final AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn,
				pcjTableName);

		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("birthDate", new LiteralImpl("1983-03-17", new URIImpl(
				"http://www.w3.org/2001/XMLSchema#date")));
		bs.addBinding("name", new URIImpl("http://Alice"));

		final QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("birthDate", new LiteralImpl("1983-04-18", new URIImpl(
				"http://www.w3.org/2001/XMLSchema#date")));
		bs2.addBinding("name", new URIImpl("http://Bob"));

		final Set<BindingSet> bSets = Sets.<BindingSet> newHashSet(bs, bs2);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		final QueryBindingSet alice = new QueryBindingSet();
		alice.addBinding("name", new URIImpl("http://Alice"));
		alice.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		alice.addBinding("birthDate", new LiteralImpl("1983-03-17",
				new URIImpl("http://www.w3.org/2001/XMLSchema#date")));

		final QueryBindingSet bob = new QueryBindingSet();
		bob.addBinding("name", new URIImpl("http://Bob"));
		bob.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		bob.addBinding("birthDate", new LiteralImpl("1983-04-18", new URIImpl(
				"http://www.w3.org/2001/XMLSchema#date")));

		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			final BindingSet next = results.next();
			System.out.println(next);
			fetchedResults.add(next);
		}

		Assert.assertEquals(Sets.<BindingSet> newHashSet(alice, bob),
				fetchedResults);
	}

	@Test
	public void accumuloIndexSetTestWithNoBindingSet()
			throws RepositoryException, PcjException, TableNotFoundException,
			RyaTypeResolverException, MalformedQueryException, SailException,
			QueryEvaluationException, MutationsRejectedException {
		// Load some Triples into Rya.
		final Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://hasAge"), new NumericLiteralImpl(12,
						XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));

		for (final Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		final String sparql = "SELECT ?name ?age " + "{"
				+ "FILTER(?age < 30) ." + "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> \"Soccer\" " + "}";

		final String pcjTableName = new PcjTableNameFactory().makeTableName(
				prefix, "testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age" },
				Optional.<PcjVarOrderFactory> absent());

		final AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn,
				pcjTableName);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(new HashSet<BindingSet>());

		Assert.assertEquals(false, results.hasNext());

	}

	@Test
	public void multipleCommonVarBindingTest() throws RepositoryException,
			PcjException, TableNotFoundException, RyaTypeResolverException,
			MalformedQueryException, SailException, QueryEvaluationException,
			MutationsRejectedException {
		// Load some Triples into Rya.
		final Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://hasAge"), new NumericLiteralImpl(12,
						XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));

		for (final Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		final String sparql = "SELECT ?name ?age " + "{"
				+ "FILTER(?age < 30) ." + "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> \"Soccer\" " + "}";

		final String pcjTableName = new PcjTableNameFactory().makeTableName(
				prefix, "testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age" },
				Optional.<PcjVarOrderFactory> absent());

		final AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn,
				pcjTableName);

		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("birthDate", new LiteralImpl("1983-03-17", new URIImpl(
				"http://www.w3.org/2001/XMLSchema#date")));
		bs.addBinding("location", new URIImpl("http://Virginia"));

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bs);

		final QueryBindingSet alice = new QueryBindingSet();
		alice.addBinding("name", new URIImpl("http://Alice"));
		alice.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		alice.addAll(bs);

		final QueryBindingSet bob = new QueryBindingSet();
		bob.addBinding("name", new URIImpl("http://Bob"));
		bob.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		bob.addAll(bs);

		final QueryBindingSet charlie = new QueryBindingSet();
		charlie.addBinding("name", new URIImpl("http://Charlie"));
		charlie.addBinding("age", new NumericLiteralImpl(12, XMLSchema.INTEGER));
		charlie.addAll(bs);

		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			fetchedResults.add(results.next());
		}
		Assert.assertEquals(3, fetchedResults.size());
		Assert.assertEquals(Sets.<BindingSet> newHashSet(alice, bob, charlie),
				fetchedResults);
	}

	@Test
	public void manyCommonVarBindingTest() throws RepositoryException,
			PcjException, TableNotFoundException, RyaTypeResolverException,
			MalformedQueryException, SailException, QueryEvaluationException,
			MutationsRejectedException {
		// Load some Triples into Rya.
		final Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://hasAge"), new NumericLiteralImpl(12,
						XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));

		for (final Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		final String sparql = "SELECT ?name ?age " + "{"
				+ "FILTER(?age < 30) ." + "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> \"Soccer\" " + "}";

		final String pcjTableName = new PcjTableNameFactory().makeTableName(
				prefix, "testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age" },
				Optional.<PcjVarOrderFactory> absent());

		final AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn,
				pcjTableName);

		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("birthDate", new LiteralImpl("1983-03-17", new URIImpl(
				"http://www.w3.org/2001/XMLSchema#date")));
		bs.addBinding("location", new URIImpl("http://Virginia"));

		final QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("birthDate", new LiteralImpl("1983-04-18", new URIImpl(
				"http://www.w3.org/2001/XMLSchema#date")));
		bs2.addBinding("location", new URIImpl("http://Georgia"));

		final Set<BindingSet> bSets = Sets.<BindingSet> newHashSet(bs, bs2);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		final QueryBindingSet alice1 = new QueryBindingSet();
		alice1.addBinding("name", new URIImpl("http://Alice"));
		alice1.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		alice1.addAll(bs);

		final QueryBindingSet bob1 = new QueryBindingSet();
		bob1.addBinding("name", new URIImpl("http://Bob"));
		bob1.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		bob1.addAll(bs);

		final QueryBindingSet charlie1 = new QueryBindingSet();
		charlie1.addBinding("name", new URIImpl("http://Charlie"));
		charlie1.addBinding("age",
				new NumericLiteralImpl(12, XMLSchema.INTEGER));
		charlie1.addAll(bs);

		final QueryBindingSet alice2 = new QueryBindingSet();
		alice2.addBinding("name", new URIImpl("http://Alice"));
		alice2.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		alice2.addAll(bs2);

		final QueryBindingSet bob2 = new QueryBindingSet();
		bob2.addBinding("name", new URIImpl("http://Bob"));
		bob2.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		bob2.addAll(bs2);

		final QueryBindingSet charlie2 = new QueryBindingSet();
		charlie2.addBinding("name", new URIImpl("http://Charlie"));
		charlie2.addBinding("age",
				new NumericLiteralImpl(12, XMLSchema.INTEGER));
		charlie2.addAll(bs2);

		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			final BindingSet next = results.next();
			System.out.println(next);
			fetchedResults.add(next);
		}

		Assert.assertEquals(Sets.<BindingSet> newHashSet(alice1, bob1,
				charlie1, alice2, bob2, charlie2), fetchedResults);
	}

	@Test
	public void variableNormalizationTest() throws RepositoryException,
			PcjException, TableNotFoundException, RyaTypeResolverException,
			MalformedQueryException, SailException, QueryEvaluationException,
			MutationsRejectedException {
		// Load some Triples into Rya.
		final Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://hasAge"), new NumericLiteralImpl(12,
						XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));

		for (final Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		final String sparql = "SELECT ?name ?age " + "{"
				+ "FILTER(?age < 30) ." + "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> \"Soccer\" " + "}";

		final String pcjTableName = new PcjTableNameFactory().makeTableName(
				prefix, "testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age" },
				Optional.<PcjVarOrderFactory> absent());

		final String sparql2 = "SELECT ?x ?y " + "{" + "FILTER(?y < 30) ."
				+ "?x <http://hasAge> ?y."
				+ "?x <http://playsSport> \"Soccer\" " + "}";

		final SPARQLParser p = new SPARQLParser();
		final ParsedQuery pq = p.parseQuery(sparql2, null);

		final Map<String, String> map = new HashMap<>();
		map.put("x", "name");
		map.put("y", "age");
		final AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn,
				pcjTableName);
		ais.setProjectionExpr((Projection) pq.getTupleExpr());
		ais.setTableVarMap(map);
		ais.setSupportedVariableOrderMap(Lists.<String> newArrayList("x;y",
				"y;x"));

		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("birthDate", new LiteralImpl("1983-03-17", new URIImpl(
				"http://www.w3.org/2001/XMLSchema#date")));
		bs.addBinding("x", new URIImpl("http://Alice"));

		final QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("birthDate", new LiteralImpl("1983-04-18", new URIImpl(
				"http://www.w3.org/2001/XMLSchema#date")));
		bs2.addBinding("x", new URIImpl("http://Bob"));

		final Set<BindingSet> bSets = Sets.<BindingSet> newHashSet(bs, bs2);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		final QueryBindingSet alice = new QueryBindingSet();
		alice.addBinding("x", new URIImpl("http://Alice"));
		alice.addBinding("y", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		alice.addBinding("birthDate", new LiteralImpl("1983-03-17",
				new URIImpl("http://www.w3.org/2001/XMLSchema#date")));

		final QueryBindingSet bob = new QueryBindingSet();
		bob.addBinding("x", new URIImpl("http://Bob"));
		bob.addBinding("y", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		bob.addBinding("birthDate", new LiteralImpl("1983-04-18", new URIImpl(
				"http://www.w3.org/2001/XMLSchema#date")));

		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			final BindingSet next = results.next();
			System.out.println(next);
			fetchedResults.add(next);
		}

		Assert.assertEquals(Sets.<BindingSet> newHashSet(alice, bob),
				fetchedResults);
	}

	@Test
	public void variableInstantiationTest() throws Exception {
		// Load some Triples into Rya.
		final Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://hasAge"), new NumericLiteralImpl(12,
						XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Charlie"),
				new URIImpl("http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Eve"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));

		for (final Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		final String sparql = "SELECT ?name ?age " + "{"
				+ "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> \"Soccer\" " + "}";

		final String pcjTableName = new PcjTableNameFactory().makeTableName(
				prefix, "testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age" },
				Optional.<PcjVarOrderFactory> absent());

		final String sparql2 = "SELECT ?x " + "{" + "?x <http://hasAge> 16 ."
				+ "?x <http://playsSport> \"Soccer\" " + "}";

		final SPARQLParser p = new SPARQLParser();
		final ParsedQuery pq1 = p.parseQuery(sparql, null);
		final ParsedQuery pq2 = p.parseQuery(sparql2, null);

		final AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn,
				pcjTableName);
		ais.setProjectionExpr((Projection) QueryVariableNormalizer
				.getNormalizedIndex(pq2.getTupleExpr(), pq1.getTupleExpr())
				.get(0));

		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("birthDate", new LiteralImpl("1983-03-17", new URIImpl(
				"http://www.w3.org/2001/XMLSchema#date")));
		bs.addBinding("x", new URIImpl("http://Alice"));

		final QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("birthDate", new LiteralImpl("1983-04-18", new URIImpl(
				"http://www.w3.org/2001/XMLSchema#date")));
		bs2.addBinding("x", new URIImpl("http://Bob"));

		final Set<BindingSet> bSets = Sets.<BindingSet> newHashSet(bs, bs2);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			final BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(Sets.<BindingSet> newHashSet(bs2), fetchedResults);
	}

	@Test
	public void accumuloIndexSetTestAttemptJoinAccrossTypes() throws Exception {
		// Load some Triples into Rya.
		final Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));

		for (final Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		final String sparql = "SELECT ?name ?age " + "{"
				+ "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> \"Soccer\" " + "}";

		final String pcjTableName = new PcjTableNameFactory().makeTableName(
				prefix, "testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age" },
				Optional.<PcjVarOrderFactory> absent());
		AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn, pcjTableName);

		final QueryBindingSet bs1 = new QueryBindingSet();
		bs1.addBinding("age", new LiteralImpl("16"));
		final QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));

		final Set<BindingSet> bSets = Sets.<BindingSet> newHashSet(bs1, bs2);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			final BindingSet next = results.next();
			fetchedResults.add(next);
		}

		bs2.addBinding("name", new URIImpl("http://Alice"));
		Assert.assertEquals(Sets.<BindingSet> newHashSet(bs2), fetchedResults);
	}

	@Test
	public void optionalBindingSetTest() throws Exception {
		// Load some Triples into Rya.
		final Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(32, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));

		for (final Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		final String sparql = "SELECT ?name ?age " + "{"
				+ "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> \"Soccer\" " + "}";

		final String pcjTableName = new PcjTableNameFactory().makeTableName(
				prefix, "testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age" },
				Optional.<PcjVarOrderFactory> absent());
		AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn, pcjTableName);

		final QueryBindingSet bs1 = new QueryBindingSet();
		bs1.addBinding("name", new URIImpl("http://Alice"));
		final QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("name", new URIImpl("http://Bob"));
		bs2.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));

		final List<BindingSet> bSets = Lists
				.<BindingSet> newArrayList(bs1, bs2);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		Binding exBinding1 = new BindingImpl("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		Binding exBinding2 = new BindingImpl("name", new URIImpl("http://Alice"));
		Binding exBinding5 = new BindingImpl("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		Binding exBinding6 = new BindingImpl("name", new URIImpl("http://Bob"));

		QueryBindingSet pcjBs1 = new QueryBindingSet();
		pcjBs1.addBinding(exBinding1);
		pcjBs1.addBinding(exBinding2);

		QueryBindingSet pcjBs2 = new QueryBindingSet();
		pcjBs2.addBinding(exBinding5);
		pcjBs2.addBinding(exBinding6);

		Set<BindingSet> expectedResults = new HashSet<>();
		expectedResults.add(pcjBs1);
		expectedResults.add(pcjBs2);

		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			final BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expectedResults, fetchedResults);
	}

	@Test
	public void variableCommonVarOrderLenWithCrossProductTest()
			throws Exception {
		// Load some Triples into Rya.
		final Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(32, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));

		for (final Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		final String sparql = "SELECT ?name ?age " + "{"
				+ "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> \"Soccer\" " + "}";

		final String pcjTableName = new PcjTableNameFactory().makeTableName(
				prefix, "testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age" },
				Optional.<PcjVarOrderFactory> absent());
		AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn, pcjTableName);

		QueryBindingSet bs1 = new QueryBindingSet();
		bs1.addBinding("name", new URIImpl("http://Alice"));
		QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("name", new URIImpl("http://Bob"));
		bs2.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		QueryBindingSet bs3 = new QueryBindingSet();
		bs3.addBinding("location", new URIImpl("http://Virginia"));

		final List<BindingSet> bSets = Lists.<BindingSet> newArrayList(bs1,
				bs2, bs3);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		Binding exBinding1 = new BindingImpl("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		Binding exBinding2 = new BindingImpl("name", new URIImpl("http://Alice"));
		Binding exBinding5 = new BindingImpl("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		Binding exBinding6 = new BindingImpl("name", new URIImpl("http://Bob"));
		Binding exBinding3 = new BindingImpl("age", new NumericLiteralImpl(32, XMLSchema.INTEGER));
		Binding exBinding4 = new BindingImpl("name", new URIImpl("http://Bob"));

		QueryBindingSet pcjBs1 = new QueryBindingSet();
		pcjBs1.addBinding(exBinding1);
		pcjBs1.addBinding(exBinding2);

		QueryBindingSet pcjBs2 = new QueryBindingSet();
		pcjBs2.addBinding(exBinding5);
		pcjBs2.addBinding(exBinding6);

		QueryBindingSet pcjBs3 = new QueryBindingSet();
		pcjBs3.addBinding(exBinding3);
		pcjBs3.addBinding(exBinding4);

		QueryBindingSet exBs1 = new QueryBindingSet(bs3);
		exBs1.addAll(pcjBs1);
		QueryBindingSet exBs2 = new QueryBindingSet(bs3);
		exBs2.addAll(pcjBs2);
		QueryBindingSet exBs3 = new QueryBindingSet(bs3);
		exBs3.addAll(pcjBs3);

		Set<BindingSet> expectedResults = new HashSet<>();
		expectedResults.add(exBs1);
		expectedResults.add(exBs2);
		expectedResults.add(exBs3);
		expectedResults.add(pcjBs1);
		expectedResults.add(pcjBs2);


		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expectedResults, fetchedResults);
	}

	@Test
	public void variableCommonVarOrderLenCrossProductTestWithConstantInstantiation()
			throws Exception {
		// Load some Triples into Rya.
		final Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(32, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));

		for (final Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		final String sparql = "SELECT ?name ?age " + "{"
				+ "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> \"Soccer\" " + "}";

		final String pcjTableName = new PcjTableNameFactory().makeTableName(
				prefix, "testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age" },
				Optional.<PcjVarOrderFactory> absent());

		final String sparql2 = "SELECT ?x " + "{" + "?x <http://hasAge> 16 ."
				+ "?x <http://playsSport> \"Soccer\" " + "}";

		final SPARQLParser p = new SPARQLParser();
		final ParsedQuery pq1 = p.parseQuery(sparql, null);
		final ParsedQuery pq2 = p.parseQuery(sparql2, null);

		final AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn,
				pcjTableName);
		ais.setProjectionExpr((Projection) QueryVariableNormalizer
				.getNormalizedIndex(pq2.getTupleExpr(), pq1.getTupleExpr())
				.get(0));

		QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("x", new URIImpl("http://Bob"));
		QueryBindingSet bs3 = new QueryBindingSet();
		bs3.addBinding("location", new URIImpl("http://Virginia"));

		final List<BindingSet> bSets = Lists.<BindingSet> newArrayList(bs2, bs3);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		Binding exBinding6 = new BindingImpl("x", new URIImpl("http://Bob"));

		QueryBindingSet pcjBs2 = new QueryBindingSet();
		pcjBs2.addBinding(exBinding6);

		QueryBindingSet exBs4 = new QueryBindingSet(bs3);
		exBs4.addAll(pcjBs2);

		Set<BindingSet> expectedResults = new HashSet<>();
		expectedResults.add(exBs4);
		expectedResults.add(pcjBs2);

		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expectedResults, fetchedResults);
	}



	@Test
	public void variableCommonVarOrderLenCrossProductTestWithConstantInstantiation2() throws Exception {
		// Load some Triples into Rya.
		Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Baseball")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(185,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(125,
				XMLSchema.INTEGER)));

		for (Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		String sparql = "SELECT ?name ?age ?sport ?weight " + "{"
				+ "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> ?sport . "
				+ "?name <http://hasWeight> ?weight " + "}";

		String sparql2 = "SELECT ?x ?y ?z " + "{" + "?x <http://hasAge> ?y."
				+ "?x <http://playsSport> \"Baseball\" . "
				+ "?x <http://hasWeight> ?z " + "}";

		String pcjTableName = new PcjTableNameFactory().makeTableName(prefix,
				"testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age", "sport",
						"weight" }, Optional.<PcjVarOrderFactory> absent());

		SPARQLParser p = new SPARQLParser();
		ParsedQuery pq1 = p.parseQuery(sparql, null);
		ParsedQuery pq2 = p.parseQuery(sparql2, null);

		AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn, pcjTableName);
		ais.setProjectionExpr((Projection) QueryVariableNormalizer
				.getNormalizedIndex(pq2.getTupleExpr(), pq1.getTupleExpr())
				.get(0));

		QueryBindingSet bs1 = new QueryBindingSet();
		bs1.addBinding("x", new URIImpl("http://Bob"));
		QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("x", new URIImpl("http://Alice"));
		bs2.addBinding("y", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		QueryBindingSet bs3 = new QueryBindingSet();
		bs3.addBinding("location", new URIImpl("http://Virginia"));

		final List<BindingSet> bSets = Lists.<BindingSet> newArrayList(bs1, bs2, bs3);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		Binding exBinding5 = new BindingImpl("y", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		Binding exBinding6 = new BindingImpl("x", new URIImpl("http://Bob"));
		Binding exBinding8 = new BindingImpl("z", new NumericLiteralImpl(185, XMLSchema.INTEGER));

		QueryBindingSet pcjBs2 = new QueryBindingSet();
		pcjBs2.addBinding(exBinding5);
		pcjBs2.addBinding(exBinding6);
		pcjBs2.addBinding(exBinding8);

		QueryBindingSet exBs4 = new QueryBindingSet(bs3);
		exBs4.addAll(pcjBs2);

		Set<BindingSet> expectedResults = new HashSet<>();
		expectedResults.add(exBs4);
		expectedResults.add(pcjBs2);

		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expectedResults, fetchedResults);
	}





	@Test
	public void partiallySupportedVarOrderTest() throws Exception {
		// Load some Triples into Rya.
		Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Baseball")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(185,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(125,
				XMLSchema.INTEGER)));

		for (Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		String sparql = "SELECT ?name ?age ?sport ?weight " + "{"
				+ "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> ?sport . "
				+ "?name <http://hasWeight> ?weight " + "}";

		String pcjTableName = new PcjTableNameFactory().makeTableName(prefix,
				"testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age", "sport",
						"weight" }, Optional.<PcjVarOrderFactory> absent());
		AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn, pcjTableName);

		QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("name", new URIImpl("http://Bob"));
		bs.addBinding("sport", new LiteralImpl("Baseball"));

		final List<BindingSet> bSets = Lists.<BindingSet> newArrayList(bs);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		Binding exBinding5 = new BindingImpl("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		Binding exBinding6 = new BindingImpl("name", new URIImpl("http://Bob"));
		Binding exBinding7 = new BindingImpl("sport", new LiteralImpl("Baseball"));
		Binding exBinding8 = new BindingImpl("weight", new NumericLiteralImpl(185, XMLSchema.INTEGER));


		QueryBindingSet pcjBs2 = new QueryBindingSet();
		pcjBs2.addBinding(exBinding5);
		pcjBs2.addBinding(exBinding6);
		pcjBs2.addBinding(exBinding7);
		pcjBs2.addBinding(exBinding8);

		QueryBindingSet exBs3 = new QueryBindingSet(bs);
		exBs3.addAll(pcjBs2);

		Set<BindingSet> expectedResults = new HashSet<>();
		expectedResults.add(exBs3);

		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expectedResults, fetchedResults);
	}

	@Test
	public void partiallySupportedVarOrderTestWithConstants() throws Exception {
		// Load some Triples into Rya.
		Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Baseball")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(185,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(125,
				XMLSchema.INTEGER)));

		for (Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		String sparql = "SELECT ?name ?age ?sport ?weight " + "{"
				+ "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> ?sport . "
				+ "?name <http://hasWeight> ?weight " + "}";

		String sparql2 = "SELECT ?x ?y ?z " + "{" + "?x <http://hasAge> ?y."
				+ "?x <http://playsSport> \"Baseball\" . "
				+ "?x <http://hasWeight> ?z " + "}";

		String pcjTableName = new PcjTableNameFactory().makeTableName(prefix,
				"testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age", "sport",
						"weight" }, Optional.<PcjVarOrderFactory> absent());

		SPARQLParser p = new SPARQLParser();
		ParsedQuery pq1 = p.parseQuery(sparql, null);
		ParsedQuery pq2 = p.parseQuery(sparql2, null);

		AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn, pcjTableName);
		ais.setProjectionExpr((Projection) QueryVariableNormalizer
				.getNormalizedIndex(pq2.getTupleExpr(), pq1.getTupleExpr())
				.get(0));

		QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("x", new URIImpl("http://Bob"));

		final List<BindingSet> bSets = Lists.<BindingSet> newArrayList(bs);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		Binding exBinding5 = new BindingImpl("y", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		Binding exBinding6 = new BindingImpl("x", new URIImpl("http://Bob"));
		Binding exBinding8 = new BindingImpl("z", new NumericLiteralImpl(185, XMLSchema.INTEGER));


		QueryBindingSet pcjBs2 = new QueryBindingSet();
		pcjBs2.addBinding(exBinding5);
		pcjBs2.addBinding(exBinding6);
		pcjBs2.addBinding(exBinding8);

		QueryBindingSet exBs3 = new QueryBindingSet(bs);
		exBs3.addAll(pcjBs2);

		Set<BindingSet> expectedResults = new HashSet<>();
		expectedResults.add(exBs3);


		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expectedResults, fetchedResults);
	}






	@Test
	public void partiallySupportedConstantOrderNoBindingSet() throws Exception {
		// Load some Triples into Rya.
		Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Baseball")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(185,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(125,
				XMLSchema.INTEGER)));

		for (Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		String sparql = "SELECT ?name ?age ?sport ?weight " + "{"
				+ "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> ?sport . "
				+ "?name <http://hasWeight> ?weight " + "}";

		String sparql2 = "SELECT ?y ?z " + "{"
				+ "<http://Bob> <http://hasAge> ?y."
				+ "<http://Bob> <http://playsSport> \"Baseball\" . "
				+ "<http://Bob> <http://hasWeight> ?z " + "}";

		String pcjTableName = new PcjTableNameFactory().makeTableName(prefix,
				"testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age", "sport",
						"weight" }, Optional.<PcjVarOrderFactory> absent());

		SPARQLParser p = new SPARQLParser();
		ParsedQuery pq1 = p.parseQuery(sparql, null);
		ParsedQuery pq2 = p.parseQuery(sparql2, null);

		AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn, pcjTableName);
		ais.setProjectionExpr((Projection) QueryVariableNormalizer
				.getNormalizedIndex(pq2.getTupleExpr(), pq1.getTupleExpr())
				.get(0));

		QueryBindingSet bs = new QueryBindingSet();
		List<BindingSet> bSets = Lists.<BindingSet> newArrayList(bs);

		CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		Binding exBinding5 = new BindingImpl("y", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		Binding exBinding8 = new BindingImpl("z", new NumericLiteralImpl(185, XMLSchema.INTEGER));

		QueryBindingSet pcjBs2 = new QueryBindingSet();
		pcjBs2.addBinding(exBinding5);
		pcjBs2.addBinding(exBinding8);


		Set<BindingSet> expectedResults = new HashSet<>();
		expectedResults.add(pcjBs2);

		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expectedResults, fetchedResults);
	}

	@Test
	public void singleCrossProductBindingSetWithConstantConstraintsTest()
			throws Exception {
		// Load some Triples into Rya.
		Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Baseball")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(185,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(125,
				XMLSchema.INTEGER)));

		for (Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		String sparql = "SELECT ?name ?age ?sport ?weight " + "{"
				+ "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> ?sport . "
				+ "?name <http://hasWeight> ?weight " + "}";

		String sparql2 = "SELECT ?x ?y ?z " + "{" + "?x <http://hasAge> ?y."
				+ "?x <http://playsSport> \"Soccer\" . "
				+ "?x <http://hasWeight> ?z " + "}";

		String pcjTableName = new PcjTableNameFactory().makeTableName(prefix,
				"testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age", "sport",
						"weight" }, Optional.<PcjVarOrderFactory> absent());
		Scanner scanner = accumuloConn.createScanner(pcjTableName,
				new Authorizations());
		for (Map.Entry<Key, org.apache.accumulo.core.data.Value> e : scanner) {
			System.out.println(e.getKey().getRow());
		}

		SPARQLParser p = new SPARQLParser();
		ParsedQuery pq1 = p.parseQuery(sparql, null);
		ParsedQuery pq2 = p.parseQuery(sparql2, null);

		AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn, pcjTableName);
		ais.setProjectionExpr((Projection) QueryVariableNormalizer
				.getNormalizedIndex(pq2.getTupleExpr(), pq1.getTupleExpr())
				.get(0));

		QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("location", new URIImpl("http://Virginia"));
		bs.addBinding("profession", new LiteralImpl("Plumber"));
		QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("location", new URIImpl("http://California"));
		bs2.addBinding("profession", new LiteralImpl("Doctor"));

		final List<BindingSet> bSets = Lists.<BindingSet> newArrayList(bs, bs2);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);


		Binding exBinding1 = new BindingImpl("y", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		Binding exBinding2 = new BindingImpl("x", new URIImpl("http://Alice"));
		Binding exBinding4 = new BindingImpl("z", new NumericLiteralImpl(125, XMLSchema.INTEGER));

		QueryBindingSet pcjBs1 = new QueryBindingSet();
		pcjBs1.addBinding(exBinding1);
		pcjBs1.addBinding(exBinding2);
		pcjBs1.addBinding(exBinding4);

		QueryBindingSet exBs1 = new QueryBindingSet(bs);
		exBs1.addAll(pcjBs1);
		QueryBindingSet exBs2 = new QueryBindingSet(bs2);
		exBs2.addAll(pcjBs1);

		Set<BindingSet> expectedResults = new HashSet<>();
		expectedResults.add(exBs1);
		expectedResults.add(exBs2);

		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expectedResults, fetchedResults);
	}

	@Test
	public void singleCrossProductBindingSetTest() throws Exception {
		// Load some Triples into Rya.
		Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Baseball")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(185,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(125,
				XMLSchema.INTEGER)));

		for (Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		String sparql = "SELECT ?name ?age ?sport ?weight " + "{"
				+ "?name <http://hasAge> ?age."
				+ "?name <http://playsSport> ?sport . "
				+ "?name <http://hasWeight> ?weight " + "}";

		String pcjTableName = new PcjTableNameFactory().makeTableName(prefix,
				"testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age", "sport",
						"weight" }, Optional.<PcjVarOrderFactory> absent());
		// Scanner scanner = accumuloConn.createScanner(pcjTableName,
		// new Authorizations());
		// for (Map.Entry<Key, org.apache.accumulo.core.data.Value> e : scanner)
		// {
		// System.out.println(e.getKey().getRow());
		// }
		AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn, pcjTableName);

		QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("location", new URIImpl("http://Virginia"));
		bs.addBinding("profession", new LiteralImpl("Plumber"));
		QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("location", new URIImpl("http://California"));
		bs2.addBinding("profession", new LiteralImpl("Doctor"));

		List<BindingSet> bSets = Lists.<BindingSet> newArrayList(bs, bs2);

		CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		Binding exBinding1 = new BindingImpl("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		Binding exBinding2 = new BindingImpl("name", new URIImpl("http://Alice"));
		Binding exBinding3 = new BindingImpl("sport", new LiteralImpl("Soccer") );
		Binding exBinding4 = new BindingImpl("weight", new NumericLiteralImpl(125, XMLSchema.INTEGER));
		Binding exBinding5 = new BindingImpl("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		Binding exBinding6 = new BindingImpl("name", new URIImpl("http://Bob"));
		Binding exBinding7 = new BindingImpl("sport", new LiteralImpl("Baseball"));
		Binding exBinding8 = new BindingImpl("weight", new NumericLiteralImpl(185, XMLSchema.INTEGER));

		QueryBindingSet pcjBs1 = new QueryBindingSet();
		pcjBs1.addBinding(exBinding1);
		pcjBs1.addBinding(exBinding2);
		pcjBs1.addBinding(exBinding3);
		pcjBs1.addBinding(exBinding4);

		QueryBindingSet pcjBs2 = new QueryBindingSet();
		pcjBs2.addBinding(exBinding5);
		pcjBs2.addBinding(exBinding6);
		pcjBs2.addBinding(exBinding7);
		pcjBs2.addBinding(exBinding8);

		QueryBindingSet exBs1 = new QueryBindingSet(bs);
		exBs1.addAll(pcjBs1);
		QueryBindingSet exBs2 = new QueryBindingSet(bs2);
		exBs2.addAll(pcjBs1);
		QueryBindingSet exBs3 = new QueryBindingSet(bs);
		exBs3.addAll(pcjBs2);
		QueryBindingSet exBs4 = new QueryBindingSet(bs2);
		exBs4.addAll(pcjBs2);

		Set<BindingSet> expectedResults = new HashSet<>();
		expectedResults.add(exBs1);
		expectedResults.add(exBs2);
		expectedResults.add(exBs3);
		expectedResults.add(exBs4);

		Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expectedResults, fetchedResults);
	}


	@Test
	public void variableBindingSetOptionalQueryTest() throws Exception {
		// Load some Triples into Rya.
		Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Baseball")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(185,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(125,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Joe"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(235,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Joe"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(42, XMLSchema.INTEGER)));

		for (Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		String sparql = "SELECT ?name ?age ?sport ?weight " + "{"
				+ "?name <http://hasAge> ?age."
				+ "OPTIONAL{?name <http://playsSport> ?sport} . "
				+ "?name <http://hasWeight> ?weight " + "}";

		String pcjTableName = new PcjTableNameFactory().makeTableName(prefix,
				"testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age", "sport",
						"weight" }, Optional.<PcjVarOrderFactory> absent());
		// Scanner scanner = accumuloConn.createScanner(pcjTableName,
		// new Authorizations());
		// for (Map.Entry<Key, org.apache.accumulo.core.data.Value> e : scanner)
		// {
		// System.out.println(e.getKey().getRow());
		// }
		AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn, pcjTableName);

		QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("name", new URIImpl("http://Joe"));
		bs.addBinding("age", new NumericLiteralImpl(42, XMLSchema.INTEGER));
		bs.addBinding("weight", new NumericLiteralImpl(235,
				XMLSchema.INTEGER));
		QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("name", new URIImpl("http://Alice"));
		bs2.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		bs2.addBinding("weight", new NumericLiteralImpl(125,
				XMLSchema.INTEGER));
		bs2.addBinding("sport", new LiteralImpl("Soccer"));
		QueryBindingSet bs3 = new QueryBindingSet();
		bs3.addBinding("name", new URIImpl("http://Bob"));
		bs3.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		bs3.addBinding("weight", new NumericLiteralImpl(185,
				XMLSchema.INTEGER));
		bs3.addBinding("sport", new LiteralImpl("Baseball"));


		List<BindingSet> bSets = Lists.<BindingSet> newArrayList(bs);
		bSets.add(bs2);
		bSets.add(bs3);

		CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		Set<BindingSet> expectedResults = new HashSet<>();
		expectedResults.add(bs);
		expectedResults.add(bs2);
		expectedResults.add(bs3);

		Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expectedResults, fetchedResults);
	}





	@Test
	public void joinOnOptionalVariableTest() throws Exception {
		// Load some Triples into Rya.
		Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Baseball")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(185,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(125,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Joe"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(235,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Joe"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(42, XMLSchema.INTEGER)));

		for (Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		String sparql = "SELECT ?name ?age ?sport ?weight " + "{"
				+ "?name <http://hasAge> ?age."
				+ "OPTIONAL{?name <http://playsSport> ?sport} . "
				+ "?name <http://hasWeight> ?weight " + "}";

		String pcjTableName = new PcjTableNameFactory().makeTableName(prefix,
				"testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age", "sport",
						"weight" }, Optional.<PcjVarOrderFactory> absent());
		// Scanner scanner = accumuloConn.createScanner(pcjTableName,
		// new Authorizations());
		// for (Map.Entry<Key, org.apache.accumulo.core.data.Value> e : scanner)
		// {
		// System.out.println(e.getKey().getRow());
		// }
		AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn, pcjTableName);

		QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("name", new URIImpl("http://Joe"));
		bs.addBinding("sport", new LiteralImpl("Football"));
		QueryBindingSet bs4 = new QueryBindingSet();
		bs4.addBinding("name", new URIImpl("http://Alice"));
		bs4.addBinding("sport", new LiteralImpl("Soccer"));
		QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("sport", new LiteralImpl("Hockey"));
		bs2.addBinding("profession", new LiteralImpl("Doctor"));
		QueryBindingSet bs3 = new QueryBindingSet();
		bs3.addBinding("sport", new LiteralImpl("Basketball"));

		List<BindingSet> bSets = Lists.<BindingSet> newArrayList(bs);
		bSets.add(bs2);
		bSets.add(bs3);
		bSets.add(bs4);

		CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		Binding exBinding1 = new BindingImpl("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		Binding exBinding2 = new BindingImpl("name", new URIImpl("http://Alice"));
		Binding exBinding3 = new BindingImpl("sport", new LiteralImpl("Soccer") );
		Binding exBinding4 = new BindingImpl("weight", new NumericLiteralImpl(125, XMLSchema.INTEGER));
		Binding exBinding5 = new BindingImpl("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		Binding exBinding6 = new BindingImpl("name", new URIImpl("http://Bob"));
		Binding exBinding8 = new BindingImpl("weight", new NumericLiteralImpl(185, XMLSchema.INTEGER));
		Binding exBinding9 = new BindingImpl("age", new NumericLiteralImpl(42, XMLSchema.INTEGER));
		Binding exBinding10 = new BindingImpl("name", new URIImpl("http://Joe"));
		Binding exBinding11 = new BindingImpl("weight", new NumericLiteralImpl(235, XMLSchema.INTEGER));

		QueryBindingSet pcjBs1 = new QueryBindingSet();
		pcjBs1.addBinding(exBinding1);
		pcjBs1.addBinding(exBinding2);
		pcjBs1.addBinding(exBinding4);

		QueryBindingSet pcjBs2 = new QueryBindingSet();
		pcjBs2.addBinding(exBinding5);
		pcjBs2.addBinding(exBinding6);
		pcjBs2.addBinding(exBinding8);

		QueryBindingSet pcjBs3 = new QueryBindingSet();
		pcjBs3.addBinding(exBinding9);
		pcjBs3.addBinding(exBinding10);
		pcjBs3.addBinding(exBinding11);

		QueryBindingSet exBs1 = new QueryBindingSet(bs2);
		exBs1.addAll(pcjBs1);
		QueryBindingSet exBs2 = new QueryBindingSet(bs2);
		exBs2.addAll(pcjBs2);
		QueryBindingSet exBs3 = new QueryBindingSet(bs2);
		exBs3.addAll(pcjBs3);
		QueryBindingSet exBs4 = new QueryBindingSet(bs3);
		exBs4.addAll(pcjBs1);
		QueryBindingSet exBs5 = new QueryBindingSet(bs3);
		exBs5.addAll(pcjBs2);
		QueryBindingSet exBs6 = new QueryBindingSet(bs3);
		exBs6.addAll(pcjBs3);
		QueryBindingSet exBs7 = new QueryBindingSet(pcjBs3);
		exBs7.addBinding("sport", new LiteralImpl("Football"));

		Set<BindingSet> expectedResults = new HashSet<>();
		expectedResults.add(exBs1);
		expectedResults.add(exBs2);
		expectedResults.add(exBs3);
		expectedResults.add(exBs4);
		expectedResults.add(exBs5);
		expectedResults.add(exBs6);
		expectedResults.add(exBs7);

		pcjBs1.addBinding(exBinding3);
		expectedResults.add(pcjBs1);

		Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expectedResults, fetchedResults);
	}




	@Test
	public void joinOnOptionalVariableTest2() throws Exception {
		// Load some Triples into Rya.
		Set<Statement> triples = new HashSet<>();
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Soccer")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://playsSport"), new LiteralImpl("Baseball")));
		triples.add(new StatementImpl(new URIImpl("http://Bob"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(185,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Alice"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(125,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Joe"), new URIImpl(
				"http://hasWeight"), new NumericLiteralImpl(235,
				XMLSchema.INTEGER)));
		triples.add(new StatementImpl(new URIImpl("http://Joe"), new URIImpl(
				"http://hasAge"), new NumericLiteralImpl(42, XMLSchema.INTEGER)));

		for (Statement triple : triples) {
			ryaConn.add(triple);
		}

		// Create a PCJ table will include those triples in its results.
		String sparql = "SELECT ?name ?age ?sport ?weight " + "{"
				+ "?name <http://hasAge> ?age."
				+ "OPTIONAL{?name <http://playsSport> ?sport} . "
				+ "?name <http://hasWeight> ?weight " + "}";

		String pcjTableName = new PcjTableNameFactory().makeTableName(prefix,
				"testPcj");

		// Create and populate the PCJ table.
		PcjIntegrationTestingUtil.createAndPopulatePcj(ryaConn, accumuloConn,
				pcjTableName, sparql, new String[] { "name", "age", "sport",
						"weight" }, Optional.<PcjVarOrderFactory> absent());
		// Scanner scanner = accumuloConn.createScanner(pcjTableName,
		// new Authorizations());
		// for (Map.Entry<Key, org.apache.accumulo.core.data.Value> e : scanner)
		// {
		// System.out.println(e.getKey().getRow());
		// }
		AccumuloIndexSet ais = new AccumuloIndexSet(accumuloConn, pcjTableName);

		QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("name", new URIImpl("http://Alice"));
		bs.addBinding("sport", new LiteralImpl("Basketball"));

		List<BindingSet> bSets = Lists.<BindingSet> newArrayList(bs);

		CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		Binding exBinding1 = new BindingImpl("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));
		Binding exBinding2 = new BindingImpl("name", new URIImpl("http://Alice"));
		Binding exBinding4 = new BindingImpl("weight", new NumericLiteralImpl(125, XMLSchema.INTEGER));

		QueryBindingSet pcjBs1 = new QueryBindingSet();
		pcjBs1.addBinding(exBinding1);
		pcjBs1.addBinding(exBinding2);
		pcjBs1.addBinding(exBinding4);

		QueryBindingSet exBs1 = new QueryBindingSet(bs);
		exBs1.addAll(pcjBs1);

		Set<BindingSet> expectedResults = new HashSet<>();
		expectedResults.add(exBs1);

		Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expectedResults, fetchedResults);
	}






	@After
	public void close() throws RepositoryException {
		ryaConn.close();
		ryaRepo.shutDown();
	}

	private static Configuration getConf() {
		final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
		conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
		conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, "rya_");
		conf.set(ConfigUtils.CLOUDBASE_USER, "root");
		conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
		conf.set(ConfigUtils.CLOUDBASE_INSTANCE, "instance");
		conf.set(ConfigUtils.CLOUDBASE_AUTHS, "");
		return conf;
	}

}
