package mvm.rya.indexing.external.tupleSet;
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
import info.aduna.iteration.CloseableIteration;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.external.accumulo.AccumuloPcjStorage;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.impl.NumericLiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.repository.RepositoryException;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class AccumuloIndexSetColumnVisibilityTest {

	private static final Logger log = Logger
			.getLogger(AccumuloIndexSetColumnVisibilityTest.class);

	private static Connector accCon;
	private static String pcjTableName;
	private static AccumuloPcjStorage storage;
	private static Configuration conf;
	private static final String sparql = "SELECT ?name ?age " + "{"
			+ "?name <http://hasAge> ?age ."
			+ "?name <http://playsSport> \"Soccer\" " + "}";
	private static QueryBindingSet pcjBs1, pcjBs2, pcjBs3;
	private static MiniAccumuloCluster accumulo;
	private static String instance;
	private static String zooKeepers;

	@BeforeClass
	public static void init() throws AccumuloException,
			AccumuloSecurityException, PCJStorageException, IOException,
			InterruptedException, TableNotFoundException {
		accumulo = startMiniAccumulo();
		accumulo.getZooKeepers();
		instance = accumulo.getInstanceName();
		zooKeepers = accumulo.getZooKeepers();
		conf = getConf();
		accCon.securityOperations().changeUserAuthorizations("root", new Authorizations("U","USA"));
		storage = new AccumuloPcjStorage(accCon, "rya_");
		Set<VariableOrder> varOrders = new HashSet<>();
		varOrders.add(new VariableOrder("age;name"));
		varOrders.add(new VariableOrder("name;age"));
		pcjTableName = storage.createPcj(sparql, varOrders);

		Binding exBinding1 = new BindingImpl("age", new NumericLiteralImpl(14,
				XMLSchema.INTEGER));
		Binding exBinding2 = new BindingImpl("name",
				new URIImpl("http://Alice"));
		Binding exBinding3 = new BindingImpl("age", new NumericLiteralImpl(16,
				XMLSchema.INTEGER));
		Binding exBinding4 = new BindingImpl("name", new URIImpl("http://Bob"));
		Binding exBinding5 = new BindingImpl("age", new NumericLiteralImpl(34,
				XMLSchema.INTEGER));
		Binding exBinding6 = new BindingImpl("name", new URIImpl("http://Joe"));

		pcjBs1 = new QueryBindingSet();
		pcjBs1.addBinding(exBinding1);
		pcjBs1.addBinding(exBinding2);

		pcjBs2 = new QueryBindingSet();
		pcjBs2.addBinding(exBinding3);
		pcjBs2.addBinding(exBinding4);

		pcjBs3 = new QueryBindingSet();
		pcjBs3.addBinding(exBinding5);
		pcjBs3.addBinding(exBinding6);

		Set<BindingSet> bindingSets = new HashSet<>();
		bindingSets.add(pcjBs1);
		bindingSets.add(pcjBs2);
		bindingSets.add(pcjBs3);

		Set<VisibilityBindingSet> visBs = new HashSet<>();
		for (BindingSet bs : bindingSets) {
			visBs.add(new VisibilityBindingSet(bs, "U|USA"));
		}

		storage.addResults(pcjTableName, visBs);

//		Scanner scanner = accCon.createScanner(pcjTableName, new Authorizations("U","USA"));
//		for(Entry<Key, Value> entry : scanner) {
//			System.out.println(entry.getKey());
//		}


	}

	@AfterClass
	public static void close() throws RepositoryException, PCJStorageException {
		storage.close();

		if (accumulo != null) {
			try {
				log.info("Shutting down the Mini Accumulo being used as a Rya store.");
				accumulo.stop();
				log.info("Mini Accumulo being used as a Rya store shut down.");
			} catch (final Exception e) {
				log.error("Could not shut down the Mini Accumulo.", e);
			}
		}
	}

	@Test
	public void variableInstantiationTest() throws Exception {

		final AccumuloIndexSet ais = new AccumuloIndexSet(conf, pcjTableName);
		final QueryBindingSet bs = new QueryBindingSet();
		bs.addBinding("name", new URIImpl("http://Alice"));

		final QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("name", new URIImpl("http://Bob"));

		final Set<BindingSet> bSets = Sets.<BindingSet> newHashSet(bs, bs2);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);

		Set<BindingSet> expected = new HashSet<>();
		expected.add(pcjBs1);
		expected.add(pcjBs2);
		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			final BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expected, fetchedResults);
	}

	@Test
	public void accumuloIndexSetTestAttemptJoinAccrossTypes() throws Exception {
		// Load some Triples into Rya.
		AccumuloIndexSet ais = new AccumuloIndexSet(conf, pcjTableName);

		final QueryBindingSet bs1 = new QueryBindingSet();
		bs1.addBinding("age", new NumericLiteralImpl(16, XMLSchema.INTEGER));
		final QueryBindingSet bs2 = new QueryBindingSet();
		bs2.addBinding("age", new NumericLiteralImpl(14, XMLSchema.INTEGER));

		final Set<BindingSet> bSets = Sets.<BindingSet> newHashSet(bs1, bs2);

		final CloseableIteration<BindingSet, QueryEvaluationException> results = ais
				.evaluate(bSets);
		Set<BindingSet> expected = new HashSet<>();
		expected.add(pcjBs1);
		expected.add(pcjBs2);
		final Set<BindingSet> fetchedResults = new HashSet<>();
		while (results.hasNext()) {
			final BindingSet next = results.next();
			fetchedResults.add(next);
		}

		Assert.assertEquals(expected, fetchedResults);
	}

	private static MiniAccumuloCluster startMiniAccumulo() throws IOException,
			InterruptedException, AccumuloException, AccumuloSecurityException {
		final File miniDataDir = Files.createTempDir();

		// Setup and start the Mini Accumulo.
		final MiniAccumuloCluster accumulo = new MiniAccumuloCluster(
				miniDataDir, "password");
		accumulo.start();

		// Store a connector to the Mini Accumulo.
		final Instance instance = new ZooKeeperInstance(
				accumulo.getInstanceName(), accumulo.getZooKeepers());
		accCon = instance.getConnector("root", new PasswordToken("password"));

		return accumulo;
	}

	private static Configuration getConf() {
		final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
		conf.setTablePrefix("rya_");
		conf.set(ConfigUtils.CLOUDBASE_USER, "root");
		conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "password");
		conf.set(ConfigUtils.CLOUDBASE_INSTANCE, instance);
		conf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, zooKeepers);
		conf.set(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, "U,USA");
		return conf;
	}

}
