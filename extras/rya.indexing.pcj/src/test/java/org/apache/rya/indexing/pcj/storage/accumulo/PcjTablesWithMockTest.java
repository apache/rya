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


package org.apache.rya.indexing.pcj.storage.accumulo;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.NumericLiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepositoryConnection;

import com.google.common.base.Optional;

public class PcjTablesWithMockTest {

	private static final Logger log = Logger
			.getLogger(PcjTablesWithMockTest.class);

	private static final String USE_MOCK_INSTANCE = ".useMockInstance";
	private static final String CLOUDBASE_INSTANCE = "sc.cloudbase.instancename";
	private static final String CLOUDBASE_USER = "sc.cloudbase.username";
	private static final String CLOUDBASE_PASSWORD = "sc.cloudbase.password";
	private static final String RYA_TABLE_PREFIX = "demo_";
	private Connector accumuloConn;
	private RyaSailRepository ryaRepo;
	private SailRepositoryConnection ryaConn;

	@Before
	public void init() throws AccumuloException, AccumuloSecurityException, RepositoryException {
		Instance instance = new MockInstance("instance");
		accumuloConn = instance.getConnector("root", new PasswordToken(""));
		ryaRepo = setupRya(accumuloConn);
		ryaConn = ryaRepo.getConnection();
	}


	 @Test
	    public void populatePcj() throws RepositoryException, PcjException, TableNotFoundException, BindingSetConversionException {
	        // Load some Triples into Rya.
	        final Set<Statement> triples = new HashSet<>();
	        triples.add( new StatementImpl(new URIImpl("http://Alice"), new URIImpl("http://hasAge"), new NumericLiteralImpl(14, XMLSchema.INTEGER)) );
	        triples.add( new StatementImpl(new URIImpl("http://Alice"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
	        triples.add( new StatementImpl(new URIImpl("http://Bob"), new URIImpl("http://hasAge"), new NumericLiteralImpl(16, XMLSchema.INTEGER)) );
	        triples.add( new StatementImpl(new URIImpl("http://Bob"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
	        triples.add( new StatementImpl(new URIImpl("http://Charlie"), new URIImpl("http://hasAge"), new NumericLiteralImpl(12, XMLSchema.INTEGER)) );
	        triples.add( new StatementImpl(new URIImpl("http://Charlie"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );
	        triples.add( new StatementImpl(new URIImpl("http://Eve"), new URIImpl("http://hasAge"), new NumericLiteralImpl(43, XMLSchema.INTEGER)) );
	        triples.add( new StatementImpl(new URIImpl("http://Eve"), new URIImpl("http://playsSport"), new LiteralImpl("Soccer")) );

	        for(final Statement triple : triples) {
	            ryaConn.add(triple);
	        }

	        // Create a PCJ table that will include those triples in its results.
	        final String sparql =
	                "SELECT ?name ?age " +
	                "{" +
	                  "?name <http://hasAge> ?age." +
	                  "?name <http://playsSport> \"Soccer\" " +
	                "}";

	        final String pcjTableName = new PcjTableNameFactory().makeTableName(RYA_TABLE_PREFIX, "testPcj");
	        final PcjTables pcjs = new PcjTables();
	        pcjs.createAndPopulatePcj(ryaConn, accumuloConn, pcjTableName, sparql, new String[]{"age","name"}, Optional.<PcjVarOrderFactory>absent());

	        // Make sure the cardinality was updated.
	        final PcjMetadata metadata = pcjs.getPcjMetadata(accumuloConn, pcjTableName);
	        assertEquals(4, metadata.getCardinality());
	    }


	@After
	public void shutdown() {
		if (ryaConn != null) {
			try {
				log.info("Shutting down Rya Connection.");
				ryaConn.close();
				log.info("Rya Connection shut down.");
			} catch (final Exception e) {
				log.error("Could not shut down the Rya Connection.", e);
			}
		}

		if (ryaRepo != null) {
			try {
				log.info("Shutting down Rya Repo.");
				ryaRepo.shutDown();
				log.info("Rya Repo shut down.");
			} catch (final Exception e) {
				log.error("Could not shut down the Rya Repo.", e);
			}
		}
	}

	private static RyaSailRepository setupRya(Connector accumuloConn)
			throws AccumuloException, AccumuloSecurityException,
			RepositoryException {

		// Setup the Rya Repository that will be used to create Repository
		// Connections.
		final RdfCloudTripleStore ryaStore = new RdfCloudTripleStore();
		final AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
		crdfdao.setConnector(accumuloConn);

		// Setup Rya configuration values.
		final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
		conf.setTablePrefix("demo_");
		conf.setDisplayQueryPlan(false);

		conf.setBoolean(USE_MOCK_INSTANCE, true);
		conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX, RYA_TABLE_PREFIX);
		conf.set(CLOUDBASE_USER, "root");
		conf.set(CLOUDBASE_PASSWORD, "");
		conf.set(CLOUDBASE_INSTANCE, "instance");

		crdfdao.setConf(conf);
		ryaStore.setRyaDAO(crdfdao);

		final RyaSailRepository ryaRepo = new RyaSailRepository(ryaStore);
		ryaRepo.initialize();

		return ryaRepo;
	}

}
