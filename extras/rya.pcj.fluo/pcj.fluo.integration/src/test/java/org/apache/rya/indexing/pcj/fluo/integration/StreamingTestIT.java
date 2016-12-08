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
package org.apache.rya.indexing.pcj.fluo.integration;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.pcj.fluo.ITBase;
import org.apache.rya.indexing.pcj.fluo.api.CreatePcj;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjStorage;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.sail.config.RyaSailFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;


public class StreamingTestIT extends ITBase {

	private static final Logger log = Logger.getLogger(ITBase.class);
	private static String query = "select ?name ?uuid where {   ?uuid <http://pred1> ?name ; <http://pred2> \"literal\".}";
	private static String uuidPrefix = "http://uuid_";
	private static String name = "number_";
	private static String pred1 = "http://pred1";
	private static String pred2 = "http://pred2";
	
	private PcjTables pcjTables = new PcjTables();
	private String pcjTableName;
	
	private Sail sail;
	private SailRepository repo;
	private SailRepositoryConnection conn;
	
	
	@Before
	public void init() throws Exception {
		AccumuloRdfConfiguration conf = makeConfig(instanceName, zookeepers);
		conf.set(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, "U");
		conf.set(RdfCloudTripleStoreConfiguration.CONF_CV, "U");
		accumuloConn.securityOperations().changeUserAuthorizations("root", new Authorizations("U"));
		sail =  RyaSailFactory.getInstance(conf);
		repo = new SailRepository(sail);
		conn = repo.getConnection();
	}
	
	@After
	public void close() throws RepositoryException, SailException {
		conn.close();
		repo.shutDown();
		sail.shutDown();
	}
	
	
	@Test
	public void testRandomStreamingIngest() throws Exception {
		
		pcjTableName = createPcj(query);
		log.info("Adding Join Pairs...");
		addRandomQueryStatementPairs(100);
		Assert.assertEquals(100, countPcjs());
		
	}
	
	private String createPcj(String pcj) throws Exception {
		accumuloConn.securityOperations().changeUserAuthorizations("root", new Authorizations("U"));
	    // Create the PCJ table.
        final PrecomputedJoinStorage pcjStorage = new AccumuloPcjStorage(accumuloConn, RYA_INSTANCE_NAME);
        final String pcjId = pcjStorage.createPcj(pcj);
        new CreatePcj().withRyaIntegration(pcjId, pcjStorage, fluoClient, accumuloConn, RYA_INSTANCE_NAME);
		String tableName = RYA_INSTANCE_NAME + "INDEX_" + pcjId;
		
		return tableName;
	}
	
	private void addRandomQueryStatementPairs(int numPairs) throws Exception {

		Set<Statement> statementPairs = new HashSet<>();
		for (int i = 0; i < numPairs; i++) {
			String uri = uuidPrefix + UUID.randomUUID().toString();
			Statement statement1 = new StatementImpl(new URIImpl(uri), new URIImpl(pred1),
					new LiteralImpl(name + (i + 1)));
			Statement statement2 = new StatementImpl(new URIImpl(uri), new URIImpl(pred2), new LiteralImpl("literal"));
			statementPairs.add(statement1);
			statementPairs.add(statement2);
		}
		conn.add(statementPairs, new Resource[0]);
		fluo.waitForObservers();
	}
	
	private int countPcjs() throws Exception {
		Iterable<BindingSet> bindingsets = pcjTables.listResults(accumuloConn, pcjTableName, new Authorizations("U"));
		int count = 0;
		for (BindingSet bs : bindingsets) {
//			System.out.println(bs);
			count++;
		}
//		IncUpdateDAO.printAll(fluoClient);
		return count;
	}
	
	
}
