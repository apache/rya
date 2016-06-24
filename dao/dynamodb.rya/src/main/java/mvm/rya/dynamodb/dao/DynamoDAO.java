package mvm.rya.dynamodb.dao;
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

import java.util.Iterator;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAO;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.RyaNamespaceManager;
import mvm.rya.api.persist.query.RyaQueryEngine;

public class DynamoDAO implements RyaDAO<DynamoRdfConfiguration> {
	
	private DynamoStorageStrategy strategy;
	private DynamoRdfConfiguration conf;
	private boolean isInitialized = false;
	private DynamoQueryEngine engine;

	@Override
	public void setConf(DynamoRdfConfiguration conf) {
		this.conf = conf;
	}

	@Override
	public DynamoRdfConfiguration getConf() {
		return conf;
	}

	@Override
	public void init() throws RyaDAOException {
		this.strategy = new DynamoStorageStrategy(conf);
		this.engine = new DynamoQueryEngine(conf, strategy);
		this.isInitialized = true;
	}

	@Override
	public boolean isInitialized() throws RyaDAOException {
		return isInitialized;
	}

	@Override
	public void destroy() throws RyaDAOException {
		strategy.close();
	}

	@Override
	public void add(RyaStatement statement) throws RyaDAOException {
		strategy.add(statement);
	}

	@Override
	public void add(Iterator<RyaStatement> statement) throws RyaDAOException {
		strategy.add(statement);
	}

	@Override
	public void delete(RyaStatement statement, DynamoRdfConfiguration conf) throws RyaDAOException {
		strategy.delete(statement);
	}

	@Override
	public void dropGraph(DynamoRdfConfiguration conf, RyaURI... graphs) throws RyaDAOException {
		throw new RyaDAOException("Method not supported!");
	}

	@Override
	public void delete(Iterator<RyaStatement> statements, DynamoRdfConfiguration conf) throws RyaDAOException {
		strategy.delete(statements);
	}

	@Override
	public String getVersion() throws RyaDAOException {
		return "1.0";
	}

	@Override
	public RyaQueryEngine<DynamoRdfConfiguration> getQueryEngine() {
		return this.engine;
	}

	@Override
	public RyaNamespaceManager<DynamoRdfConfiguration> getNamespaceManager() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void purge(RdfCloudTripleStoreConfiguration configuration) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void dropAndDestroy() throws RyaDAOException {
		// TODO Auto-generated method stub
		
	}

	public static void main(String[] args) throws Exception {
		  DynamoRdfConfiguration conf = new DynamoRdfConfiguration();
		  conf.setAWSUserName("FakeID");
		  conf.setAWSSecretKey("FakeKey");
		  conf.setAWSEndPoint("http://localhost:8000");
		  DynamoDAO dao = new DynamoDAO();
		  dao.setConf(conf);
		  dao.init();
		  RyaStatement statement = new RyaStatement(new RyaURI("urn:subj"), new RyaURI("urn:pred"), new RyaURI("urn:obj"));
		  RyaStatement statement2 = new RyaStatement(new RyaURI("urn:subj"), new RyaURI("urn:pred2"), new RyaURI("urn:obj"));
		  RyaStatement statement3 = new RyaStatement(new RyaURI("urn:subj"), null, new RyaURI("urn:obj"));
		  dao.add(statement);
		  dao.add(statement2);
		  CloseableIteration<RyaStatement, RyaDAOException> queryResults = dao.getQueryEngine().query(statement3, conf);
		  while(queryResults.hasNext()){
			  System.out.println(queryResults.next());
		  }
	    }

}
