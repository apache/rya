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

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.Sail;
import org.openrdf.sail.federation.Federation;
import org.openrdf.sail.memory.MemoryStore;

import com.google.common.base.Optional;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.indexing.accumulo.geo.GeoConstants;
import mvm.rya.indexing.external.tupleSet.PcjTables;
import mvm.rya.indexing.external.tupleSet.PcjTables.PcjException;
import mvm.rya.indexing.external.tupleSet.PcjTables.PcjVarOrderFactory;
import mvm.rya.rdftriplestore.inference.InferenceEngineException;
import mvm.rya.sail.config.RyaSailFactory;

public class FederatedRyaMockExample {
	private static final Logger log = Logger.getLogger(RyaDirectExample.class);

	//
	// Connection configuration parameters
	//

	private static final boolean USE_MOCK_INSTANCE = true;
	private static final boolean PRINT_QUERIES = true;
	private static final String INSTANCE = "instance";
	private static final String RYA_TABLE_PREFIX = "x_test_triplestore_";
	private static final String AUTHS = "";

	public static void main(String[] args) throws Exception {
		final Configuration conf = getConf();
		conf.setBoolean(ConfigUtils.DISPLAY_QUERY_PLAN, PRINT_QUERIES);
	
		SailRepository repo = null;	
		Repository test_repo = null;
		SailRepositoryConnection con=null;	
		RepositoryConnection test_con = null;

		try {
			log.info("Connecting to Federation Sail Repository.");

			Federation federation = new Federation();
			repo = new SailRepository(federation);
			repo.initialize();
	//
	// Insert 5 test memebers into federation
	//		
			for(int numBook=0;numBook<5;numBook++){
	        federation.addMember( createMember(numBook,conf));
			}
    //
    // Insert 6th member seperately, all those members are having same table prefix
    //			
			test_repo= createMember(6, conf);
			federation.addMember(test_repo);
			con = repo.getConnection();
			test_con=test_repo.getConnection();
			
		 final long start = System.currentTimeMillis();	
	
	//  Execute queries for con & test_con
			String query = "select ?p ?o { GRAPH <http://updated/test> {<http://acme.com/people/Jack> ?p ?o . }}";
			final CountingResultHandler resultHandler_1 = new CountingResultHandler();	
			final CountingResultHandler resultHandler_2 = new CountingResultHandler();	
						
			TupleQuery tupleQuery_1 = con.prepareTupleQuery(QueryLanguage.SPARQL,
					query);
			tupleQuery_1.evaluate(resultHandler_1);
			log.info("Result count : " + resultHandler_1.getCount());

			TupleQuery tupleQuery_2 = con.prepareTupleQuery(QueryLanguage.SPARQL,
					query);
			tupleQuery_2.evaluate(resultHandler_2);
			log.info("Result count : " + resultHandler_2.getCount());

    // Get execution time
			log.info("TIME: " + (System.currentTimeMillis() - start) / 1000.);
		} finally {
			log.info("Shutting down");
			closeQuietly(con);
			closeQuietly(repo);
	        test_con.close();
		    test_repo.shutDown();
		
		}
	}

	//
	// Insert one data into each memeber
	//
	private static Repository createMember(int ID,Configuration conf)
			throws RepositoryException, RDFParseException, IOException, AccumuloException, AccumuloSecurityException, RyaDAOException, InferenceEngineException, MalformedQueryException, UpdateExecutionException {
	    Sail extSail = RyaSailFactory.getInstance(conf);
		SailRepository repository = new SailRepository(extSail);

		repository.initialize();
		SailRepositoryConnection con = repository.getConnection();
		try {
			String query = "INSERT DATA\n"//
					+ "{ GRAPH <http://updated/test> {\n"//
					+ "  <http://acme.com/people/Jack> " //
					+ "       <http://acme.com/actions/likes> \"Book"+ID+"\" .\n"
					+ "} }"; 
			
			Update update = con.prepareUpdate(QueryLanguage.SPARQL, query);
			update.execute();	
		} finally {
		closeQuietly(con);
		}
		return repository;
	}

	private static void closeQuietly(SailRepository repository) {
		if (repository != null) {
			try {
				repository.shutDown();
			} catch (final RepositoryException e) {
				// quietly absorb this exception
			}
		}
	}

	private static void closeQuietly(SailRepositoryConnection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (final RepositoryException e) {
				// quietly absorb this exception
			}
		}
	}

	private static Configuration getConf() {

		final AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();

		conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, USE_MOCK_INSTANCE);
		conf.set(ConfigUtils.USE_PCJ, "true");
		conf.set(ConfigUtils.USE_GEO, "true");
		conf.set(ConfigUtils.USE_FREETEXT, "true");
		conf.set(ConfigUtils.USE_TEMPORAL, "true");
		conf.set(RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX,
				RYA_TABLE_PREFIX);
		conf.set(ConfigUtils.CLOUDBASE_USER, "root");
		conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "");
		conf.set(ConfigUtils.CLOUDBASE_INSTANCE, INSTANCE);
		conf.setInt(ConfigUtils.NUM_PARTITIONS, 3);
		conf.set(ConfigUtils.CLOUDBASE_AUTHS, AUTHS);

		// only geo index statements with geo:asWKT predicates
		conf.set(ConfigUtils.GEO_PREDICATES_LIST,
				GeoConstants.GEO_AS_WKT.stringValue());
		return conf;
	}

	public static void testAddAndDelete(SailRepositoryConnection conn)
			throws MalformedQueryException, RepositoryException,
			UpdateExecutionException, QueryEvaluationException,
			TupleQueryResultHandlerException, AccumuloException,
			AccumuloSecurityException, TableNotFoundException {

		// Add data
		String query = "INSERT DATA\n"//
				+ "{ GRAPH <http://updated/test> {\n"//
				+ "  <http://acme.com/people/Jack> " //
				+ "       <http://acme.com/actions/likes> \"A new book\" ;\n"//
				+ "       <http://acme.com/actions/likes> \"Avocados\" .\n"
				+ "} }";

		log.info("Performing Query");

		Update update = conn.prepareUpdate(QueryLanguage.SPARQL, query);
		update.execute();

		query = "select ?p ?o { GRAPH <http://updated/test> {<http://acme.com/people/Jack> ?p ?o . }}";
		final CountingResultHandler resultHandler = new CountingResultHandler();
		TupleQuery tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL,
				query);
		tupleQuery.evaluate(resultHandler);
		log.info("Result count : " + resultHandler.getCount());

		Validate.isTrue(resultHandler.getCount() == 2);
		resultHandler.resetCount();

		// Delete Data
		query = "DELETE DATA\n" //
				+ "{ GRAPH <http://updated/test> {\n"
				+ "  <http://acme.com/people/Jack> <http://acme.com/actions/likes> \"A new book\" ;\n"
				+ "   <http://acme.com/actions/likes> \"Avocados\" .\n" + "}}";

		update = conn.prepareUpdate(QueryLanguage.SPARQL, query);
		update.execute();

		query = "select ?p ?o { GRAPH <http://updated/test> {<http://acme.com/people/Jack> ?p ?o . }}";
		tupleQuery = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
		tupleQuery.evaluate(resultHandler);
		log.info("Result count : " + resultHandler.getCount());

		Validate.isTrue(resultHandler.getCount() == 0);
	}


	private static class CountingResultHandler implements
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
			count++;
			System.out.println(arg0);
		}

		@Override
		public void endQueryResult() throws TupleQueryResultHandlerException {
		}

		@Override
		public void handleBoolean(boolean arg0)
				throws QueryResultHandlerException {
		}

		@Override
		public void handleLinks(List<String> arg0)
				throws QueryResultHandlerException {
		}
	}
}
