package mvm.rya.indexing.mongodb;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.api.resolver.RyaToRdfConversions;
import mvm.rya.indexing.FreeTextIndexer;
import mvm.rya.indexing.StatementContraints;
import mvm.rya.indexing.accumulo.ConfigUtils;
import mvm.rya.mongodb.MongoDBRdfConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;

public class MongoFreeTextIndexer extends AbstractMongoIndexer implements FreeTextIndexer {

	private static final Logger logger = Logger
			.getLogger(MongoFreeTextIndexer.class);

	private TextMongoDBStorageStrategy storageStrategy;
	private MongoClient mongoClient;
	private DB db;
	private DBCollection coll;
	private Set<URI> predicates;
	private Configuration conf;
	private boolean isInit = false;
	private String tableName = "";

	private void init() throws IOException{
        boolean useMongoTest = conf.getBoolean(MongoDBRdfConfiguration.USE_TEST_MONGO, false);
        if (useMongoTest) {
        	boolean initializedClient = false;
        	if (conf instanceof MongoDBRdfConfiguration){
        		MongoDBRdfConfiguration castedConf = (MongoDBRdfConfiguration) conf;
        		if (castedConf.getMongoClient() != null){
        			this.mongoClient = castedConf.getMongoClient();
        			initializedClient = true;
        		}
        	}
        	if (!initializedClient){
        		MongodForTestsFactory testsFactory = MongodForTestsFactory.with(Version.Main.PRODUCTION);
        		mongoClient = testsFactory.newMongo();
        		int port = mongoClient.getServerAddressList().get(0).getPort();
        		conf.set(MongoDBRdfConfiguration.MONGO_INSTANCE_PORT, Integer.toString(port));
        	}        } else {
            ServerAddress server = new ServerAddress(conf.get(MongoDBRdfConfiguration.MONGO_INSTANCE),
                    Integer.valueOf(conf.get(MongoDBRdfConfiguration.MONGO_INSTANCE_PORT)));
            if (conf.get(MongoDBRdfConfiguration.MONGO_USER) != null) {
                MongoCredential cred = MongoCredential.createCredential(
                        conf.get(MongoDBRdfConfiguration.MONGO_USER),
                        conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME),
                        conf.get(MongoDBRdfConfiguration.MONGO_USER_PASSWORD).toCharArray());
                mongoClient = new MongoClient(server, Arrays.asList(cred));
            } else {
                mongoClient = new MongoClient(server);
            }
        }
        predicates = ConfigUtils.getFreeTextPredicates(conf);
        tableName = conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME);
        db = mongoClient.getDB(tableName);
        coll = db.getCollection(conf.get(MongoDBRdfConfiguration.MONGO_COLLECTION_PREFIX, "rya"));
        storageStrategy = new TextMongoDBStorageStrategy();
        storageStrategy.createIndices(coll);
    }

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	// setConf initializes because index is created via reflection
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		if (!isInit) {
			try {
				init();
				isInit = true;
			} catch (IOException e) {
				logger.warn(
						"Unable to initialize index.  Throwing Runtime Exception. ",
						e);
				throw new RuntimeException(e);
			}
		}
	}

	private void storeStatement(Statement statement) throws IOException {
		// if this is a valid predicate and a valid geometry
		boolean isValidPredicate = predicates.isEmpty()
				|| predicates.contains(statement.getPredicate());

		if (isValidPredicate && (statement.getObject() instanceof Literal)) {

			// add it to the collection
			try {
				DBObject obj = storageStrategy.serialize(statement);
				if (obj != null) {
					DBObject query = storageStrategy
							.getQuery(RdfToRyaConversions
									.convertStatement(statement));
					coll.update(query, obj, true, false);
				}
			} catch (com.mongodb.MongoException.DuplicateKey exception) {
				// ignore
			} catch (com.mongodb.DuplicateKeyException exception) {
				// ignore
			} catch (Exception ex) {
				// ignore single exceptions
				ex.printStackTrace();
			}
		}
	}

	@Override
	public void storeStatement(RyaStatement statement) throws IOException {
		storeStatement(RyaToRdfConversions.convertStatement(statement));
	}


	private CloseableIteration<Statement, QueryEvaluationException> getIteratorWrapper(
			final DBObject query, final DBCollection coll,
			final TextMongoDBStorageStrategy storageStrategy) {

		return new CloseableIteration<Statement, QueryEvaluationException>() {

			private DBCursor cursor = null;

			private DBCursor getIterator() throws QueryEvaluationException {
				if (cursor == null) {
					cursor = coll.find(query);
				}
				return cursor;
			}

			@Override
			public boolean hasNext() throws QueryEvaluationException {
				return getIterator().hasNext();
			}

			@Override
			public Statement next() throws QueryEvaluationException {
				DBObject feature = getIterator().next();
				RyaStatement statement = storageStrategy
						.deserializeDBObject(feature);
				return RyaToRdfConversions.convertStatement(statement);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException(
						"Remove not implemented");
			}

			@Override
			public void close() throws QueryEvaluationException {
				getIterator().close();
			}
		};
	}

	@Override
	public Set<URI> getIndexablePredicates() {
		return predicates;
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		mongoClient.close();
	}

	@Override
	public void deleteStatement(RyaStatement stmt) throws IOException {
	   DBObject obj = storageStrategy.getQuery(stmt);
	   coll.remove(obj);
	}

	@Override
	public CloseableIteration<Statement, QueryEvaluationException> queryText(
			String query, StatementContraints contraints) throws IOException {
		DBObject obj = storageStrategy.getQuery(contraints, query);
		long count = coll.count(obj);
		return getIteratorWrapper(obj, coll, storageStrategy);
	}

}
