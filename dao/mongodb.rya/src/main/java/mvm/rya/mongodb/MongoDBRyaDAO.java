package mvm.rya.mongodb;

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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.persist.RyaDAO;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.RyaNamespaceManager;
import mvm.rya.api.persist.index.RyaSecondaryIndexer;
import mvm.rya.api.persist.query.RyaQueryEngine;
import mvm.rya.mongodb.dao.MongoDBNamespaceManager;
import mvm.rya.mongodb.dao.MongoDBStorageStrategy;
import mvm.rya.mongodb.dao.SimpleMongoDBNamespaceManager;
import mvm.rya.mongodb.dao.SimpleMongoDBStorageStrategy;

import org.apache.commons.io.IOUtils;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.InsertOptions;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;

public class MongoDBRyaDAO implements RyaDAO<MongoDBRdfConfiguration>{

	
	private MongoDBRdfConfiguration conf;
	private MongoClient mongoClient;
	private DB db;
	private DBCollection coll;
	private MongoDBQueryEngine queryEngine;
	private MongoDBStorageStrategy storageStrategy;
	private MongoDBNamespaceManager nameSpaceManager;
	private MongodForTestsFactory testsFactory;
	
	private List<RyaSecondaryIndexer> secondaryIndexers;
	
	public MongoDBRyaDAO(MongoDBRdfConfiguration conf) throws RyaDAOException{
		this.conf = conf;
		initConnection();
		conf.setMongoClient(mongoClient);
		init();
	}

	
	public MongoDBRyaDAO(MongoDBRdfConfiguration conf, MongoClient mongoClient) throws RyaDAOException{
		this.conf = conf;
		this.mongoClient = mongoClient;
		conf.setMongoClient(mongoClient);
		init();
	}

	public void setConf(MongoDBRdfConfiguration conf) {
		this.conf = conf;
	}
	
	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}
	
	
	public MongoClient getMongoClient(){
		return mongoClient;
	}

	public void setDB(DB db) {
		this.db = db;
	}

	
	public void setDBCollection(DBCollection coll) {
		this.coll = coll;
	}

    public MongoDBRdfConfiguration getConf() {
        return conf;
    }

    public void initConnection() throws RyaDAOException {
        try {
            boolean useMongoTest = conf.getUseTestMongo();
            if (useMongoTest) {
                testsFactory = MongodForTestsFactory.with(Version.Main.PRODUCTION);
                mongoClient = testsFactory.newMongo();
                int port = mongoClient.getServerAddressList().get(0).getPort();
                conf.set(MongoDBRdfConfiguration.MONGO_INSTANCE_PORT, Integer.toString(port));
            } else {
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
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    
    public void init() throws RyaDAOException {
        try {
            secondaryIndexers = conf.getAdditionalIndexers();
            for(RyaSecondaryIndexer index: secondaryIndexers) {
                index.setConf(conf);
            }
            
            db = mongoClient.getDB(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            coll = db.getCollection(conf.getTriplesCollectionName());
            nameSpaceManager = new SimpleMongoDBNamespaceManager(db.getCollection(conf.getNameSpacesCollectionName()));
            queryEngine = new MongoDBQueryEngine(conf);
            storageStrategy = new SimpleMongoDBStorageStrategy();
            storageStrategy.createIndices(coll);

        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public boolean isInitialized() throws RyaDAOException {
        return true;
    }

    public void destroy() throws RyaDAOException {
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (conf.getUseTestMongo()) {
            testsFactory.shutdown();
        }

        IOUtils.closeQuietly(queryEngine);
    }

	public void add(RyaStatement statement) throws RyaDAOException {		
		// add it to the collection
		try {
			coll.insert(storageStrategy.serialize(statement));
			for(RyaSecondaryIndexer index: secondaryIndexers) {
			    index.storeStatement(statement);
			}
		}
		catch (com.mongodb.MongoException.DuplicateKey exception){
			// ignore
		}
		catch (com.mongodb.DuplicateKeyException exception){
			// ignore
		}
		catch (Exception ex){
			// ignore single exceptions
			ex.printStackTrace();
		}
	}
	
	public void add(Iterator<RyaStatement> statement) throws RyaDAOException {
		List<DBObject> dbInserts = new ArrayList<DBObject>();
		while (statement.hasNext()){
			RyaStatement ryaStatement = statement.next();
			DBObject insert = storageStrategy.serialize(ryaStatement);
			dbInserts.add(insert);
			
            try {
                for (RyaSecondaryIndexer index : secondaryIndexers) {
                    index.storeStatement(ryaStatement);
                }
            } catch (IOException e) {
                throw new RyaDAOException(e);
            }
            
		}
		coll.insert(dbInserts, new InsertOptions().continueOnError(true));
	}

	public void delete(RyaStatement statement, MongoDBRdfConfiguration conf)
			throws RyaDAOException {
		DBObject obj = storageStrategy.getQuery(statement);
		coll.remove(obj);
	}

	public void dropGraph(MongoDBRdfConfiguration conf, RyaURI... graphs)
			throws RyaDAOException {
		
	}

	public void delete(Iterator<RyaStatement> statements,
			MongoDBRdfConfiguration conf) throws RyaDAOException {
		while (statements.hasNext()){
			RyaStatement ryaStatement = statements.next();
			coll.remove(storageStrategy.getQuery(ryaStatement));
		}
		
	}

	public String getVersion() throws RyaDAOException {
		return "1.0";
	}

	public RyaQueryEngine<MongoDBRdfConfiguration> getQueryEngine() {
		return queryEngine;
	}

	public RyaNamespaceManager<MongoDBRdfConfiguration> getNamespaceManager() {
		return nameSpaceManager;
	}

	public void purge(RdfCloudTripleStoreConfiguration configuration) {
		// TODO Auto-generated method stub
		
	}

	public void dropAndDestroy() throws RyaDAOException {
		db.dropDatabase(); // this is dangerous!
	}


}
