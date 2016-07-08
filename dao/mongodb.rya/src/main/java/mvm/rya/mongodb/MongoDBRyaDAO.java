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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.InsertOptions;
import com.mongodb.MongoClient;

import de.flapdoodle.embed.mongo.tests.MongodForTestsFactory;
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

/**
 * Default DAO for mongo backed RYA allowing for CRUD operations.
 */
public final class MongoDBRyaDAO implements RyaDAO<MongoDBRdfConfiguration>{
    private static final Logger log = Logger.getLogger(MongoDBRyaDAO.class);

    private MongoDBRdfConfiguration conf;
    private MongoClient mongoClient;
    private DB db;
    private DBCollection coll;
    private MongoDBQueryEngine queryEngine;
    private MongoDBStorageStrategy storageStrategy;
    private MongoDBNamespaceManager nameSpaceManager;
    private MongodForTestsFactory testsFactory;

    private List<RyaSecondaryIndexer> secondaryIndexers;

    /**
     * Creates a new {@link MongoDBRyaDAO}
     * @param conf
     * @throws RyaDAOException
     */
    public MongoDBRyaDAO(final MongoDBRdfConfiguration conf) throws RyaDAOException, NumberFormatException, UnknownHostException {
        this.conf = conf;
        try {
            mongoClient = MongoConnectorFactory.getMongoClient(conf);
            conf.setMongoClient(mongoClient);
            init();
        } catch (NumberFormatException | UnknownHostException e) {
            log.error("Unable to create a connection to mongo.", e);
            throw e;
        }
    }


    public MongoDBRyaDAO(final MongoDBRdfConfiguration conf, final MongoClient mongoClient) throws RyaDAOException{
        this.conf = conf;
        this.mongoClient = mongoClient;
        conf.setMongoClient(mongoClient);
        init();
    }

    @Override
    public void setConf(final MongoDBRdfConfiguration conf) {
        this.conf = conf;
    }

    public MongoClient getMongoClient(){
        return mongoClient;
    }

    public void setDB(final DB db) {
        this.db = db;
    }


    public void setDBCollection(final DBCollection coll) {
        this.coll = coll;
    }

    @Override
    public MongoDBRdfConfiguration getConf() {
        return conf;
    }

    @Override
    public void init() throws RyaDAOException {
            secondaryIndexers = conf.getAdditionalIndexers();
            for(final RyaSecondaryIndexer index: secondaryIndexers) {
                index.setConf(conf);
            }

            db = mongoClient.getDB(conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
            coll = db.getCollection(conf.getTriplesCollectionName());
            nameSpaceManager = new SimpleMongoDBNamespaceManager(db.getCollection(conf.getNameSpacesCollectionName()));
            queryEngine = new MongoDBQueryEngine(conf, mongoClient);
            storageStrategy = new SimpleMongoDBStorageStrategy();
            storageStrategy.createIndices(coll);
    }

    @Override
    public boolean isInitialized() throws RyaDAOException {
        return true;
    }

    @Override
    public void destroy() throws RyaDAOException {
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (conf.getUseTestMongo()) {
            testsFactory.shutdown();
        }

        IOUtils.closeQuietly(queryEngine);
    }

    @Override
    public void add(final RyaStatement statement) throws RyaDAOException {
        // add it to the collection
        try {
            coll.insert(storageStrategy.serialize(statement));
            for(final RyaSecondaryIndexer index: secondaryIndexers) {
                index.storeStatement(statement);
            }
        } catch (final IOException e) {
            log.error("Unable to add: " + statement.toString());
            throw new RyaDAOException(e);
        }
    }

    @Override
    public void add(final Iterator<RyaStatement> statement) throws RyaDAOException {
        final List<DBObject> dbInserts = new ArrayList<DBObject>();
        while (statement.hasNext()){
            final RyaStatement ryaStatement = statement.next();
            final DBObject insert = storageStrategy.serialize(ryaStatement);
            dbInserts.add(insert);

            try {
                for (final RyaSecondaryIndexer index : secondaryIndexers) {
                    index.storeStatement(ryaStatement);
                }
            } catch (final IOException e) {
                log.error("Failed to add: " + ryaStatement.toString() + " to the indexer");
            }

        }
        coll.insert(dbInserts, new InsertOptions().continueOnError(true));
    }

    @Override
    public void delete(final RyaStatement statement, final MongoDBRdfConfiguration conf)
            throws RyaDAOException {
        final DBObject obj = storageStrategy.getQuery(statement);
        coll.remove(obj);
    }

    @Override
    public void dropGraph(final MongoDBRdfConfiguration conf, final RyaURI... graphs)
            throws RyaDAOException {

    }

    @Override
    public void delete(final Iterator<RyaStatement> statements,
            final MongoDBRdfConfiguration conf) throws RyaDAOException {
        while (statements.hasNext()){
            final RyaStatement ryaStatement = statements.next();
            coll.remove(storageStrategy.getQuery(ryaStatement));
        }

    }

    @Override
    public String getVersion() throws RyaDAOException {
        return "1.0";
    }

    @Override
    public RyaQueryEngine<MongoDBRdfConfiguration> getQueryEngine() {
        return queryEngine;
    }

    @Override
    public RyaNamespaceManager<MongoDBRdfConfiguration> getNamespaceManager() {
        return nameSpaceManager;
    }

    @Override
    public void purge(final RdfCloudTripleStoreConfiguration configuration) {

    }

    @Override
    public void dropAndDestroy() throws RyaDAOException {
        db.dropDatabase(); // this is dangerous!
    }
}
