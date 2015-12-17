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


import info.aduna.iteration.CloseableIteration;

import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.query.BatchRyaQuery;
import mvm.rya.api.persist.query.RyaQuery;
import mvm.rya.api.persist.query.RyaQueryEngine;
import mvm.rya.mongodb.dao.MongoDBStorageStrategy;
import mvm.rya.mongodb.dao.SimpleMongoDBStorageStrategy;
import mvm.rya.mongodb.iter.NonCloseableRyaStatementCursorIterator;
import mvm.rya.mongodb.iter.RyaStatementBindingSetCursorIterator;
import mvm.rya.mongodb.iter.RyaStatementCursorIterable;
import mvm.rya.mongodb.iter.RyaStatementCursorIterator;

import org.calrissian.mango.collect.CloseableIterable;
import org.openrdf.query.BindingSet;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * Date: 7/17/12
 * Time: 9:28 AM
 */
public class MongoDBQueryEngine implements RyaQueryEngine<MongoDBRdfConfiguration>, Closeable {

    private MongoDBRdfConfiguration configuration;
	private MongoClient mongoClient;
	private DBCollection coll;
	private MongoDBStorageStrategy strategy;
    
    public MongoDBQueryEngine(MongoDBRdfConfiguration conf) throws NumberFormatException, UnknownHostException{
		mongoClient = new MongoClient(conf.get(MongoDBRdfConfiguration.MONGO_INSTANCE),
				Integer.valueOf(conf.get(MongoDBRdfConfiguration.MONGO_INSTANCE_PORT)));
		DB db = mongoClient.getDB( conf.get(MongoDBRdfConfiguration.MONGO_DB_NAME));
		coll = db.getCollection(conf.getTriplesCollectionName());
		this.strategy = new SimpleMongoDBStorageStrategy();
    }
    
    
	@Override
	public void setConf(MongoDBRdfConfiguration conf) {
		configuration = conf;
	}
	
	@Override
	public MongoDBRdfConfiguration getConf() {
		return configuration;
	}
	
	@Override
	public CloseableIteration<RyaStatement, RyaDAOException> query(
			RyaStatement stmt, MongoDBRdfConfiguration conf)
			throws RyaDAOException {
        if (conf == null) {
            conf = configuration;
        }
        Long maxResults = conf.getLimit();
        Set<DBObject> queries = new HashSet<DBObject>();
        DBObject query = strategy.getQuery(stmt);
        queries.add(query);
        RyaStatementCursorIterator iterator = new RyaStatementCursorIterator(coll, queries, strategy);
        
        if (maxResults != null) {
            iterator.setMaxResults(maxResults);
        }
        return iterator;
	}
	@Override
	public CloseableIteration<? extends Entry<RyaStatement, BindingSet>, RyaDAOException> queryWithBindingSet(
			Collection<Entry<RyaStatement, BindingSet>> stmts,
			MongoDBRdfConfiguration conf) throws RyaDAOException {
        if (conf == null) {
            conf = configuration;
        }
        Long maxResults = conf.getLimit();
        Map<DBObject, BindingSet> rangeMap = new HashMap<DBObject, BindingSet>();
 
        //TODO: cannot span multiple tables here
        try {
            for (Map.Entry<RyaStatement, BindingSet> stmtbs : stmts) {
                RyaStatement stmt = stmtbs.getKey();
                BindingSet bs = stmtbs.getValue();
                DBObject query = strategy.getQuery(stmt);
                rangeMap.put(query, bs);
            }
            
            // TODO not sure what to do about regex ranges?
            RyaStatementBindingSetCursorIterator iterator = new RyaStatementBindingSetCursorIterator(coll, rangeMap, strategy);
            
            if (maxResults != null) {
                iterator.setMaxResults(maxResults);
            }
            return iterator;
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }

	}
	@Override
	public CloseableIteration<RyaStatement, RyaDAOException> batchQuery(
			Collection<RyaStatement> stmts, MongoDBRdfConfiguration conf)
			throws RyaDAOException {
        if (conf == null) {
            conf = configuration;
        }
        Long maxResults = conf.getLimit();
        Set<DBObject> queries = new HashSet<DBObject>();
 
        try {
            for (RyaStatement stmt : stmts) {
                queries.add( strategy.getQuery(stmt));
             }
            
            // TODO not sure what to do about regex ranges?
            RyaStatementCursorIterator iterator = new RyaStatementCursorIterator(coll, queries, strategy);
            
            if (maxResults != null) {
                iterator.setMaxResults(maxResults);
            }
            return iterator;
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }

	}
	@Override
	public CloseableIterable<RyaStatement> query(RyaQuery ryaQuery)
			throws RyaDAOException {
	        Set<DBObject> queries = new HashSet<DBObject>();
	 
	        try {
                queries.add( strategy.getQuery(ryaQuery));
	            
	            // TODO not sure what to do about regex ranges?
                // TODO this is gross
                RyaStatementCursorIterable iterator = new RyaStatementCursorIterable(new NonCloseableRyaStatementCursorIterator(new RyaStatementCursorIterator(coll, queries, strategy)));
	            
	            return iterator;
	        } catch (Exception e) {
	            throw new RyaDAOException(e);
	        }
	}
	@Override
	public CloseableIterable<RyaStatement> query(BatchRyaQuery batchRyaQuery)
			throws RyaDAOException {
         try {
             Set<DBObject> queries = new HashSet<DBObject>();
            for (RyaStatement statement : batchRyaQuery.getQueries()){
                queries.add( strategy.getQuery(statement));
                            	
            }
            
            // TODO not sure what to do about regex ranges?
            // TODO this is gross
            RyaStatementCursorIterable iterator = new RyaStatementCursorIterable(new NonCloseableRyaStatementCursorIterator(new RyaStatementCursorIterator(coll, queries, strategy)));
            
            return iterator;
        } catch (Exception e) {
            throw new RyaDAOException(e);
        }
 	}

    @Override
    public void close() throws IOException {
        if (mongoClient != null){ mongoClient.close(); }
    }
	
	
	


}
