package mvm.rya.mongodb.iter;

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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.mongodb.dao.MongoDBStorageStrategy;

import org.openrdf.query.BindingSet;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class RyaStatementBindingSetCursorIterator implements CloseableIteration<Entry<RyaStatement, BindingSet>, RyaDAOException> {

	private DBCollection coll;
	private Map<DBObject, BindingSet> rangeMap;
	private Iterator<DBObject> queryIterator;
	private Long maxResults;
	private DBCursor currentCursor;
	private BindingSet currentBindingSet;
	private MongoDBStorageStrategy strategy;

	public RyaStatementBindingSetCursorIterator(DBCollection coll,
			Map<DBObject, BindingSet> rangeMap, MongoDBStorageStrategy strategy) {
		this.coll = coll;
		this.rangeMap = rangeMap;
		this.queryIterator = rangeMap.keySet().iterator();
		this.strategy = strategy;
	}

	@Override
	public boolean hasNext() {
		if (!currentCursorIsValid()) {
			findNextValidCursor();
		}
		return currentCursorIsValid();
	}

	@Override
	public Entry<RyaStatement, BindingSet> next() {
		if (!currentCursorIsValid()) {
			findNextValidCursor();
		}
		if (currentCursorIsValid()) {
			// convert to Rya Statement
			DBObject queryResult = currentCursor.next();
			RyaStatement statement = strategy.deserializeDBObject(queryResult);
			return new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>(statement, currentBindingSet);
		}
		return null;
	}
	
	private void findNextValidCursor() {
		while (queryIterator.hasNext()){
			DBObject currentQuery = queryIterator.next();
			currentCursor = coll.find(currentQuery);
			currentBindingSet = rangeMap.get(currentQuery);
			if (currentCursor.hasNext()) break;
		}
	}
	
	private boolean currentCursorIsValid() {
		return (currentCursor != null) && currentCursor.hasNext();
	}


	public void setMaxResults(Long maxResults) {
		this.maxResults = maxResults;
	}

	@Override
	public void close() throws RyaDAOException {
		// TODO don't know what to do here
	}

	@Override
	public void remove() throws RyaDAOException {
		next();
	}

}
