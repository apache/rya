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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.mongodb.dao.MongoDBStorageStrategy;

import org.openrdf.query.BindingSet;

import com.google.common.collect.Multimap;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class RyaStatementBindingSetCursorIterator implements CloseableIteration<Entry<RyaStatement, BindingSet>, RyaDAOException> {

	private DBCollection coll;
	private Multimap<DBObject, BindingSet> rangeMap;
	private Iterator<DBObject> queryIterator;
	private Long maxResults;
	private DBCursor resultCursor;
	private RyaStatement currentStatement;
	private Collection<BindingSet> currentBindingSetCollection;
	private Iterator<BindingSet> currentBindingSetIterator;
	private MongoDBStorageStrategy strategy;

	public RyaStatementBindingSetCursorIterator(DBCollection coll,
			Multimap<DBObject, BindingSet> rangeMap, MongoDBStorageStrategy strategy) {
		this.coll = coll;
		this.rangeMap = rangeMap;
		this.queryIterator = rangeMap.keySet().iterator();
		this.strategy = strategy;
	}

	@Override
	public boolean hasNext() {
		if (!currentBindingSetIteratorIsValid()) {
			findNextResult();
		}
		return currentBindingSetIteratorIsValid();
	}

	@Override
	public Entry<RyaStatement, BindingSet> next() {
		if (!currentBindingSetIteratorIsValid()) {
			findNextResult();
		}
		if (currentBindingSetIteratorIsValid()) {
			BindingSet currentBindingSet = currentBindingSetIterator.next();
			return new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>(currentStatement, currentBindingSet);
		}
		return null;
	}
	
	private boolean currentBindingSetIteratorIsValid() {
		return (currentBindingSetIterator != null) && currentBindingSetIterator.hasNext();
	}

	private void findNextResult() {
		if (!currentResultCursorIsValid()) {
			findNextValidResultCursor();
		}
		if (currentResultCursorIsValid()) {
			// convert to Rya Statement
			DBObject queryResult = resultCursor.next();
			currentStatement = strategy.deserializeDBObject(queryResult);
			currentBindingSetIterator = currentBindingSetCollection.iterator();
		}
	}

	private void findNextValidResultCursor() {
		while (queryIterator.hasNext()){
			DBObject currentQuery = queryIterator.next();
			resultCursor = coll.find(currentQuery);
			currentBindingSetCollection = rangeMap.get(currentQuery);
			if (resultCursor.hasNext()) return;
		}
	}
	
	private boolean currentResultCursorIsValid() {
		return (resultCursor != null) && resultCursor.hasNext();
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
