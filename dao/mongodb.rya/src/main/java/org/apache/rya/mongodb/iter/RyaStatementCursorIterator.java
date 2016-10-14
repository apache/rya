package org.apache.rya.mongodb.iter;

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
import java.util.Map.Entry;
import java.util.Set;

import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.dao.MongoDBStorageStrategy;

import org.calrissian.mango.collect.CloseableIterable;
import org.openrdf.query.BindingSet;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class RyaStatementCursorIterator implements CloseableIteration<RyaStatement, RyaDAOException> {

	private DBCollection coll;
	private Iterator<DBObject> queryIterator;
	private DBCursor currentCursor;
	private MongoDBStorageStrategy strategy;
	private Long maxResults;

	public RyaStatementCursorIterator(DBCollection coll, Set<DBObject> queries, MongoDBStorageStrategy strategy) {
		this.coll = coll;
		this.queryIterator = queries.iterator();
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
	public RyaStatement next() {
		if (!currentCursorIsValid()) {
			findNextValidCursor();
		}
		if (currentCursorIsValid()) {
			// convert to Rya Statement
			DBObject queryResult = currentCursor.next();
			RyaStatement statement = strategy.deserializeDBObject(queryResult);
			return statement;
		}
		return null;
	}
	
	private void findNextValidCursor() {
		while (queryIterator.hasNext()){
			DBObject currentQuery = queryIterator.next();
			currentCursor = coll.find(currentQuery);
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
