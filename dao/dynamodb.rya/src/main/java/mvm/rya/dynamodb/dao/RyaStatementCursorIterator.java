package mvm.rya.dynamodb.dao;

import java.util.Set;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;

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
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;

public class RyaStatementCursorIterator implements CloseableIteration<RyaStatement, RyaDAOException> {

	private Long maxResults;
	private ItemCollection<QueryOutcome> coll;
	private IteratorSupport<Item, QueryOutcome> iterator;
	private DynamoStorageStrategy strategy;

	public RyaStatementCursorIterator(ItemCollection<QueryOutcome> coll, DynamoStorageStrategy strategy) {
		this.coll = coll;
		this.iterator = coll.iterator();
		this.strategy = strategy;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public RyaStatement next() {
		Item item = iterator.next();

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
