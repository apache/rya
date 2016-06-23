package mvm.rya.dynamodb.iter;
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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import org.openrdf.query.BindingSet;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.google.common.collect.Multimap;

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
import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.dynamodb.dao.DynamoStorageStrategy;

public class RyaStatementBSItemIterator implements CloseableIteration<Entry<RyaStatement, BindingSet>, RyaDAOException> {

	private Multimap<ItemCollection<QueryOutcome>, BindingSet> rangeMap;
	private Iterator<ItemCollection<QueryOutcome>> queryIterator;
	private Long maxResults;
	private Iterator<Item> resultCursor;
	private RyaStatement currentStatement;
	private Collection<BindingSet> currentBindingSetCollection;
	private Iterator<BindingSet> currentBindingSetIterator;
	private DynamoStorageStrategy strategy;

	public RyaStatementBSItemIterator(Multimap<ItemCollection<QueryOutcome>, BindingSet> rangeMap,
			DynamoStorageStrategy strategy) {
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
			Item queryResult = resultCursor.next();
			currentStatement = strategy.convertToStatement(queryResult);
			currentBindingSetIterator = currentBindingSetCollection.iterator();
		}
	}

	private void findNextValidResultCursor() {
		while (queryIterator.hasNext()){
			ItemCollection<QueryOutcome> currentQuery = queryIterator.next();
			resultCursor = currentQuery.iterator();
			currentBindingSetCollection = rangeMap.get(currentQuery);
			if (resultCursor.hasNext()) return;
		}
	}
	
	private boolean currentResultCursorIsValid() {
		return (resultCursor != null) && resultCursor.hasNext();
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
