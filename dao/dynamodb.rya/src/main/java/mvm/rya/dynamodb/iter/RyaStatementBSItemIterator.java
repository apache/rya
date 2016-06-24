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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.openrdf.query.BindingSet;

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
import mvm.rya.api.RdfCloudTripleStoreUtils;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.dynamodb.dao.DynamoStorageStrategy;

public class RyaStatementBSItemIterator implements CloseableIteration<Entry<RyaStatement, BindingSet>, RyaDAOException> {

	private Map<ItemCollection<QueryOutcome>, BindingSet> rangeMap;
	private Iterator<ItemCollection<QueryOutcome>> queryIterator;
	private IteratorSupport<Item, QueryOutcome> currentCursor;
	private BindingSet currentBindingSet;
	private DynamoStorageStrategy strategy;

	public RyaStatementBSItemIterator(Map<ItemCollection<QueryOutcome>,
			BindingSet> rangeMap, DynamoStorageStrategy strategy) {
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
			Item item = currentCursor.next();
			RyaStatement statement = strategy.convertToStatement(item);
			return new RdfCloudTripleStoreUtils.CustomEntry<RyaStatement, BindingSet>(statement, currentBindingSet);
		}
		return null;
	}
	
	private void findNextValidCursor() {
		while (queryIterator.hasNext()){
			ItemCollection<QueryOutcome> currentQuery = queryIterator.next();
			currentCursor = currentQuery.iterator();
			currentBindingSet = rangeMap.get(currentQuery);
			if (currentCursor.hasNext()) break;
		}
	}
	
	private boolean currentCursorIsValid() {
		return (currentCursor != null) && currentCursor.hasNext();
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
