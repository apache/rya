package mvm.rya.dynamodb.dao;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.calrissian.mango.collect.CloseableIterable;
import org.openrdf.query.BindingSet;

import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.persist.RyaDAOException;
import mvm.rya.api.persist.query.BatchRyaQuery;
import mvm.rya.api.persist.query.RyaQuery;
import mvm.rya.api.persist.query.RyaQueryEngine;
import mvm.rya.dynamodb.iter.NonCloseableRyaItemIterator;
import mvm.rya.dynamodb.iter.RyaItemCloseableIterable;
import mvm.rya.dynamodb.iter.RyaItemCollectionIterator;
import mvm.rya.dynamodb.iter.RyaStatementBSItemIterator;

public class DynamoQueryEngine implements RyaQueryEngine<DynamoRdfConfiguration> {
	
	
	private DynamoRdfConfiguration conf;
	private DynamoStorageStrategy storageStrategy;
	
	

	public DynamoQueryEngine(DynamoRdfConfiguration conf2, DynamoStorageStrategy strategy) {
		this.conf = conf2;
		this.storageStrategy = strategy;
	}

	@Override
	public void setConf(DynamoRdfConfiguration conf) {
		this.conf = conf;
	}
	
	public void setStorageStrategy(DynamoStorageStrategy strategy){
		this.storageStrategy = strategy;
	}

	@Override
	public DynamoRdfConfiguration getConf() {
		return conf;
	}

	@Override
	public CloseableIteration<RyaStatement, RyaDAOException> query(RyaStatement stmt, DynamoRdfConfiguration conf)
			throws RyaDAOException {
		if (conf == null){
			this.conf = conf;
		}
		ItemCollection<QueryOutcome> dynamoReturn = storageStrategy.getQuery(stmt);
		return new RyaItemCollectionIterator(Collections.singleton(dynamoReturn), storageStrategy);
	}

	@Override
	public CloseableIteration<? extends Entry<RyaStatement, BindingSet>, RyaDAOException> queryWithBindingSet(
			Collection<Entry<RyaStatement, BindingSet>> stmts, DynamoRdfConfiguration conf) throws RyaDAOException {
	       final Map<ItemCollection<QueryOutcome>, BindingSet> rangeMap = new HashMap<ItemCollection<QueryOutcome>, BindingSet>();

	        try {
	            for (Map.Entry<RyaStatement, BindingSet> stmtbs : stmts) {
	                RyaStatement stmt = stmtbs.getKey();
	                BindingSet bs = stmtbs.getValue();
	                ItemCollection<QueryOutcome> query = storageStrategy.getQuery(stmt);
	                rangeMap.put(query, bs);
	            }

	            // TODO not sure what to do about regex ranges?
	           RyaStatementBSItemIterator iterator = new RyaStatementBSItemIterator(rangeMap, storageStrategy);
	            return iterator;
	        } catch (final Exception e) {
	            throw new RyaDAOException(e);
	        }
	}

	@Override
	public CloseableIteration<RyaStatement, RyaDAOException> batchQuery(Collection<RyaStatement> stmts,
			DynamoRdfConfiguration conf) throws RyaDAOException {
		Collection<ItemCollection<QueryOutcome>> dynamoReturn = storageStrategy.getBatchQuery(stmts);
		return new RyaItemCollectionIterator(dynamoReturn, storageStrategy);
	}

	@Override
	public CloseableIterable<RyaStatement> query(RyaQuery ryaQuery) throws RyaDAOException {
		ItemCollection<QueryOutcome> dynamoReturn = storageStrategy.getQuery(ryaQuery.getQuery());
		return new RyaItemCloseableIterable(
				new NonCloseableRyaItemIterator(
						new RyaItemCollectionIterator(Collections.singleton(dynamoReturn), storageStrategy)));
	}

	@Override
	public CloseableIterable<RyaStatement> query(BatchRyaQuery batchRyaQuery) throws RyaDAOException {
		Collection<ItemCollection<QueryOutcome>> dynamoReturn = storageStrategy.getBatchQuery(batchRyaQuery.getQueries());
		return new RyaItemCloseableIterable(
				new NonCloseableRyaItemIterator(
						new RyaItemCollectionIterator(dynamoReturn, storageStrategy)));
	}
}
