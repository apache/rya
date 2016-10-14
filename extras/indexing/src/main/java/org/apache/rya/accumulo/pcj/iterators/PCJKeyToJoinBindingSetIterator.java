package org.apache.rya.accumulo.pcj.iterators;

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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBiMap;

/**
 * This class takes in a {@link Scanner} and a Collection of BindingSets,
 * deserializes each {@link Map.Entry<Key,Value>} taken from the Scanner into a
 * {@link BindingSet}, and creates a {@link Map.Entry<String, BindingSet>}
 * object to perform as hash join. The user can also specify a {@link Map
 * <String, Value>} of constant constraints that can be used to filter.
 *
 */
public class PCJKeyToJoinBindingSetIterator
		implements
		CloseableIteration<Map.Entry<String, BindingSet>, QueryEvaluationException> {

	//map of variables as they appear in PCJ table to query variables
	private Map<String, String> pcjVarMap;
	//constant constraints used for filtering
	private Map<String, Value> constantConstraints;
	//max number of variables an entry in the batch of BindingSets had in common with PCJ
	//this is used for constructing hash join key.
	private int maxPrefixLen;
	private static final AccumuloPcjSerializer converter = new AccumuloPcjSerializer();
	private final Map.Entry<String, BindingSet> EMPTY_ENTRY = new RdfCloudTripleStoreUtils.CustomEntry<String, BindingSet>(
			"", new QueryBindingSet());
	private Iterator<Entry<Key, org.apache.accumulo.core.data.Value>> iterator;
	private boolean hasNextCalled = false;
	private boolean isEmpty = false;
	private Map.Entry<String, BindingSet> next;
	private BatchScanner scanner;

	public PCJKeyToJoinBindingSetIterator(BatchScanner scanner,
			Map<String, String> pcjVarMap,
			Map<String, Value> constantConstraints, int maxPrefixLen) {
		Preconditions.checkNotNull(scanner);
		Preconditions.checkArgument(pcjVarMap.size() > 0,
				"Variable map must contain at least one variable!");
		Preconditions.checkNotNull(constantConstraints,
				"Constant constraints cannot be null.");
		Preconditions.checkArgument(maxPrefixLen > 0,
				"Max prefix length must be greater than 0.");
		Preconditions
				.checkArgument(maxPrefixLen <= pcjVarMap.size(),
						"Max prefix length must be less than total number of binding names.");
		this.scanner = scanner;
		this.pcjVarMap = HashBiMap.create(pcjVarMap).inverse();
		this.constantConstraints = constantConstraints;
		this.maxPrefixLen = maxPrefixLen;
		this.iterator = scanner.iterator();

	}

	public PCJKeyToJoinBindingSetIterator(BatchScanner scanner,
			Map<String, String> pcjVarMap, int maxPrefixLen) {
		this(scanner, pcjVarMap, new HashMap<String, Value>(), maxPrefixLen);
	}

	@Override
	public boolean hasNext() throws QueryEvaluationException {

		if (!hasNextCalled && !isEmpty) {
			while (iterator.hasNext()) {
				Key key = iterator.next().getKey();
				// get bindings from scan without values associated with
				// constant constraints
				try {
					next = getBindingSetEntryAndMatchConstants(key);
				} catch (BindingSetConversionException e) {
					throw new QueryEvaluationException(
							"Could not deserialize PCJ BindingSet.");
				}
				// skip key if constant constraint don't match
				if (next == EMPTY_ENTRY) {
					continue;
				}
				hasNextCalled = true;
				return true;
			}
			isEmpty = true;
			return false;
		} else if (isEmpty) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public Entry<String, BindingSet> next() throws QueryEvaluationException {
		if (hasNextCalled) {
			hasNextCalled = false;
		} else if (isEmpty) {
			throw new NoSuchElementException();
		} else {
			if (this.hasNext()) {
				hasNextCalled = false;
			} else {
				throw new NoSuchElementException();
			}
		}
		return next;
	}

	@Override
	public void remove() throws QueryEvaluationException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws QueryEvaluationException {
		scanner.close();
	}

	/**
	 *
	 * @param key
	 *            - Accumulo key obtained from scan
	 * @return - Entry<String,BindingSet> satisfying the constant constraints
	 * @throws BindingSetConversionException
	 */
	private Map.Entry<String, BindingSet> getBindingSetEntryAndMatchConstants(
			Key key) throws BindingSetConversionException {
		byte[] row = key.getRow().getBytes();
		String[] varOrder = key.getColumnFamily().toString()
				.split(ExternalTupleSet.VAR_ORDER_DELIM);

		BindingSet bindingSet = converter.convert(row, new VariableOrder(
				varOrder));

		QueryBindingSet bs = new QueryBindingSet();
		for (String var : bindingSet.getBindingNames()) {
			String mappedVar = pcjVarMap.get(var);
			if (mappedVar.startsWith(ExternalTupleSet.CONST_PREFIX)
					&& constantConstraints.containsKey(mappedVar)
					&& !constantConstraints.get(mappedVar).equals(
							bindingSet.getValue(var))) {
				return EMPTY_ENTRY;
			} else {
				bs.addBinding(mappedVar, bindingSet.getValue(var));
			}
		}

		String orderedValueString = bindingSet.getValue(varOrder[0]).toString();
		for (int i = 1; i < maxPrefixLen; i++) {
			Value value = bindingSet.getValue(varOrder[i]);
			if (value != null) {
				orderedValueString = orderedValueString
						+ ExternalTupleSet.VALUE_DELIM + value.toString();
			}
		}

		return new RdfCloudTripleStoreUtils.CustomEntry<String, BindingSet>(
				orderedValueString, bs);
	}

}
