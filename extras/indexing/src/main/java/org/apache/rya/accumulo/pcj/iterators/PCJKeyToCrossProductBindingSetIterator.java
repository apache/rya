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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.collect.HashBiMap;

/**
 *	This class takes in a {@link Scanner} and a Collection of BindingSets,
 *	deserializes each {@link Map.Entry<Key,Value>} taken from the Scanner into
 *  a {@link BindingSet}, and performs a cross product on the BindingSet with
 *  each BindingSet in the provided Collection.  The user can also specify a
 *  {@link Map<String, Value>} of constant constraints that can be used to filter.
 *
 */
public class PCJKeyToCrossProductBindingSetIterator implements
		CloseableIteration<BindingSet, QueryEvaluationException> {

	//BindingSets passed to PCJ used to form cross product
	private List<BindingSet> crossProductBs;
	//Scanner over PCJ table
	private Scanner scanner;
	//Iterator over PCJ scanner
	private Iterator<Map.Entry<Key, org.apache.accumulo.core.data.Value>> iterator;
	//Map of PCJ variables in table to variable in query
	private Map<String, String> pcjVarMap;
	//if PCJ contains LeftJoin, this is a set of variables that only appear in
	//LeftJoin.  Used when performing the cross product.
	private Set<String> unAssuredVariables;
	private final AccumuloPcjSerializer converter = new AccumuloPcjSerializer();
	private final BindingSet EMPTY_BINDINGSET = new QueryBindingSet();
	private Iterator<BindingSet> crossProductIter = Collections.emptyIterator();
	private Map<String, Value> constantConstraints;
	private BindingSet next;
	private boolean hasNextCalled = false;
	private boolean isEmpty = false;
	private boolean crossProductBsExist = false;
	private boolean constantConstraintsExist = false;

	public PCJKeyToCrossProductBindingSetIterator(Scanner scanner,
			List<BindingSet> crossProductBs,
			Map<String, Value> constantConstraints, Set<String> unAssuredVariables,
			Map<String, String> pcjVarMap) {
		this.crossProductBs = crossProductBs;
		this.scanner = scanner;
		this.iterator = scanner.iterator();
		this.pcjVarMap = HashBiMap.create(pcjVarMap).inverse();
		this.constantConstraints = constantConstraints;
		this.crossProductBsExist = crossProductBs.size() > 0;
		this.constantConstraintsExist = constantConstraints.size() > 0;
		this.unAssuredVariables = unAssuredVariables;
	}

	@Override
	public boolean hasNext() throws QueryEvaluationException {
		if (!hasNextCalled && !isEmpty) {
			if (crossProductBsExist) {
				while (crossProductIter.hasNext() || iterator.hasNext()) {
					if (!crossProductIter.hasNext()) {
						Key key = iterator.next().getKey();
						try {
							crossProductIter = getCrossProducts(getBindingSet(key));
						} catch (BindingSetConversionException e) {
							throw new QueryEvaluationException(e);
						}
					}
					if (!crossProductIter.hasNext()) {
						continue;
					}
					next = crossProductIter.next();
					hasNextCalled = true;
					return true;
				}
			} else {
				while (iterator.hasNext()) {
					Key key = iterator.next().getKey();
					try {
						next = getBindingSet(key);
					} catch (BindingSetConversionException e) {
						throw new QueryEvaluationException(e);
					}
					//BindingSet cannot be deserialized or is filtered
					//out by constant constraints
					if (next == null || next == EMPTY_BINDINGSET) {
						continue;
					}
					hasNextCalled = true;
					return true;
				}
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
	public BindingSet next() throws QueryEvaluationException {
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
	 * @return - BindingSet satisfying any specified constant constraints
	 * @throws BindingSetConversionException
	 * @throws QueryEvaluationException
	 */
	private BindingSet getBindingSet(Key key)
			throws BindingSetConversionException, QueryEvaluationException {
		byte[] row = key.getRow().getBytes();
		String[] varOrder = key.getColumnFamily().toString()
				.split(ExternalTupleSet.VAR_ORDER_DELIM);

		BindingSet bindingSet = converter.convert(row, new VariableOrder(
				varOrder));

		QueryBindingSet bs = new QueryBindingSet();
		for (String var : bindingSet.getBindingNames()) {
			String mappedVar = null;
			if(pcjVarMap.containsKey(var)) {
				mappedVar = pcjVarMap.get(var);
			} else {
				throw new QueryEvaluationException("PCJ Variable has no mapping to query variable.");
			}
			if (constantConstraintsExist) {
				if (mappedVar.startsWith(ExternalTupleSet.CONST_PREFIX)
						&& constantConstraints.containsKey(mappedVar)
						&& !constantConstraints.get(mappedVar).equals(
								bindingSet.getValue(var))) {
					return EMPTY_BINDINGSET;
				}
			}

			if (!mappedVar.startsWith(ExternalTupleSet.CONST_PREFIX)) {
					bs.addBinding(mappedVar, bindingSet.getValue(var));
			}
		}
		return bs;
	}

	/**
	 * This method forms the cross-product between an input BindingSet and the
	 * BindingSets contained in crossProdcutBs.
	 *
	 * @param bs
	 *            - {@link BindingSet} used to form cross product with
	 *            cross-product BindingSets
	 * @return - Iterator over resulting cross-product
	 */
	private Iterator<BindingSet> getCrossProducts(BindingSet bs) {
		Set<BindingSet> crossProducts = new HashSet<BindingSet>();

		for (BindingSet bSet : crossProductBs) {
			BindingSet prod = takeCrossProduct(bSet, bs);
			if (prod != EMPTY_BINDINGSET) {
				crossProducts.add(prod);
			}
		}

		return crossProducts.iterator();

	}

	/**
	 * This method compute the cross product of the BindingSet passed to the PCJ
	 * and the PCJ BindingSet.  It verifies that only common variables are unassured
	 * variables, and if leftBs and rightBs have distinct values for a given variable,
	 * this method uses the value from leftBs in the cross product BindingSet - this
	 * is effectively performing a LeftJoin.
	 *
	 * @param leftBs - BindingSet passed to PCJ
	 * @param rightBs - PCJ BindingSet
	 * @return - cross product BindingSet
	 */
	private BindingSet takeCrossProduct(BindingSet leftBs, BindingSet rightBs) {
		if (bindingSetsIntersect(leftBs, rightBs)) {
			return EMPTY_BINDINGSET;
		}
		QueryBindingSet bs = new QueryBindingSet(leftBs);

		//only add Bindings corresponding to variables that have no value
		//assigned.  This takes into account case where leftBs and rightBs
		//share a common, unAssuredVariable.  In this case, use value corresponding
		//to leftBs, which is effectively performing a LeftJoin.
		for(String s: rightBs.getBindingNames()) {
			if(bs.getValue(s) == null) {
				bs.addBinding(s, rightBs.getValue(s));
			}
		}
		return bs;
	}

	private boolean bindingSetsIntersect(BindingSet bs1, BindingSet bs2) {

		for(String s: bs1.getBindingNames()) {
			if(bs2.getValue(s) != null && !unAssuredVariables.contains(s)) {
				return true;
			}
		}
		return false;
	}

}
