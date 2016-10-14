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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * This {@link CloseableIteration} performs a hash join by joining each
 * {@link Map.Entry<String, BindingSet>} with all corresponding
 * {@link BindingSet} in a Multimap with the same String key.
 *
 */
public class BindingSetHashJoinIterator implements
		CloseableIteration<BindingSet, QueryEvaluationException> {

	//BindingSets passed to PCJ mapped according to values
	//associated with common variables with table
	private Multimap<String, BindingSet> bindingJoinVarHash;
	//BindingSets taken from PCJ table
	private CloseableIteration<Map.Entry<String, BindingSet>, QueryEvaluationException> joinIter;
	private Iterator<BindingSet> joinedBindingSets = Collections
			.emptyIterator();
	//If PCJ contains LeftJoin, this is a set of variable in LeftJoin.  Used when performing Join.
	private Set<String> unAssuredVariables;
	//indicates when HashJoin formed from a single collection of join variable or if the size and
	//collection of join variables varies -- this is to optimize the join process
	private HashJoinType type;
	private final BindingSet EMPTY_BINDINGSET = new QueryBindingSet();
	private BindingSet next;
	private boolean hasNextCalled = false;
	private boolean isEmpty = false;

	/**
	 * Enum type to indicate whether HashJoin will be performed over a fixed
	 * subset of variables common to each {@link BindingSet}, or if there is a
	 * collection of variable subsets over which to join.
	 *
	 */
	public enum HashJoinType {
		CONSTANT_JOIN_VAR, VARIABLE_JOIN_VAR
	};

	public BindingSetHashJoinIterator(
			Multimap<String, BindingSet> bindingJoinVarHash,
			CloseableIteration<Map.Entry<String, BindingSet>, QueryEvaluationException> joinIter,
			Set<String> unAssuredVariables, HashJoinType type) {
		this.bindingJoinVarHash = bindingJoinVarHash;
		this.joinIter = joinIter;
		this.type = type;
		this.unAssuredVariables = unAssuredVariables;
	}

	@Override
	public boolean hasNext() throws QueryEvaluationException {
		if (!hasNextCalled && !isEmpty) {
			while (joinedBindingSets.hasNext() || joinIter.hasNext()) {
				if (!joinedBindingSets.hasNext()) {
					Entry<String, BindingSet> entry = joinIter.next();
					joinedBindingSets = joinBindingSetEntry(entry);
				}
				if (!joinedBindingSets.hasNext()) {
					continue;
				}
				next = joinedBindingSets.next();
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
		joinIter.close();
	}

	/**
	 * This method takes the valOrderString, which is a key used for computing
	 * hash joins, and generates multiple keys by pulling off one delimiter
	 * separated component at a time. This is used when the size of the join key
	 * varies from {@link Map.Entry} to Entry. It allows the BindingSet to be
	 * joined using all prefixes of the key.
	 *
	 * @param valOrderString
	 *            - key used for hash join
	 * @return
	 */
	private List<String> getValueOrders(String valOrderString) {

		List<String> valueOrders = new ArrayList<>();
		String[] splitValOrderString = valOrderString
				.split(ExternalTupleSet.VALUE_DELIM);
		StringBuffer buffer = new StringBuffer();
		buffer.append(splitValOrderString[0]);
		valueOrders.add(buffer.substring(0));

		for (int i = 1; i < splitValOrderString.length; i++) {
			buffer.append(ExternalTupleSet.VALUE_DELIM + splitValOrderString[i]);
			valueOrders.add(buffer.substring(0));
		}

		return valueOrders;
	}

	/**
	 * This method verifies that all common variables have a common value and
	 * then joins the BindingSets together. In the case that the PCJ contains a
	 * LeftJoin, if the leftBs and rightBs have a common variable with distinct
	 * values and that common variable is unassured (only appears in LeftJoin),
	 * this method uses the value corresponding to leftBs.
	 *
	 * @param leftBs
	 *            - BindingSet passed into PCJ
	 * @param rightBs
	 *            - PCJ BindingSet
	 * @return - joined BindingSet
	 */
	private BindingSet joinBindingSets(BindingSet leftBs, BindingSet rightBs) {

		Set<String> commonVars = Sets.intersection(leftBs.getBindingNames(),
				rightBs.getBindingNames());
		// compare values associated with common variables to make sure
		// BindingSets can be joined. Possible for leftBs and rightBs
		// to have a common unAssuredVariable in event PCJ contains LeftJoin.
		// if values corresponding to common unAssuredVariable do not agree
		// add value corresponding to leftBs
		for (String s : commonVars) {
			if (!leftBs.getValue(s).equals(rightBs.getValue(s))
					&& !unAssuredVariables.contains(s)) {
				return EMPTY_BINDINGSET;
			}
		}
		QueryBindingSet bs = new QueryBindingSet(removeConstants(leftBs));

		rightBs = removeConstants(rightBs);
		// only add Bindings corresponding to variables that have no value
		// assigned. This takes into account case where leftBs and rightBs
		// share a common, unAssuredVariable. In this case, use value
		// corresponding
		// to leftBs, which is effectively performing a LeftJoin.
		for (String s : rightBs.getBindingNames()) {
			if (bs.getValue(s) == null) {
				bs.addBinding(s, rightBs.getValue(s));
			}
		}

		return bs;
	}

	private BindingSet removeConstants(BindingSet bs) {
		QueryBindingSet bSet = new QueryBindingSet();
		for (String s : bs.getBindingNames()) {
			if (!s.startsWith(ExternalTupleSet.CONST_PREFIX)) {
				bSet.addBinding(bs.getBinding(s));
			}
		}
		return bSet;
	}

	/**
	 * This method returns an Iterator which joins the given Entry's BindingSet
	 * to all BindingSets which matching the Entry's key.
	 *
	 * @param entry - entry to be joined
	 * @return - Iterator over joined BindingSets
	 */
	private Iterator<BindingSet> joinBindingSetEntry(
			Map.Entry<String, BindingSet> entry) {

		List<Collection<BindingSet>> matches = new ArrayList<>();
		if (type == HashJoinType.CONSTANT_JOIN_VAR) {
			if (bindingJoinVarHash.containsKey(entry.getKey())) {
				matches.add(bindingJoinVarHash.get(entry.getKey()));
			}
		} else {
			List<String> valOrders = getValueOrders(entry.getKey());
			for (String s : valOrders) {
				if (bindingJoinVarHash.containsKey(s)) {
					matches.add(bindingJoinVarHash.get(s));
				}
			}
		}

		if (matches.size() == 0) {
			return Collections.emptyIterator();
		} else {
			return new BindingSetCollectionsJoinIterator(entry.getValue(),
					matches);
		}

	}

	/**
	 * Given a BindingSet and a List of Collections of BindingSets, this
	 * Iterator joins the BindingSet with the BindingSets in each Collection
	 *
	 */
	private class BindingSetCollectionsJoinIterator implements
			Iterator<BindingSet> {

		private Iterator<Collection<BindingSet>> collectionIter;
		private Iterator<BindingSet> bsIter = Collections.emptyIterator();
		private BindingSet next;
		private BindingSet joinBs;
		private boolean hasNextCalled = false;
		private boolean isEmpty = false;

		public BindingSetCollectionsJoinIterator(BindingSet bs,
				List<Collection<BindingSet>> collection) {
			this.collectionIter = collection.iterator();
			this.joinBs = bs;
		}

		@Override
		public boolean hasNext() {

			if (!hasNextCalled && !isEmpty) {
				while (bsIter.hasNext() || collectionIter.hasNext()) {
					if (!bsIter.hasNext()) {
						bsIter = collectionIter.next().iterator();
					}
					next = joinBindingSets(bsIter.next(), joinBs);
					if (next == EMPTY_BINDINGSET) {
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
		public BindingSet next() {
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
		public void remove() {
			throw new UnsupportedOperationException();
		}

	}

}
