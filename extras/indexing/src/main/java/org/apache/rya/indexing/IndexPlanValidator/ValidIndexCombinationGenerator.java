package org.apache.rya.indexing.IndexPlanValidator;

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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ValidIndexCombinationGenerator {

	private TupleExpr query;
	private Set<String> invalidCombos = Sets.newTreeSet();
	private Set<QueryModelNode> spFilterSet;

	public ValidIndexCombinationGenerator(TupleExpr query) {
		this.query = query;
		SpFilterCollector sfc = new SpFilterCollector();
		query.visit(sfc);
		spFilterSet = sfc.getSpFilterSet();
	}

	public Iterator<List<ExternalTupleSet>> getValidIndexCombos(
			List<ExternalTupleSet> indexSet) {

		Collections.shuffle(indexSet);
		final List<ExternalTupleSet> list = indexSet;
		final Iterator<List<Integer>> iter = getValidCombos(list);

		return new Iterator<List<ExternalTupleSet>>() {

			private List<ExternalTupleSet> next = null;
			private List<Integer> nextCombo = null;
			private boolean hasNextCalled = false;
			private boolean isEmpty = false;

			@Override
			public boolean hasNext() {

				if (!hasNextCalled && !isEmpty) {
					if (!iter.hasNext()) {
						isEmpty = true;
						return false;
					} else {
						nextCombo = iter.next();
						List<ExternalTupleSet> indexCombo = Lists
								.newArrayList();
						for (Integer i : nextCombo) {
							indexCombo.add(list.get(i));
						}
						next = indexCombo;
						hasNextCalled = true;
						return true;

					}

				} else if (isEmpty) {
					return false;
				} else {
					return true;
				}
			}

			@Override
			public List<ExternalTupleSet> next() {

				if (hasNextCalled) {
					hasNextCalled = false;
					return next;
				} else if (isEmpty) {
					throw new NoSuchElementException();
				} else {
					if (this.hasNext()) {
						hasNextCalled = false;
						return next;
					} else {
						throw new NoSuchElementException();
					}
				}
			}

			@Override
			public void remove() {

				throw new UnsupportedOperationException(
						"Cannot delete from iterator!");

			}

		};

	}

	private Iterator<List<Integer>> getValidCombos(
			List<ExternalTupleSet> indexList) {

		final List<ExternalTupleSet> list = indexList;
		final int indexSize = list.size();
		final Iterator<List<Integer>> iter = getCombos(indexSize);

		return new Iterator<List<Integer>>() {

			private List<Integer> next = null;
			private boolean hasNextCalled = false;
			private boolean isEmpty = false;

			@Override
			public boolean hasNext() {
				if (!hasNextCalled && !isEmpty) {

					while (iter.hasNext()) {
						List<Integer> tempNext = iter.next();
						if (isValid(tempNext, list)) {
							next = tempNext;
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
			public List<Integer> next() {

				if (hasNextCalled) {
					hasNextCalled = false;
					return next;
				} else if (isEmpty) {
					throw new NoSuchElementException();
				} else {
					if (this.hasNext()) {
						hasNextCalled = false;
						return next;
					} else {
						throw new NoSuchElementException();
					}

				}

			}

			@Override
			public void remove() {

				throw new UnsupportedOperationException(
						"Cannot delete from iterator!");

			}

		};
	}

	private Iterator<List<Integer>> getCombos(int indexListSize) {

		final int indexSize = indexListSize;
		final int maxSubListSize = spFilterSet.size() / 2;

		return new Iterator<List<Integer>>() {

			private List<Integer> next = null;
			private boolean hasNextCalled = false;
			private boolean isEmpty = false;
			private int subListSize = Math.min(maxSubListSize, indexSize) + 1;
			Iterator<List<Integer>> subList = null;

			@Override
			public boolean hasNext() {

				if (!hasNextCalled && !isEmpty) {
					if (subList != null && subList.hasNext()) {
						next = subList.next();
						hasNextCalled = true;
						return true;
					} else {
						subListSize--;
						if (subListSize == 0) {
							isEmpty = true;
							return false;
						}
						subList = getCombos(subListSize, indexSize);
						if (subList == null) {
							throw new IllegalStateException(
									"Combos cannot be null!");
						}
						next = subList.next();
						hasNextCalled = true;
						return true;

					}
				} else if (isEmpty) {
					return false;
				} else {
					return true;
				}
			}

			@Override
			public List<Integer> next() {

				if (hasNextCalled) {
					hasNextCalled = false;
					return next;
				} else if (isEmpty) {
					throw new NoSuchElementException();
				} else {
					if (this.hasNext()) {
						hasNextCalled = false;
						return next;
					} else {
						throw new NoSuchElementException();
					}

				}

			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException(
						"Cannot delete from iterator!");
			}

		};

	}

	private Iterator<List<Integer>> getCombos(int subListSize, int indexListSize) {

		if (subListSize > indexListSize) {
			throw new IllegalArgumentException(
					"Sublist size must be less than or equal to list size!");
		}

		final int subSize = subListSize;
		final int indexSize = indexListSize;

		return new Iterator<List<Integer>>() {

			private List<Integer> next = null;
			private List<Integer> tempList = Lists.newArrayList();
			private boolean calledHasNext = false;
			private boolean isEmpty = false;

			@Override
			public boolean hasNext() {

				if (!calledHasNext && !isEmpty) {
					if (next == null) {
						for (int i = 0; i < subSize; i++) {
							tempList.add(i);
						}
						next = tempList;
						calledHasNext = true;
						return true;
					} else {
						next = getNext(next, indexSize - 1);
						if (next == null) {
							isEmpty = true;
							return false;
						} else {
							calledHasNext = true;
							return true;
						}

					}
				} else if (isEmpty) {
					return false;
				} else {
					return true;
				}

			}

			@Override
			public List<Integer> next() {

				if (calledHasNext) {
					calledHasNext = false;
					return next;
				} else if (isEmpty) {
					throw new NoSuchElementException();
				} else {
					if (this.hasNext()) {
						calledHasNext = false;
						return next;
					} else {
						throw new NoSuchElementException();
					}
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();

			}

		};
	}

	private List<Integer> getNext(List<Integer> prev, int maxInt) {

		List<Integer> returnList = Lists.newArrayList();
		int size = prev.size();
		int incrementPos = -1;
		int incrementVal = 0;

		for (int i = 0; i < size; i++) {
			if (prev.get(size - (i + 1)) != maxInt - i) {
				incrementPos = size - (i + 1);
				break;
			}
		}

		if (incrementPos == -1) {
			return null;
		} else {

			incrementVal = prev.get(incrementPos);
			for (int i = 0; i < incrementPos; i++) {
				returnList.add(prev.get(i));
			}

			for (int j = incrementPos; j < size; j++) {
				returnList.add(++incrementVal);
			}

			return returnList;
		}
	}

	private boolean isValid(List<Integer> combo,
			List<ExternalTupleSet> indexList) {

		String s1 = Joiner.on("\u0000").join(combo).trim();

		if (invalidCombos.contains(s1)) {
			return false;
		} else {
			int valid = indicesDisjoint(combo, indexList);

			if (valid >= 0) {
				String s2 = "";
				for (int i = 0; i < valid + 1; i++) {
					if (s2.length() == 0) {
						s2 = s2 + combo.get(i);
					} else {
						s2 = s2 + "\u0000" + combo.get(i);
					}
				}
				invalidCombos.add(s2);

				for (int i = valid + 1; i < combo.size(); i++) {
					s2 = s2 + "\u0000" + combo.get(i);
					invalidCombos.add(s2);
				}

				return false;
			} else {
				return true;
			}
		}

	}

	private int indicesDisjoint(List<Integer> combo,
			List<ExternalTupleSet> indexList) {

		Set<QueryModelNode> indexNodes = Sets.newHashSet();
		Set<QueryModelNode> tempNodes;
		TupleExpr temp;

		int j = 0;
		for (Integer i : combo) {
			temp = indexList.get(i).getTupleExpr();
			SpFilterCollector spf = new SpFilterCollector();
			temp.visit(spf);
			tempNodes = spf.getSpFilterSet();
			if (Sets.intersection(indexNodes, tempNodes).size() == 0) {
				indexNodes = Sets.union(indexNodes, tempNodes);
				if (indexNodes.size() > spFilterSet.size()) {
					return j;
				}
			} else {
				return j;
			}
			j++;
		}

		return -1;
	}

	private static class SpFilterCollector extends
			QueryModelVisitorBase<RuntimeException> {

		private Set<QueryModelNode> spFilterSet = Sets.newHashSet();

		public int getNodeNumber() {
			return spFilterSet.size();
		}

		public Set<QueryModelNode> getSpFilterSet() {
			return spFilterSet;
		}

		@Override
		public void meet(StatementPattern node) {

			spFilterSet.add(node);
			return;

		}

		@Override
		public void meet(Filter node) {

			spFilterSet.add(node.getCondition());
			node.getArg().visit(this);
		}

	}
}
