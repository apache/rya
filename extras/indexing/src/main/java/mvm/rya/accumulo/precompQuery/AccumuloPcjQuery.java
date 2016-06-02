package mvm.rya.accumulo.precompQuery;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

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

/**
 *
 */
import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.Iteration;
import mvm.rya.api.resolver.RyaTypeResolverException;
import mvm.rya.indexing.PcjQuery;
import mvm.rya.indexing.external.tupleSet.AccumuloIndexSet;
import mvm.rya.indexing.external.tupleSet.ExternalTupleSet;

/**
 * This class encapsulates how pre-computed join tables are used during query
 * evaluation. The method
 * {@link AccumuloPcjQuery#queryPrecompJoin(List, String, Map, Map, Collection)
 * is used by {@link AccumuloIndexSet#evaluate(BindingSet)} to evaluate the
 * {@link AccumuloIndexSet#getTupleExpr()} associated with the Accumulo
 * pre-computed join table. Given the {@link BindingSet} constraints, it uses
 * the variables common to the BindingSet constraints and the pre-computed join
 * table TupleExpr to build a {@Range} prefix to scan the pre-computed
 * join table to obtain results for the constrained sub-query.
 *
 */
public class AccumuloPcjQuery implements PcjQuery {
	private static final AccumuloPcjSerializer converter = new AccumuloPcjSerializer();

	private final Connector accCon;
	private final String tableName;

	public AccumuloPcjQuery(Connector accCon, String tableName) {
		this.accCon = accCon;
		this.tableName = tableName;
	}

	/**
	 * @param commonVars - variables common to bsConstraints and table in terms of query variables
	 * @param localityGroup - the column family to scan in terms of table variables
	 * @param valMap - Literal type associated with constant constraints
	 * @param varMap - map query variables to pre-computed join table variables
	 * @param bsConstraints - binding set constraints
	 * @return {@link Iteration} over result BindingSets
	 */
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> queryPrecompJoin(
			List<String> commonVars, String localityGroup,
			Map<String, org.openrdf.model.Value> valMap,
			Map<String, String> varMap, Collection<BindingSet> bsConstraints)
			throws QueryEvaluationException, TableNotFoundException {

		final Iterator<Entry<Key, Value>> accIter;
		final Map<String, org.openrdf.model.Value> constValMap = valMap;
		final HashMultimap<Range, BindingSet> map = HashMultimap.create();

		final List<BindingSet> extProdList = Lists.newArrayList();
		final List<String> prefixVars = commonVars;
		final BatchScanner bs = accCon.createBatchScanner(tableName,
				new Authorizations(), 10);
		final Set<Range> ranges = Sets.newHashSet();
		final Map<String, String> tableVarMap = varMap;
		final boolean bsContainsPrefixVar = bindingsContainsPrefixVar(
				bsConstraints, prefixVars);

		bs.fetchColumnFamily(new Text(localityGroup));
		// process bindingSet and constant constraints
		for (final BindingSet bSet : bsConstraints) {
			// bindings sets and PCJ have common vars
			if (bsContainsPrefixVar) {
				byte[] rangePrefix = null;
				final QueryBindingSet rangeBs = new QueryBindingSet();
				for (final String var : prefixVars) {
					if (var.startsWith(ExternalTupleSet.CONST_PREFIX)) {
						rangeBs.addBinding(var, constValMap.get(var));
					} else {
						rangeBs.addBinding(var, bSet.getBinding(var).getValue());
					}
				}
				try {
					rangePrefix = converter.convert(rangeBs, new VariableOrder(commonVars));
				} catch (final BindingSetConversionException e) {
					throw new QueryEvaluationException(e);
				}
				final Range r = Range.prefix(new Text(rangePrefix));
				map.put(r, bSet);
				ranges.add(r);
				// non-empty binding sets and no common vars with no constant
				// constraints
			} else if (bSet.size() > 0 && prefixVars.size() == 0) {
				extProdList.add(bSet);
			}
		}

		// constant constraints and no bindingSet constraints
		// add range of entire table if no constant constraints and
		// bsConstraints consists of single, empty set (occurs when AIS is
		// first node evaluated in query)
		if (ranges.isEmpty()) {
			// constant constraints
			if (prefixVars.size() > 0) {
				byte[] rangePrefix = null;
				final QueryBindingSet rangeBs = new QueryBindingSet();
				for (final String var : prefixVars) {
					if (var.startsWith(ExternalTupleSet.CONST_PREFIX)) {
						rangeBs.addBinding(var, constValMap.get(var));
					}
				}
				try {
					rangePrefix = converter.convert(rangeBs, new VariableOrder(commonVars));
				} catch (final BindingSetConversionException e) {
					throw new QueryEvaluationException(e);
				}
				final Range r = Range.prefix(new Text(rangePrefix));
				ranges.add(r);
			}
			// no constant or bindingSet constraints
			else {
				ranges.add(new Range("", true, "~", false));
			}
		}

		if (ranges.size() == 0) {
			accIter = null;
		} else {
			bs.setRanges(ranges);
			accIter = bs.iterator();
		}
		return new CloseableIteration<BindingSet, QueryEvaluationException>() {
			@Override
			public void remove() throws QueryEvaluationException {
				throw new UnsupportedOperationException();
			}

			private Iterator<BindingSet> inputSet = null;
			private QueryBindingSet currentSolutionBs = null;
			private boolean hasNextCalled = false;
			private boolean isEmpty = false;

			@Override
			public BindingSet next() throws QueryEvaluationException {
				final QueryBindingSet bs = new QueryBindingSet();
				if (hasNextCalled) {
					hasNextCalled = false;
					if (inputSet != null) {
						bs.addAll(inputSet.next());
					}
					bs.addAll(currentSolutionBs);
				} else if (isEmpty) {
					throw new NoSuchElementException();
				} else {
					if (this.hasNext()) {
						hasNextCalled = false;
						if (inputSet != null) {
							bs.addAll(inputSet.next());
						}
						bs.addAll(currentSolutionBs);
					} else {
						throw new NoSuchElementException();
					}
				}
				return bs;
			}

			@Override
			public boolean hasNext() throws QueryEvaluationException {
				if (accIter == null) {
					isEmpty = true;
					return false;
				}
				if (!hasNextCalled && !isEmpty) {
					while (accIter.hasNext() || inputSet != null
							&& inputSet.hasNext()) {
						if (inputSet != null && inputSet.hasNext()) {
							hasNextCalled = true;
							return true;
						}
						final Key k = accIter.next().getKey();
						// get bindings from scan without values associated with
						// constant constraints
						BindingSet bs;
						try {
							bs = getBindingSetWithoutConstants(k, tableVarMap);
						} catch (final BindingSetConversionException e) {
							throw new QueryEvaluationException(e);
						}
						currentSolutionBs = new QueryBindingSet();
						currentSolutionBs.addAll(bs);

						// check to see if additional bindingSet constraints
						// exist in map
						if (map.size() > 0) {
							// get prefix range to retrieve remainder of
							// bindingSet from map
							byte[] rangePrefix;
							try {
								rangePrefix = getPrefixByte(bs, constValMap,
										prefixVars);
							} catch (final BindingSetConversionException e) {
								throw new QueryEvaluationException(e);
							}
							final Range r = Range.prefix(new Text(rangePrefix));
							inputSet = map.get(r).iterator();
							if (!inputSet.hasNext()) {
								continue;
							} else {
								hasNextCalled = true;
								return true;
							}
							// check to see if binding set constraints exist,
							// but no common vars
						} else if (extProdList.size() > 0) {
							inputSet = extProdList.iterator();
							hasNextCalled = true;
							return true;
						}
						// no bindingsSet constraints--only constant constraints
						// or none
						else {
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
			public void close() throws QueryEvaluationException {
				bs.close();
			}
		};
	}

	/**
	 *
	 * @param bindingSets - binding set constraints
	 * @param prefixVars - common prefix variables to table and any constant constraints
	 * @return true if there are variables common to binding sets and table and false
	 * if prefixVars only consists of constant constraints
	 */
	private boolean bindingsContainsPrefixVar(
			Collection<BindingSet> bindingSets, List<String> prefixVars) {
		final Iterator<BindingSet> iter = bindingSets.iterator();
		if (iter.hasNext()) {
			final BindingSet tempBindingSet = iter.next();
			final Set<String> bindings = tempBindingSet.getBindingNames();
			for (final String var : prefixVars) {
				if (bindings.contains(var)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 *
	 * @param bs - binding set from which byte is extracted
	 * @param valMap - map which associated Value type to constant constraint
	 * @param prefixVars - prefix of variables common to binding sets and table and constant constraints
	 * @return - bytes associated with values in bs that are associated with prefixVars
	 * @throws RyaTypeResolverException
	 */
	private static byte[] getPrefixByte(BindingSet bs,
			Map<String, org.openrdf.model.Value> valMap, List<String> prefixVars)
			throws BindingSetConversionException {
		final QueryBindingSet bSet = new QueryBindingSet();
		for (final String var : prefixVars) {
			if (var.startsWith(ExternalTupleSet.CONST_PREFIX)) {
				bSet.addBinding(var, valMap.get(var));
			} else if (bs.getBindingNames().size() > 0
					&& bs.getBinding(var) != null) {
				bSet.addBinding(var, bs.getBinding(var).getValue());
			}
		}

		return converter.convert(bSet, new VariableOrder(prefixVars));
	}

	/**
	 *
	 * @param key - Accumulo key obtained from scan
	 * @param tableVarMap - map that associated query variables and table variables
	 * @return - BindingSet without values associated with constant constraints
	 * @throws BindingSetConversionException
	 */
	private static BindingSet getBindingSetWithoutConstants(Key key,
			Map<String, String> tableVarMap) throws BindingSetConversionException {
		final byte[] row = key.getRow().getBytes();
		final String[] varOrder = key.getColumnFamily().toString()
				.split(ExternalTupleSet.VAR_ORDER_DELIM);

		BindingSet bindingSet = converter.convert(row, new VariableOrder(varOrder));
		final QueryBindingSet temp = new QueryBindingSet(bindingSet);

		final QueryBindingSet bs = new QueryBindingSet();
		for (final String var : temp.getBindingNames()) {
			if (!tableVarMap.get(var).startsWith(ExternalTupleSet.CONST_PREFIX)) {
				bs.addBinding(tableVarMap.get(var), temp.getValue(var));
			}
		}
		return bs;
	}

}
