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
package org.apache.rya.indexing.external.tupleSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.PrecomputedJoinStorage.PCJStorageException;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.sail.SailException;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import info.aduna.iteration.CloseableIteration;
import org.apache.rya.accumulo.pcj.iterators.BindingSetHashJoinIterator;
import org.apache.rya.accumulo.pcj.iterators.BindingSetHashJoinIterator.HashJoinType;
import org.apache.rya.accumulo.pcj.iterators.IteratorCombiner;
import org.apache.rya.accumulo.pcj.iterators.PCJKeyToCrossProductBindingSetIterator;
import org.apache.rya.accumulo.pcj.iterators.PCJKeyToJoinBindingSetIterator;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.utils.IteratorWrapper;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.pcj.matching.PCJOptimizerUtilities;
import org.apache.rya.rdftriplestore.evaluation.ExternalBatchingIterator;

/**
 * During query planning, this node is inserted into the parsed query to
 * represent part of the original query (a sub-query). This sub-query is the
 * value returned by {@link ExternalTupleSet#getTupleExpr()}. The results
 * associated with this sub-query are stored in an external Accumulo table,
 * where accCon and tablename are the associated {@link Connector} and table
 * name. During evaluation, the portion of the query in {@link AccumuloIndexSet}
 * is evaluated by scanning the external Accumulo table. This class is extremely
 * useful for caching queries and reusing results from previous SPARQL queries.
 * <p>
 *
 * The the {@link TupleExpr} returned by {@link ExternalTupleSet#getTupleExpr()}
 * may have different variables than the query and variables stored in the
 * external Accumulo table. The mapping of variables from the TupleExpr to the
 * table variables are given by {@link ExternalTupleSet#getTableVarMap()}. In
 * addition to allowing the variables to differ, it is possible for TupleExpr to
 * have fewer variables than the table query--that is, some of the variables in
 * the table query may appear as constants in the TupleExpr. Theses expression
 * are extracted from TupleExpr by the methods
 * {@link AccumuloIndexSet#getConstantConstraints()} and by the Visitor
 * {@link ValueMapVisitor} to be used as constraints when scanning the Accumulo
 * table. This allows for the pre-computed results to be used for a larger class
 * of sub-queries.
 *
 */
public class AccumuloIndexSet extends ExternalTupleSet implements
		ExternalBatchingIterator {

	private final Connector accCon; // connector to Accumulo table where results
									// are stored
	private final String tablename; // name of Accumulo table
	private List<String> varOrder = null; // orders in which results are written
											// to table
	private final PcjTables pcj = new PcjTables();
	private final Authorizations auths;


	@Override
	public Map<String, Set<String>> getSupportedVariableOrders() {
		return this.getSupportedVariableOrderMap();
	}

	@Override
	public String getSignature() {
		return "AccumuloIndexSet(" + tablename + ") : "
				+ Joiner.on(", ").join(this.getTupleExpr().getBindingNames());
	}

	/**
	 *
	 * @param sparql
	 *            - name of sparql query whose results will be stored in PCJ
	 *            table
	 * @param accCon
	 *            - connection to a valid Accumulo instance
	 * @param tablename
	 *            - name of an existing PCJ table
	 * @throws MalformedQueryException
	 * @throws SailException
	 * @throws QueryEvaluationException
	 * @throws TableNotFoundException
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 * @throws PCJStorageException
	 */
	public AccumuloIndexSet(final String sparql, final Configuration conf,
			final String tablename) throws MalformedQueryException, SailException,
			QueryEvaluationException, TableNotFoundException, AccumuloException, AccumuloSecurityException, PCJStorageException {
		this.tablename = tablename;
		this.accCon = ConfigUtils.getConnector(conf);
		this.auths = getAuthorizations(conf);
		final SPARQLParser sp = new SPARQLParser();
		final ParsedTupleQuery pq = (ParsedTupleQuery) sp.parseQuery(sparql, null);
		final TupleExpr te = pq.getTupleExpr();
		Preconditions.checkArgument(PCJOptimizerUtilities.isPCJValid(te),
				"TupleExpr is an invalid PCJ.");

		final Optional<Projection> projection = new ParsedQueryUtil()
				.findProjection(pq);
		if (!projection.isPresent()) {
			throw new MalformedQueryException("SPARQL query '" + sparql
					+ "' does not contain a Projection.");
		}
		setProjectionExpr(projection.get());
		Set<VariableOrder> orders = null;
		orders = pcj.getPcjMetadata(accCon, tablename).getVarOrders();

		varOrder = Lists.newArrayList();
		for (final VariableOrder var : orders) {
			varOrder.add(var.toString());
		}
		setLocalityGroups(tablename, accCon, varOrder);
		this.setSupportedVariableOrderMap(varOrder);
	}

	/**
	 *
	 * @param accCon
	 *            - connection to a valid Accumulo instance
	 * @param tablename
	 *            - name of an existing PCJ table
	 * @throws MalformedQueryException
	 * @throws SailException
	 * @throws QueryEvaluationException
	 * @throws TableNotFoundException
	 * @throws AccumuloSecurityException
	 * @throws AccumuloException
	 */
	public AccumuloIndexSet(final Configuration conf, final String tablename)
			throws MalformedQueryException, SailException,
			QueryEvaluationException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
		this.accCon = ConfigUtils.getConnector(conf);
		this.auths = getAuthorizations(conf);
		PcjMetadata meta = null;
		try {
			meta = pcj.getPcjMetadata(accCon, tablename);
		} catch (final PcjException e) {
			e.printStackTrace();
		}

		this.tablename = tablename;
		final SPARQLParser sp = new SPARQLParser();
		final ParsedTupleQuery pq = (ParsedTupleQuery) sp.parseQuery(meta.getSparql(), null);

		setProjectionExpr((Projection) pq.getTupleExpr());
		final Set<VariableOrder> orders = meta.getVarOrders();

		varOrder = Lists.newArrayList();
		for (final VariableOrder var : orders) {
			varOrder.add(var.toString());
		}
		setLocalityGroups(tablename, accCon, varOrder);
		this.setSupportedVariableOrderMap(varOrder);
	}


	private Authorizations getAuthorizations(final Configuration conf) {
		final String authString = conf.get(RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH, "");
        if (authString.isEmpty()) {
            return new Authorizations();
        }
        return new Authorizations(authString.split(","));
	}

	/**
	 * returns size of table for query planning
	 */
	@Override
	public double cardinality() {
		double cardinality = 0;
		try {
			cardinality = pcj.getPcjMetadata(accCon, tablename)
					.getCardinality();
		} catch (final PcjException e) {
			e.printStackTrace();
		}
		return cardinality;
	}

	/**
	 *
	 * @param tableName
	 * @param conn
	 * @param groups
	 *            - locality groups to be created
	 *
	 *            Sets locality groups for more efficient scans - these are
	 *            usually the variable orders in the table so that scans for
	 *            specific orders are more efficient
	 */
	private void setLocalityGroups(final String tableName, final Connector conn,
			final List<String> groups) {

		final HashMap<String, Set<Text>> localityGroups = new HashMap<String, Set<Text>>();
		for (int i = 0; i < groups.size(); i++) {
			final HashSet<Text> tempColumn = new HashSet<Text>();
			tempColumn.add(new Text(groups.get(i)));
			final String groupName = groups.get(i).replace(VALUE_DELIM, "");
			localityGroups.put(groupName, tempColumn);
		}

		try {
			conn.tableOperations().setLocalityGroups(tableName, localityGroups);
		} catch (AccumuloException | AccumuloSecurityException
				| TableNotFoundException e) {
			e.printStackTrace();
		}

	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			final BindingSet bindingset) throws QueryEvaluationException {
		return this.evaluate(Collections.singleton(bindingset));
	}

	/**
	 * Core evaluation method used during query evaluation - given a collection
	 * of binding set constraints, this method finds common binding labels
	 * between the constraints and table, uses those to build a prefix scan of
	 * the Accumulo table, and creates a solution binding set by iterating of
	 * the scan results.
	 * @param bindingset - collection of {@link BindingSet}s to be joined with PCJ
	 * @return - CloseableIteration over joined results
	 */
	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(
			final Collection<BindingSet> bindingset)
			throws QueryEvaluationException {

		if (bindingset.isEmpty()) {
			return new IteratorWrapper<BindingSet, QueryEvaluationException>(
					new HashSet<BindingSet>().iterator());
		}

		final List<BindingSet> crossProductBs = new ArrayList<>();
		final Map<String, org.openrdf.model.Value> constantConstraints = new HashMap<>();
		final Set<Range> hashJoinRanges = new HashSet<>();
		final Range EMPTY_RANGE = new Range("", true, "~", false);
		Range crossProductRange = EMPTY_RANGE;
		String localityGroupOrder = varOrder.get(0);
		int maxPrefixLen = Integer.MIN_VALUE;
		int prefixLen = 0;
		int oldPrefixLen = 0;
		final Multimap<String, BindingSet> bindingSetHashMap = HashMultimap.create();
		HashJoinType joinType = HashJoinType.CONSTANT_JOIN_VAR;
		final Set<String> unAssuredVariables = Sets.difference(getTupleExpr().getBindingNames(), getTupleExpr().getAssuredBindingNames());
		boolean useColumnScan = false;
		boolean isCrossProd = false;
		boolean containsConstantConstraints = false;
		final BindingSet constants = getConstantConstraints();
		containsConstantConstraints = constants.size() > 0;

		try {
			for (final BindingSet bs : bindingset) {
				if (bindingset.size() == 1 && bs.size() == 0) {
					// in this case, only single, empty bindingset, pcj node is
					// first node in query plan - use full Range scan with
					// column
					// family set
					useColumnScan = true;
				}
				// get common vars for PCJ - only use variables associated
				// with assured Bindings
				final QueryBindingSet commonVars = new QueryBindingSet();
				for (final String b : getTupleExpr().getAssuredBindingNames()) {
					final Binding v = bs.getBinding(b);
					if (v != null) {
						commonVars.addBinding(v);
					}
				}
				// no common vars implies cross product
				if (commonVars.size() == 0 && bs.size() != 0) {
					crossProductBs.add(bs);
					isCrossProd = true;
				}
				//get a varOrder from orders in PCJ table - use at least
				//one common variable
				final BindingSetVariableOrder varOrder = getVarOrder(
						commonVars.getBindingNames(),
						constants.getBindingNames());

				// update constant constraints not used in varOrder and
				// update Bindings used to form range by removing unused
				// variables
				commonVars.addAll(constants);
				if (commonVars.size() > varOrder.varOrderLen) {
					final Map<String, Value> valMap = getConstantValueMap();
					for (final String s : new HashSet<String>(varOrder.unusedVars)) {
						if (valMap.containsKey(s)
								&& !constantConstraints.containsKey(s)) {
							constantConstraints.put(s, valMap.get(s));
						}
						commonVars.removeBinding(s);
					}
				}

				if (containsConstantConstraints
						&& (useColumnScan || isCrossProd)) {
					// only one range required in event of a cross product or
					// empty BindingSet
					// Range will either be full table Range or determined by
					// constant constraints
					if (crossProductRange == EMPTY_RANGE) {
						crossProductRange = getRange(varOrder.varOrder,
								commonVars);
						localityGroupOrder = prefixToOrder(varOrder.varOrder);
					}
				} else if (!useColumnScan && !isCrossProd) {
					// update ranges and add BindingSet to HashJoinMap if not a
					// cross product
					hashJoinRanges.add(getRange(varOrder.varOrder, commonVars));

					prefixLen = varOrder.varOrderLen;
					// check if common Variable Orders are changing between
					// BindingSets (happens in case
					// of Optional). If common variable set length changes from
					// BindingSet to BindingSet
					// update the HashJoinType to be VARIABLE_JOIN_VAR.
					if (oldPrefixLen == 0) {
						oldPrefixLen = prefixLen;
					} else {
						if (oldPrefixLen != prefixLen
								&& joinType == HashJoinType.CONSTANT_JOIN_VAR) {
							joinType = HashJoinType.VARIABLE_JOIN_VAR;
						}
						oldPrefixLen = prefixLen;
					}
					// update max prefix len
					if (prefixLen > maxPrefixLen) {
						maxPrefixLen = prefixLen;
					}
					final String key = getHashJoinKey(varOrder.varOrder, commonVars);
					bindingSetHashMap.put(key, bs);
				}

				isCrossProd = false;
			}

			// create full Range scan iterator and set column family if empty
			// collection or if cross product BindingSet exists and no hash join
			// BindingSets
			if ((useColumnScan || crossProductBs.size() > 0)
					&& bindingSetHashMap.size() == 0) {
				final Scanner scanner = accCon.createScanner(tablename, auths);
				// cross product with no cross product constraints here
				scanner.setRange(crossProductRange);
				scanner.fetchColumnFamily(new Text(localityGroupOrder));
				return new PCJKeyToCrossProductBindingSetIterator(scanner,
						crossProductBs, constantConstraints, unAssuredVariables, getTableVarMap());
			} else if ((useColumnScan || crossProductBs.size() > 0)
					&& bindingSetHashMap.size() > 0) {

				// in this case, both hash join BindingSets and cross product
				// BindingSets exist
				// create an iterator to evaluate cross product and an iterator
				// for hash join, then combine

				final List<CloseableIteration<BindingSet, QueryEvaluationException>> iteratorList = new ArrayList<>();

				// create cross product iterator
				final Scanner scanner1 = accCon.createScanner(tablename, auths);
				scanner1.setRange(crossProductRange);
				scanner1.fetchColumnFamily(new Text(localityGroupOrder));
				iteratorList.add(new PCJKeyToCrossProductBindingSetIterator(
						scanner1, crossProductBs, constantConstraints, unAssuredVariables,
						getTableVarMap()));

				// create hash join iterator
				final BatchScanner scanner2 = accCon.createBatchScanner(tablename, auths, 10);
				scanner2.setRanges(hashJoinRanges);
				final PCJKeyToJoinBindingSetIterator iterator = new PCJKeyToJoinBindingSetIterator(
						scanner2, getTableVarMap(), maxPrefixLen);
				iteratorList.add(new BindingSetHashJoinIterator(
						bindingSetHashMap, iterator, unAssuredVariables, joinType));

				// combine iterators
				return new IteratorCombiner(iteratorList);

			} else {
				// only hash join BindingSets exist
				final BatchScanner scanner = accCon.createBatchScanner(tablename, auths, 10);
				// only need to create hash join iterator
				scanner.setRanges(hashJoinRanges);
				final PCJKeyToJoinBindingSetIterator iterator = new PCJKeyToJoinBindingSetIterator(
						scanner, getTableVarMap(), maxPrefixLen);
				return new BindingSetHashJoinIterator(bindingSetHashMap,
						iterator, unAssuredVariables, joinType);
			}
		} catch (final Exception e) {
			throw new QueryEvaluationException(e);
		}
	}

	private String getHashJoinKey(final String commonVarOrder, final BindingSet bs) {
		final String[] commonVarArray = commonVarOrder.split(VAR_ORDER_DELIM);
		String key = bs.getValue(commonVarArray[0]).toString();
		for (int i = 1; i < commonVarArray.length; i++) {
			key = key + VALUE_DELIM + bs.getValue(commonVarArray[i]).toString();
		}
		return key;
	}

	private Range getRange(final String commonVarOrder, final BindingSet bs)
			throws BindingSetConversionException {
		final AccumuloPcjSerializer converter = new AccumuloPcjSerializer();
		byte[] rangePrefix = new byte[0];
		rangePrefix = converter.convert(bs, new VariableOrder(commonVarOrder));
		return Range.prefix(new Text(rangePrefix));
	}

	/**
	 *
	 * @param variableBindingNames
	 *            - names corresponding to variables
	 * @param constantBindingNames
	 *            - names corresponding to constant constraints
	 * @return - {@link BindingSetVariableOrder} object containing largest
	 *         possible supported variable order built from variableBindingNames
	 *         and constantBindingNames
	 */
	private BindingSetVariableOrder getVarOrder(
			final Set<String> variableBindingNames, final Set<String> constantBindingNames) {
		final Map<String, Set<String>> varOrderMap = this
				.getSupportedVariableOrders();
		final Set<Map.Entry<String, Set<String>>> entries = varOrderMap.entrySet();

		Set<String> variables;
		if (variableBindingNames.size() == 0
				&& constantBindingNames.size() == 0) {
			return new BindingSetVariableOrder("", 0, new HashSet<String>());
		} else if (variableBindingNames.size() > 0
				&& constantBindingNames.size() == 0) {
			variables = variableBindingNames;
		} else if (variableBindingNames.size() == 0
				&& constantBindingNames.size() > 0) {
			variables = constantBindingNames;
		} else {
			variables = Sets.union(variableBindingNames, constantBindingNames);

			String maxPrefix = null;
			int maxPrefixLen = 0;
			Set<String> minUnusedVariables = null;

			for (final Map.Entry<String, Set<String>> e : entries) {
				final Set<String> value = e.getValue();
				if (maxPrefixLen < value.size()
						&& variables.containsAll(value)
						&& Sets.intersection(value, variableBindingNames)
								.size() > 0) {
					maxPrefixLen = value.size();
					maxPrefix = e.getKey();
					minUnusedVariables = Sets.difference(variables, value);
					if (maxPrefixLen == variables.size()) {
						break;
					}
				}
			}
			return new BindingSetVariableOrder(maxPrefix, maxPrefixLen,
					minUnusedVariables);
		}

		String maxPrefix = null;
		int maxPrefixLen = 0;
		Set<String> minUnusedVariables = null;

		for (final Map.Entry<String, Set<String>> e : entries) {
			final Set<String> value = e.getValue();
			if (maxPrefixLen < value.size() && variables.containsAll(value)) {
				maxPrefixLen = value.size();
				maxPrefix = e.getKey();
				minUnusedVariables = Sets.difference(variables, value);
				if (maxPrefixLen == variables.size()) {
					break;
				}
			}
		}
		return new BindingSetVariableOrder(maxPrefix, maxPrefixLen,
				minUnusedVariables);
	}

	/**
	 * @return - all constraints which correspond to variables in
	 *         {@link AccumuloIndexSet#getTupleExpr()} which are set equal to a
	 *         constant, but are non-constant in Accumulo table
	 */
	private BindingSet getConstantConstraints() {
		final Map<String, String> tableMap = this.getTableVarMap();
		final Set<String> keys = tableMap.keySet();

		final QueryBindingSet constants = new QueryBindingSet();
		for (final String s : keys) {
			if (s.startsWith("-const-")) {
				constants.addBinding(new BindingImpl(s, getConstantValueMap()
						.get(s)));
			}
		}
		return constants;
	}

	/**
    *
    * @param order - prefix of a full variable order
    * @return - full variable order that includes all variables whose values
    * are stored in the table - used to obtain the locality group
    */
   //given partial order of query vars, convert to PCJ vars and determine
   //if converted partial order is a substring of a full var order of PCJ variables.
   //if converted partial order is a prefix, convert corresponding full PCJ var order to query vars
   private String prefixToOrder(String order) {
       final Map<String, String> invMap = HashBiMap.create(this.getTableVarMap()).inverse();
       final String[] temp = order.split(VAR_ORDER_DELIM);
       //get order in terms of PCJ variables
       for (int i = 0; i < temp.length; i++) {
           temp[i] = this.getTableVarMap().get(temp[i]);
       }
       order = Joiner.on(VAR_ORDER_DELIM).join(temp);
       for (final String s : varOrder) {
       	//verify that partial order is prefix of a PCJ varOrder
           if (s.startsWith(order)) {
               return s;
           }
       }
       throw new NoSuchElementException("Order is not a prefix of any locality group value!");
   }


	private class BindingSetVariableOrder {

		Set<String> unusedVars;
		int varOrderLen = 0;
		String varOrder;

		public BindingSetVariableOrder(final String varOrder, final int len,
				final Set<String> unused) {
			this.varOrder = varOrder;
			this.varOrderLen = len;
			this.unusedVars = unused;
		}

	}

}
