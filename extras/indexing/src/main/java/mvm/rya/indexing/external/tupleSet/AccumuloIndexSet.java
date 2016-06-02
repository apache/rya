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
package mvm.rya.indexing.external.tupleSet;

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
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.io.Text;
import org.apache.rya.indexing.pcj.storage.PcjException;
import org.apache.rya.indexing.pcj.storage.PcjMetadata;
import org.apache.rya.indexing.pcj.storage.accumulo.PcjTables;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.sail.SailException;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import info.aduna.iteration.CloseableIteration;
import mvm.rya.accumulo.precompQuery.AccumuloPcjQuery;
import mvm.rya.api.utils.IteratorWrapper;
import mvm.rya.indexing.PcjQuery;
import mvm.rya.rdftriplestore.evaluation.ExternalBatchingIterator;

/**
 * During query planning, this node is inserted into the parsed query to
 * represent part of the original query (a sub-query). This sub-query is the
 * value returned by {@link ExternalTupleSet#getTupleExpr()}. The results
 * associated with this sub-query are stored in an external Accumulo table,
 * where accCon and tablename are the associated {@link Connector} and table
 * name. During evaluation, the portion of the query in
 * {@link AccumuloIndexSet} is evaluated by scanning the external Accumulo
 * table. This class is extremely useful for caching queries and reusing results
 * from previous SPARQL queries.
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
public class AccumuloIndexSet extends ExternalTupleSet implements ExternalBatchingIterator {

    private final Connector accCon;  //connector to Accumulo table where results are stored
    private final String tablename;  //name of Accumulo table
    private List<String> varOrder = null; // orders in which results are written to table
    private final PcjTables pcj = new PcjTables();

    @Override
    public Map<String, Set<String>> getSupportedVariableOrders() {
        return this.getSupportedVariableOrderMap();
    }

    @Override
    public String getSignature() {
        return "AccumuloIndexSet(" + tablename + ") : " + Joiner.on(", ").join(this.getTupleExpr().getBindingNames());
    }

    /**
     *
     * @param sparql - name of sparql query whose results will be stored in PCJ table
     * @param accCon - connection to a valid Accumulo instance
     * @param tablename - name of an existing PCJ table
     * @throws MalformedQueryException
     * @throws SailException
     * @throws QueryEvaluationException
     * @throws MutationsRejectedException
     * @throws TableNotFoundException
     */
    public AccumuloIndexSet(final String sparql, final Connector accCon, final String tablename) throws MalformedQueryException, SailException, QueryEvaluationException,
            MutationsRejectedException, TableNotFoundException {
        this.tablename = tablename;
        this.accCon = accCon;
        final SPARQLParser sp = new SPARQLParser();
        final ParsedTupleQuery pq = (ParsedTupleQuery) sp.parseQuery(sparql, null);

        final Optional<Projection> projection = new ParsedQueryUtil().findProjection(pq);
        if(!projection.isPresent()) {
            throw new MalformedQueryException("SPARQL query '" + sparql + "' does not contain a Projection.");
        }
        setProjectionExpr(projection.get());

        Set<VariableOrder> orders = null;
        try {
			orders = pcj.getPcjMetadata(accCon, tablename).getVarOrders();
		} catch (final PcjException e) {
			e.printStackTrace();
		}

        varOrder = Lists.newArrayList();
        for(final VariableOrder var: orders) {
            varOrder.add(var.toString());
        }
        setLocalityGroups(tablename, accCon, varOrder);
        this.setSupportedVariableOrderMap(varOrder);
    }

    /**
     *
     * @param accCon - connection to a valid Accumulo instance
     * @param tablename - name of an existing PCJ table
     * @throws MalformedQueryException
     * @throws SailException
     * @throws QueryEvaluationException
     * @throws MutationsRejectedException
     * @throws TableNotFoundException
     */
	public AccumuloIndexSet(final Connector accCon, final String tablename)
			throws MalformedQueryException, SailException,
			QueryEvaluationException, MutationsRejectedException,
			TableNotFoundException {
		PcjMetadata meta = null;
		try {
			meta = pcj.getPcjMetadata(accCon, tablename);
		} catch (final PcjException e) {
			e.printStackTrace();
		}

		this.tablename = tablename;
		this.accCon = accCon;
		final SPARQLParser sp = new SPARQLParser();
		final ParsedTupleQuery pq = (ParsedTupleQuery) sp.parseQuery(meta.getSparql(),
				null);
		setProjectionExpr((Projection) pq.getTupleExpr());
		final Set<VariableOrder> orders = meta.getVarOrders();

		varOrder = Lists.newArrayList();
		for (final VariableOrder var : orders) {
			varOrder.add(var.toString());
		}
		setLocalityGroups(tablename, accCon, varOrder);
		this.setSupportedVariableOrderMap(varOrder);
	}

	/**
	 * returns size of table for query planning
	 */
    @Override
    public double cardinality() {
    	double cardinality = 0;
        try {
			cardinality = pcj.getPcjMetadata(accCon, tablename).getCardinality();
		} catch (final PcjException e) {
			e.printStackTrace();
		}
        return cardinality;
    }


    /**
     *
     * @param tableName
     * @param conn
     * @param groups  - locality groups to be created
     *
     * Sets locality groups for more efficient scans - these are usually the variable
     * orders in the table so that scans for specific orders are more efficient
     */
    private void setLocalityGroups(final String tableName, final Connector conn, final List<String> groups) {

		final HashMap<String, Set<Text>> localityGroups = new HashMap<String, Set<Text>>();
		for (int i = 0; i < groups.size(); i++) {
			final HashSet<Text> tempColumn = new HashSet<Text>();
			tempColumn.add(new Text(groups.get(i)));
			final String groupName = groups.get(i).replace(VAR_ORDER_DELIM, "");
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
    public CloseableIteration<BindingSet,QueryEvaluationException> evaluate(final BindingSet bindingset) throws QueryEvaluationException {
        return this.evaluate(Collections.singleton(bindingset));
    }

    /**
     * Core evaluation method used during query evaluation - given a collection of binding set constraints, this
     * method finds common binding labels between the constraints and table, uses those to build a prefix scan
     * of the Accumulo table, and creates a solution binding set by iterating of the scan results.
     */
    @Override
    public CloseableIteration<BindingSet,QueryEvaluationException> evaluate(final Collection<BindingSet> bindingset) throws QueryEvaluationException {
        String localityGroup = "";
        final Set<String> commonVars = Sets.newHashSet();
        // if bindingset is empty, there are no results, so return empty iterator
        if (bindingset.isEmpty()) {
        	return new IteratorWrapper<BindingSet, QueryEvaluationException>(new HashSet<BindingSet>().iterator());
        }
      //to build range prefix, find common vars of bindingset and PCJ bindings
        else {
        	final BindingSet bs = bindingset.iterator().next();
            for (final String b : this.getTupleExpr().getAssuredBindingNames()) {
                final Binding v = bs.getBinding(b);
                if (v != null) {
                    commonVars.add(b);
                }
            }
        }
        //add any constant constraints to common vars to be used in range prefix
        commonVars.addAll(getConstantConstraints());
        PcjQuery apq = null;
        List<String> fullVarOrder =  null;
        String commonVarOrder = null;
        try {
            if (commonVars.size() > 0) {
                commonVarOrder = getVarOrder(commonVars);
                if(commonVarOrder == null) {
                    throw new IllegalStateException("Index does not support binding set!");
                }
                fullVarOrder = Lists.newArrayList(prefixToOrder(commonVarOrder).split(VAR_ORDER_DELIM));
                //use varOrder and tableVarMap to set correct scan column
                localityGroup = orderToLocGroup(fullVarOrder);
            } else {
                localityGroup = varOrder.get(0);
            }
            apq = new AccumuloPcjQuery(accCon, tablename);
            final ValueMapVisitor vmv = new ValueMapVisitor();
            this.getTupleExpr().visit(vmv);

            List<String> commonVarOrderList = null;
            if(commonVarOrder != null) {
            	commonVarOrderList = Lists.newArrayList(commonVarOrder.split(VAR_ORDER_DELIM));
            } else {
            	commonVarOrderList = new ArrayList<>();
            }

            return apq.queryPrecompJoin(commonVarOrderList, localityGroup, vmv.getValMap(),
            		HashBiMap.create(this.getTableVarMap()).inverse(), bindingset);
        } catch(final TableNotFoundException e) {
            throw new QueryEvaluationException(e);
        }
    }

    /**
     *
     * @param order - variable order as indicated by query
     * @return - locality group or column family used in scan - this
     * is just the variable order expressed in terms of the variables stored
     * in the table
     */
    private String orderToLocGroup(final List<String> order) {
        String localityGroup = "";
        for (final String s : order) {
            if (localityGroup.length() == 0) {
                localityGroup = this.getTableVarMap().get(s);
            } else {
                localityGroup = localityGroup + VAR_ORDER_DELIM + this.getTableVarMap().get(s);
            }
        }
        return localityGroup;
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
        String[] temp = order.split(VAR_ORDER_DELIM);
        //get order in terms of PCJ variables
        for (int i = 0; i < temp.length; i++) {
            temp[i] = this.getTableVarMap().get(temp[i]);
        }
        order = Joiner.on(VAR_ORDER_DELIM).join(temp);
        for (final String s : varOrder) {
        	//verify that partial order is prefix of a PCJ varOrder
            if (s.startsWith(order)) {
                temp = s.split(VAR_ORDER_DELIM);
                //convert full PCJ varOrder back to query varOrder
                for (int i = 0; i < temp.length; i++) {
                    temp[i] = invMap.get(temp[i]);
                }
                return Joiner.on(VAR_ORDER_DELIM).join(temp);
            }
        }
        throw new NoSuchElementException("Order is not a prefix of any locality group value!");
    }

    /**
     *
     * @param variables
     * @return - string representation of the Set variables, in an order that is in the
     * table
     */
    private String getVarOrder(final Set<String> variables) {
        final Map<String, Set<String>> varOrderMap = this.getSupportedVariableOrders();
        final Set<Map.Entry<String, Set<String>>> entries = varOrderMap.entrySet();
        for (final Map.Entry<String, Set<String>> e : entries) {
            if (e.getValue().equals(variables)) {
                return e.getKey();
            }
        }
        return null;
    }

    /**
     * @return - all constraints which correspond to variables
     * in {@link AccumuloIndexSet#getTupleExpr()} which are set
     * equal to a constant, but are non-constant in Accumulo table
     */
    private Set<String> getConstantConstraints() {
        final Map<String, String> tableMap = this.getTableVarMap();
        final Set<String> keys = tableMap.keySet();
        final Set<String> constants = Sets.newHashSet();
        for (final String s : keys) {
            if (s.startsWith("-const-")) {
                constants.add(s);
            }
        }
        return constants;
    }

    /**
     *
     * Extracts the values associated with constant labels in the query
     * Used to create binding sets from range scan
     */
    public class ValueMapVisitor extends QueryModelVisitorBase<RuntimeException> {
        Map<String, org.openrdf.model.Value> valMap = Maps.newHashMap();
        public Map<String, org.openrdf.model.Value> getValMap() {
            return valMap;
        }
        @Override
        public void meet(final Var node) {
            if (node.getName().startsWith("-const-")) {
                valMap.put(node.getName(), node.getValue());
            }
        }
    }

}


