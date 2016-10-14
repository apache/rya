package org.apache.rya.indexing.external.tupleSet;

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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.impl.ExternalSet;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This is an abstract class of delegating the evaluation of part
 * of a SPARQL query to an external source.  The {@link TupleExpr} returned by {@link ExternalTupleSet#getTupleExpr()}
 * represents the SPARQL string that this node evaluates, and table returned by {@link ExternalTupleSet#getTableVarMap()}
 * maps the variables of TupleExpr to the variables stored in the external store (which may be different).  The map
 * returned by {@link ExternalTupleSet#getSupportedVariableOrderMap()} provides a map of all the variable orders in which
 * data is written to the supporting, and is useful for determining which {@link BindingSet} can be passed into
 * {@link ExternalTupleSet#evaluate(BindingSet)}.
 *
 */
public abstract class ExternalTupleSet extends ExternalSet {

	public static final String VAR_ORDER_DELIM = ";";
	public static final String CONST_PREFIX = "-const-";
	public static final String VALUE_DELIM = "\u0000";
	private Projection tupleExpr;
    private Map<String, String> tableVarMap = Maps.newHashMap();  //maps vars in tupleExpr to var in stored binding sets
    private Map<String, Set<String>> supportedVarOrders = Maps.newHashMap(); //indicates supported var orders
    private Map<String, org.openrdf.model.Value> valMap;

    public ExternalTupleSet() {
    }

    public ExternalTupleSet(Projection tupleExpr) {
    	Preconditions.checkNotNull(tupleExpr);
        this.tupleExpr = tupleExpr;
        valMap = getValMap();
        updateTableVarMap(tupleExpr, tupleExpr);
    }

    @Override
    abstract public CloseableIteration<BindingSet, QueryEvaluationException> evaluate(BindingSet bindings) throws QueryEvaluationException;

    @Override
    public Set<String> getBindingNames() {
        return tupleExpr.getBindingNames();
    }

    @Override
    public Set<String> getAssuredBindingNames() {
        return tupleExpr.getAssuredBindingNames();
    }

    @Override
    public String getSignature() {
        return "(External Projection) " + Joiner.on(", ").join(tupleExpr.getProjectionElemList().getElements()).replaceAll("\\s+", " ");
    }

    public Projection getTupleExpr() {
        return tupleExpr;
    }

    public void setProjectionExpr(Projection tupleExpr) {
    	Preconditions.checkNotNull(tupleExpr);
    	if(this.tupleExpr == null) {
    		updateTableVarMap(tupleExpr, tupleExpr);
    	} else {
    		updateTableVarMap(tupleExpr, this.tupleExpr);
    	}
        this.tupleExpr = tupleExpr;
        valMap = getValMap();
		if (supportedVarOrders.size() != 0) {
			updateSupportedVarOrderMap();
		}
    }

    public void setTableVarMap(Map<String,String> vars) {
    	Preconditions.checkNotNull(vars);
        this.tableVarMap = vars;
    }

    public Map<String, String> getTableVarMap() {
        return this.tableVarMap;
    }

    public void setSupportedVariableOrderMap(Map<String, Set<String>> varOrders) {
    	Preconditions.checkNotNull(varOrders);
        this.supportedVarOrders = varOrders;
    }

    public void setSupportedVariableOrderMap(List<String> varOrders) {
    	Preconditions.checkNotNull(varOrders);
    	this.supportedVarOrders = createSupportedVarOrderMap(varOrders);
    }

    public Map<String, Set<String>> getSupportedVariableOrderMap() {
        return supportedVarOrders;
    }

    public Map<String, org.openrdf.model.Value> getConstantValueMap() {
    	return valMap;
    }

    @Override
    public ExternalSet clone() {
        final ExternalTupleSet clone = (ExternalTupleSet) super.clone();
        clone.setProjectionExpr(this.tupleExpr.clone());
        clone.tableVarMap = Maps.newHashMap();
        for(final String s: this.tableVarMap.keySet()) {
            clone.tableVarMap.put(s,this.tableVarMap.get(s));
        }
        clone.supportedVarOrders = Maps.newHashMap();
        for(final String s: this.supportedVarOrders.keySet()) {
            clone.supportedVarOrders.put(s,this.supportedVarOrders.get(s));
        }
        return clone;
    }

	public Map<String, Set<String>> getSupportedVariableOrders() {
		return supportedVarOrders;
	}

	public boolean supportsBindingSet(Set<String> bindingNames) {
		final Collection<Set<String>> values = supportedVarOrders.values();
		final Set<String> bNames = Sets.newHashSet();
		final Set<String> bNamesWithConstants = Sets.newHashSet();

		for (final String s : this.getTupleExpr().getBindingNames()) {
			if (bindingNames.contains(s)) {
				bNames.add(s);
				bNamesWithConstants.add(s);
			} else if(s.startsWith(CONST_PREFIX)) {
				bNamesWithConstants.add(s);
			}
		}
		return values.contains(bNames) || values.contains(bNamesWithConstants);
	}

	/**
	 * @param tupleMatch
	 *            - project expression - call after setting {@link tupleExpr} to
	 *            map new variables to old -- the variables in the binding list
	 *            of the new tupleExpr (tupleMatch) must map to the
	 *            corresponding variables in the binding list of the old
	 *            tupleExpr
	 */
	private void updateTableVarMap(TupleExpr newTuple, TupleExpr oldTuple) {

		final List<String> replacementVars = Lists.newArrayList(newTuple
				.getBindingNames());
		final List<String> tableVars = Lists.newArrayList(oldTuple
				.getBindingNames());

		final Map<String, String> tableMap = Maps.newHashMap();

		for (int i = 0; i < tableVars.size(); i++) {
			tableMap.put(replacementVars.get(i), tableVars.get(i));
		}
		this.setTableVarMap(tableMap);
	}

	/**
	 * call after setting {@link tableVarMap} to update map of supported
	 * variables in terms of variables in new tupleExpr
	 */
	private void updateSupportedVarOrderMap() {

		Preconditions.checkArgument(supportedVarOrders.size() != 0);;
		final Map<String, Set<String>> newSupportedOrders = Maps.newHashMap();
		final BiMap<String, String> biMap = HashBiMap.create(tableVarMap)
				.inverse();
		Set<String> temp = null;
		final Set<String> keys = supportedVarOrders.keySet();

		for (final String s : keys) {
			temp = supportedVarOrders.get(s);
			final Set<String> newSet = Sets.newHashSet();

			for (final String t : temp) {
				newSet.add(biMap.get(t));
			}

			final String[] tempStrings = s.split(VAR_ORDER_DELIM);
			String v = "";
			for (final String u : tempStrings) {
				if (v.length() == 0) {
					v = v + biMap.get(u);
				} else {
					v = v + VAR_ORDER_DELIM + biMap.get(u);
				}
			}
			newSupportedOrders.put(v, newSet);
		}
		supportedVarOrders = newSupportedOrders;
	}

	 /**
    *
    * @param orders
    * @return - map with all possible orders in which results are written to the table
    */
   private Map<String, Set<String>> createSupportedVarOrderMap(List<String> orders) {
	   final Map<String, Set<String>> supportedVars = Maps.newHashMap();

       for (final String t : orders) {
           final String[] tempOrder = t.split(VAR_ORDER_DELIM);
           final Set<String> varSet = Sets.newHashSet();
           String u = "";
           for (final String s : tempOrder) {
               if(u.length() == 0) {
                   u = s;
               } else{
                   u = u+ VAR_ORDER_DELIM + s;
               }
               varSet.add(s);
               supportedVars.put(u, new HashSet<String>(varSet));
           }
       }
       return supportedVars;
   }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ExternalTupleSet)) {
            return false;
        } else {
            final ExternalTupleSet arg = (ExternalTupleSet) other;
            if (this.getTupleExpr().equals(arg.getTupleExpr())) {
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31*result + tupleExpr.hashCode();
        return result;
    }

    private Map<String, org.openrdf.model.Value> getValMap() {
		ValueMapVisitor valMapVis = new ValueMapVisitor();
		tupleExpr.visit(valMapVis);
		return valMapVis.getValMap();
	}


	/**
	 *
	 * Extracts the values associated with constant labels in the query Used to
	 * create binding sets from range scan
	 */
	private class ValueMapVisitor extends
			QueryModelVisitorBase<RuntimeException> {
		Map<String, org.openrdf.model.Value> valMap = Maps.newHashMap();

		public Map<String, org.openrdf.model.Value> getValMap() {
			return valMap;
		}

		@Override
		public void meet(Var node) {
			if (node.getName().startsWith("-const-")) {
				valMap.put(node.getName(), node.getValue());
			}
		}
	}


}
