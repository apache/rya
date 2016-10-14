package org.apache.rya.indexing.pcj.matching;

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.rya.rdftriplestore.inference.DoNotExpandSP;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.QueryModelNodeBase;
import org.openrdf.query.algebra.QueryModelVisitor;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.Var;

import com.google.common.collect.Sets;

/**
 * This class is essentially a wrapper for {@link LeftJoin}. It provides a
 * flattened view of a LeftJoin that is useful for matching {@AccumuloIndexSet
 * } nodes to sub-queries to use for Precomputed Joins.
 * Because LeftJoins cannot automatically be interchanged with {@link Join}s and
 * other LeftJoins in the query plan, this class has utility methods to check
 * when nodes can be interchanged in the query plan. These methods track which
 * variables returned by {@link LeftJoin#getRightArg()} are bound. A variable is
 * bound if it also contained in the set returned by
 * {@link LeftJoin#getLeftArg()}. Nodes can be interchanged with a LeftJoin (and
 * hence a FlattenedOptional) so long as the bound and unbound variables do not
 * change.
 *
 */
public class FlattenedOptional extends QueryModelNodeBase implements TupleExpr {

	private Set<TupleExpr> rightArgs;
	private Set<String> boundVars;
	private Set<String> unboundVars;
	private Map<String, Integer> leftArgVarCounts = new HashMap<String, Integer>();
	private ValueExpr condition;
	private TupleExpr rightArg;
	private Set<String> bindingNames;
	private Set<String> assuredBindingNames;

	public FlattenedOptional(LeftJoin node) {
		rightArgs = getJoinArgs(node.getRightArg(), new HashSet<TupleExpr>());
		boundVars = setWithOutConstants(Sets
				.intersection(node.getLeftArg().getAssuredBindingNames(), node
						.getRightArg().getBindingNames()));
		unboundVars = setWithOutConstants(Sets.difference(node.getRightArg()
				.getBindingNames(), boundVars));
		condition = node.getCondition();
		rightArg = node.getRightArg();
		getVarCounts(node);
		assuredBindingNames = new HashSet<>(leftArgVarCounts.keySet());
		bindingNames = new HashSet<>(Sets.union(assuredBindingNames,
				unboundVars));
	}

	public FlattenedOptional(FlattenedOptional optional) {
		this.rightArgs = optional.rightArgs;
		this.boundVars = optional.boundVars;
		this.unboundVars = optional.unboundVars;
		this.condition = optional.condition;
		this.rightArg = optional.rightArg;
		this.leftArgVarCounts = optional.leftArgVarCounts;
		this.bindingNames = optional.bindingNames;
		this.assuredBindingNames = optional.assuredBindingNames;
	}

	public Set<TupleExpr> getRightArgs() {
		return rightArgs;
	}

	public TupleExpr getRightArg() {
		return rightArg;
	}

	/**
	 *
	 * @param te
	 *            - TupleExpr to be added to leftarg of {@link LeftJoin}
	 */
	public void addArg(TupleExpr te) {
		if (te instanceof FlattenedOptional) {
			return;
		}
		incrementVarCounts(te.getBindingNames());
	}

	public void removeArg(TupleExpr te) {
		if (te instanceof FlattenedOptional) {
			return;
		}
		decrementVarCounts(te.getBindingNames());
	}

	/**
	 *
	 * @param te
	 *            - {@link TupleExpr} to be added to leftArg of LeftJoin
	 * @return - true if adding TupleExpr does not affect unbound variables and
	 *         returns false otherwise
	 */
	public boolean canAddTuple(TupleExpr te) {
		// can only add LeftJoin if rightArg varNames do not intersect
		// unbound vars
		if (te instanceof FlattenedOptional) {
			FlattenedOptional lj = (FlattenedOptional) te;
			if (Sets.intersection(lj.rightArg.getBindingNames(), unboundVars)
					.size() > 0) {
				return false;
			} else {
				return true;
			}
		}

		return Sets.intersection(te.getBindingNames(), unboundVars).size() == 0;
	}

	/**
	 *
	 * @param te
	 *            - {@link TupleExpr} to be removed from leftArg of LeftJoin
	 * @return - true if removing TupleExpr does not affect bound variables and
	 *         returns false otherwise
	 */
	public boolean canRemoveTuple(TupleExpr te) {
		return canRemove(te);
	}

	@Override
	public Set<String> getBindingNames() {
		return bindingNames;
	}

	@Override
	public Set<String> getAssuredBindingNames() {
		return assuredBindingNames;
	}

	public ValueExpr getCondition() {
		return condition;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof FlattenedOptional) {
			FlattenedOptional ljDec = (FlattenedOptional) other;
			ValueExpr oCond = ljDec.getCondition();
			return nullEquals(condition, oCond)
					&& ljDec.getRightArgs().equals(rightArgs);
		}
		return false;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = prime + (rightArgs == null ? 0 : rightArgs.hashCode());
		result = prime * result
				+ (condition == null ? 0 : condition.hashCode());
		return result;
	}

	/**
	 * This method is used to retrieve a set view of all descendants of the
	 * rightArg of the LeftJoin (the optional part)
	 *
	 * @param tupleExpr
	 *            - tupleExpr whose args are being retrieved
	 * @param joinArgs
	 *            - set view of all non-join args that are descendants of
	 *            tupleExpr
	 * @return joinArgs
	 */
	private Set<TupleExpr> getJoinArgs(TupleExpr tupleExpr,
			Set<TupleExpr> joinArgs) {
		if (tupleExpr instanceof Join) {
			if (!(((Join) tupleExpr).getLeftArg() instanceof FixedStatementPattern)
					&& !(((Join) tupleExpr).getRightArg() instanceof DoNotExpandSP)) {
				Join join = (Join) tupleExpr;
				getJoinArgs(join.getLeftArg(), joinArgs);
				getJoinArgs(join.getRightArg(), joinArgs);
			}
		} else if (tupleExpr instanceof LeftJoin) { // TODO probably not
													// necessary if not
													// including leftarg
			LeftJoin lj = (LeftJoin) tupleExpr;
			joinArgs.add(new FlattenedOptional(lj));
			getJoinArgs(lj.getLeftArg(), joinArgs);
		} else if (tupleExpr instanceof Filter) {
			getJoinArgs(((Filter) tupleExpr).getArg(), joinArgs);
		} else {
			joinArgs.add(tupleExpr);
		}

		return joinArgs;
	}

	/**
	 * This method counts the number of times each variable appears in the
	 * leftArg of the LeftJoin defining this FlattenedOptional. This information
	 * is used to whether nodes can be moved out of the leftarg above the
	 * LeftJoin in the query.
	 *
	 * @param tupleExpr
	 */
	private void getVarCounts(TupleExpr tupleExpr) {
		if (tupleExpr instanceof Join) {
			Join join = (Join) tupleExpr;
			getVarCounts(join.getLeftArg());
			getVarCounts(join.getRightArg());
		} else if (tupleExpr instanceof LeftJoin) {
			LeftJoin lj = (LeftJoin) tupleExpr;
			getVarCounts(lj.getLeftArg());
		} else if (tupleExpr instanceof Filter) {
			getVarCounts(((Filter) tupleExpr).getArg());
		} else {
			incrementVarCounts(tupleExpr.getBindingNames());
		}
	}

	/**
	 *
	 * @param te
	 *            - {@link TupleExpr} to be removed from leftArg of LeftJoin
	 * @return - true if removing te doesn't affect bounded variables of
	 *         LeftJoin and false otherwise
	 */
	private boolean canRemove(TupleExpr te) {
		// can only remove LeftJoin if right varNames do not intersect
		// unbound vars
		if (te instanceof FlattenedOptional) {
			FlattenedOptional lj = (FlattenedOptional) te;
			if (Sets.intersection(lj.getRightArg().getBindingNames(),
					unboundVars).size() > 0) {
				return false;
			} else {
				return true;
			}
		}
		Set<String> vars = te.getBindingNames();
		Set<String> intersection = Sets.intersection(vars, boundVars);
		if (intersection.size() == 0) {
			return true;
		}
		for (String s : intersection) {
			if (leftArgVarCounts.containsKey(s) && leftArgVarCounts.get(s) == 1) {
				return false;
			}
		}
		return true;
	}

	private void incrementVarCounts(Set<String> vars) {
		for (String s : vars) {
			if (!s.startsWith("-const-") && leftArgVarCounts.containsKey(s)) {
				leftArgVarCounts.put(s, leftArgVarCounts.get(s) + 1);
			} else if (!s.startsWith("-const-")) {
				leftArgVarCounts.put(s, 1);
			}
		}
	}

	private void decrementVarCounts(Set<String> vars) {
		for (String s : vars) {
			if (leftArgVarCounts.containsKey(s) && leftArgVarCounts.get(s) > 1) {
				leftArgVarCounts.put(s, leftArgVarCounts.get(s) - 1);
			} else {
				leftArgVarCounts.remove(s);
				bindingNames.remove(s);
				assuredBindingNames.remove(s);
			}
		}
	}

	/**
	 *
	 * @param vars
	 *            - set of {@link Var} names, possibly contained constants
	 */
	private Set<String> setWithOutConstants(Set<String> vars) {
		Set<String> copy = new HashSet<>();
		for (String s : vars) {
			if (!s.startsWith("-const-")) {
				copy.add(s);
			}
		}

		return copy;
	}

	@Override
	public <X extends Exception> void visit(QueryModelVisitor<X> visitor)
			throws X {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return "FlattenedOptional: " + rightArgs;
	}

	@Override
	public FlattenedOptional clone() {
		return new FlattenedOptional(this);
	}

}
