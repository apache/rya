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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.pcj.matching.QueryVariableNormalizer.VarCollector;

import org.openrdf.query.algebra.Difference;
import org.openrdf.query.algebra.EmptySet;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Intersection;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;
import org.openrdf.query.algebra.Union;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.VarNameCollector;

import com.google.common.collect.Sets;

public class PCJOptimizerUtilities {


	/**
	 * This method determines whether an index node is valid. Criteria for a
	 * valid node are that is have two or more {@link StatementPattern} nodes or
	 * at least one {@link Filter} and one StatementPattern node. Additionally,
	 * the number of variables in the Filter cannot exceed the number of
	 * variables among all non-Filter nodes in the TupleExpr. Also, this method
	 * calls the {@link ValidQueryVisitor} to determine if the
	 * TupleExpr contains an invalid node type.
	 *
	 * @param node - node to be checked for validity
	 * @return - true if valid and false otherwise
	 */
	public static boolean isPCJValid(TupleExpr node) {

		final ValidQueryVisitor vqv = new ValidQueryVisitor();
		node.visit(vqv);

		if (vqv.isValid() && (vqv.getJoinCount() > 0 ||  vqv.getFilters().size() > 0 &&  vqv.getSPs().size() > 0)){
			if (vqv.getFilters().size() > 0) {
				final Set<String> spVars = getVarNames(vqv.getSPs());
				final Set<String> fVarNames = getVarNames(vqv.getFilters());
				// check that all vars contained in filters also occur in SPs
				return spVars.containsAll(fVarNames);
			} else {
				return true;
			}
		} else {
			return false;
		}
	}


	/**
	 * This method determines whether an index node is valid. Criteria for a
	 * valid node are that is have two or more {@link StatementPattern} nodes or
	 * at least one {@link Filter} and one StatementPattern node. Additionally,
	 * the number of variables in the Filter cannot exceed the number of
	 * variables among all non-Filter nodes in the TupleExpr.
	 *
	 * @param node - PCJ {@link ExternalTupleSet} index node to be checked for validity
	 * @return - true if valid and false otherwise
	 */
	public static boolean isPCJValid(ExternalTupleSet node) {
		return isPCJValid(node.getTupleExpr());
	}

	public static List<ExternalTupleSet> getValidPCJs(
			List<ExternalTupleSet> pcjs) {

		Iterator<ExternalTupleSet> iterator = pcjs.iterator();
		while (iterator.hasNext()) {
			ExternalTupleSet pcj = iterator.next();
			if (!isPCJValid(pcj)) {
				iterator.remove();
			}
		}
		return pcjs;
	}


	public static Projection getProjection(TupleExpr te) {
		ProjectionVisitor visitor = new ProjectionVisitor();
		te.visit(visitor);
		return visitor.node;
	}

	static class ProjectionVisitor extends QueryModelVisitorBase<RuntimeException> {

		Projection node = null;

		@Override
		public void meet(Projection node) {
			this.node = node;
		}
	}


	/**
	 * @param filters
	 *            - filters to be pushed down into next {@link QuerySegment}, or
	 *            as far down as binding variable names permit.
	 */
	public static void relocateFilters(Set<Filter> filters) {
		for (Filter filter : filters) {
			FilterRelocator.relocate(filter);
		}
	}

	private static Set<String> getVarNames(Collection<QueryModelNode> nodes) {
		List<String> tempVars;
		final Set<String> nodeVarNames = Sets.newHashSet();

		for (final QueryModelNode s : nodes) {
			tempVars = VarCollector.process(s);
			for (final String t : tempVars) {
				nodeVarNames.add(t);
			}
		}
		return nodeVarNames;
	}

	/**
	 * A visitor which checks a TupleExpr associated with an ExternalTupleSet to
	 * determine whether the TupleExpr contains an invalid node.
	 *
	 */
	private static class ValidQueryVisitor extends
			QueryModelVisitorBase<RuntimeException> {

		private boolean isValid = true;
		private Set<QueryModelNode> filterSet = Sets.newHashSet();
		private Set<QueryModelNode> spSet = Sets.newHashSet();
		private int joinCount = 0;

		public Set<QueryModelNode> getFilters() {
			return filterSet;
		}

		public Set<QueryModelNode> getSPs() {
			return spSet;
		}

		public boolean isValid() {
			return isValid;
		}

		public int getJoinCount() {
			return joinCount;
		}

		@Override
		public void meet(Projection node) {
			node.getArg().visit(this);
		}

		@Override
		public void meet(Filter node) {
			filterSet.add(node.getCondition());
			node.getArg().visit(this);
		}

		@Override
		public void meet(StatementPattern node) {
			spSet.add(node);
		}

		@Override
		public void meet(Join node) {
			joinCount++;
			super.meet(node);
		}

		@Override
		public void meet(LeftJoin node) {
			joinCount++;
			super.meet(node);
		}

		@Override
		public void meetNode(QueryModelNode node) {
			if (!(node instanceof Join || node instanceof LeftJoin
					|| node instanceof StatementPattern || node instanceof Var
					|| node instanceof Union || node instanceof Filter || node instanceof Projection)) {
				isValid = false;
				return;
			}
			super.meetNode(node);
		}

	}

	/**
	 * Relocates filters based on the binding variables contained in the
	 * {@link Filter}. If you don't specify the FilterRelocator to stop at the
	 * first {@link Join}, the relocator pushes the filter as far down the query
	 * plan as possible, checking if the nodes below contain its binding
	 * variables. If stopAtFirstJoin = true, the Filter is inserted at the first
	 * Join node encountered. The relocator tracks whether the node stays in the
	 * join segment or is inserted outside of the Join segment and returns true
	 * if the Filter stays in the segment and false otherwise.
	 *
	 */

	protected static class FilterRelocator extends
			QueryModelVisitorBase<RuntimeException> {

		protected Filter filter;
		protected Set<String> filterVars;

		public FilterRelocator(Filter filter) {
			this.filter = filter;
			filterVars = VarNameCollector.process(filter.getCondition());
		}

		public static void relocate(Filter filter) {
			final FilterRelocator fr = new FilterRelocator(filter);
			filter.visit(fr);
		}

		@Override
		protected void meetNode(QueryModelNode node) {
			// By default, do not traverse
			assert node instanceof TupleExpr;

			if (node instanceof UnaryTupleOperator) {
				if (((UnaryTupleOperator) node).getArg().getBindingNames()
						.containsAll(filterVars)) {
					((UnaryTupleOperator) node).getArg().visit(this);
				}
			}
			relocate(filter, (TupleExpr) node);
		}

		@Override
		public void meet(Join join) {
			if (join.getRightArg().getBindingNames().containsAll(filterVars)) {
				// All required vars are bound by the left expr
				join.getRightArg().visit(this);
			} else if (join.getLeftArg().getBindingNames()
					.containsAll(filterVars)) {
				// All required vars are bound by the right expr
				join.getLeftArg().visit(this);
			} else {
				relocate(filter, join);
			}
		}

		@Override
		public void meet(Filter node) {
			node.getArg().visit(this);
		}

		@Override
		public void meet(LeftJoin leftJoin) {
			if (leftJoin.getLeftArg().getBindingNames().containsAll(filterVars)) {
				leftJoin.getLeftArg().visit(this);
			} else {
				relocate(filter, leftJoin.getLeftArg());
			}
		}

		@Override
		public void meet(Union union) {
			if (Sets.intersection(union.getRightArg().getBindingNames(), filterVars).size() > 0) {
				relocate(filter, union.getRightArg());
			} else if (Sets.intersection(union.getLeftArg().getBindingNames(), filterVars).size() > 0) {
				Filter clone = new Filter(filter.getArg(), filter
						.getCondition().clone());
				relocate(clone, union.getLeftArg());
			}
		}

		@Override
		public void meet(Difference node) {
			if (Sets.intersection(node.getRightArg().getBindingNames(), filterVars).size() > 0) {
				relocate(filter, node.getRightArg());
			} else if (Sets.intersection(node.getLeftArg().getBindingNames(), filterVars).size() > 0) {
				Filter clone = new Filter(filter.getArg(), filter
						.getCondition().clone());
				relocate(clone, node.getLeftArg());
			}
		}

		@Override
		public void meet(Intersection node) {
			if (Sets.intersection(node.getRightArg().getBindingNames(), filterVars).size() > 0) {
				relocate(filter, node.getRightArg());
			} else if (Sets.intersection(node.getLeftArg().getBindingNames(), filterVars).size() > 0) {
				Filter clone = new Filter(filter.getArg(), filter
						.getCondition().clone());
				relocate(clone, node.getLeftArg());
			}
		}

		@Override
		public void meet(EmptySet node) {
			if (filter.getParentNode() != null) {
				// Remove filter from its original location
				filter.replaceWith(filter.getArg());
			}
		}

		protected void relocate(Filter filter, TupleExpr newFilterArg) {
			if (!filter.getArg().equals(newFilterArg)) {
				if (filter.getParentNode() != null) {
					// Remove filter from its original location
					filter.replaceWith(filter.getArg());
				}
				// Insert filter at the new location
				newFilterArg.replaceWith(filter);
				filter.setArg(newFilterArg);
			}
		}
	}



	public static boolean pcjContainsLeftJoins(ExternalTupleSet pcj) {
	    LeftJoinVisitor lj = new LeftJoinVisitor();
	    pcj.getTupleExpr().visit(lj);
        return lj.containsLeftJoin;
    }

    protected static class LeftJoinVisitor extends QueryModelVisitorBase<RuntimeException> {

        boolean containsLeftJoin = false;

        public boolean containsLeftJoin() {
            return containsLeftJoin;
        }

        @Override
        public void meet(LeftJoin node) {
            containsLeftJoin = true;
        }
    }










}
