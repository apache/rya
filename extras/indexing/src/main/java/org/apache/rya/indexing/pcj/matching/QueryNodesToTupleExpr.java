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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;

import com.google.common.collect.Lists;

/**
 *	This class converts a given collection of {@link QueryModelNode}s
 *	into a {@link TupleExpr}.  The primary purpose of this class is
 *	to reconstruct a TupleExpr representation of a {@link QuerySegment}
 *	from its List view.
 *
 */
public class QueryNodesToTupleExpr {

	private List<QueryModelNode> queryNodes;
	private Set<Filter> filters;

	public QueryNodesToTupleExpr(List<QueryModelNode> queryNodes, Set<Filter> filters) {
		this.queryNodes = queryNodes;
		this.filters = filters;
	}

	/**
	 *
	 * @return - a TupleExprAndNodes object that consists of the the
	 * TupleExpr representation of the List of QueryModelNodes and
	 * the nodes used to build the TupleExpr.
	 */
	public TupleExprAndNodes getTupleAndNodes() {
		List<QueryModelNode> nodeCopy = new ArrayList<>();
		Set<Filter> setCopy = new HashSet<>();
		for(QueryModelNode q: queryNodes) {
			nodeCopy.add(q.clone());
		}
		for(Filter f: filters) {
			setCopy.add(f.clone());
		}
		TupleExpr te = buildQuery(nodeCopy, setCopy);

		return new TupleExprAndNodes(te, nodeCopy, setCopy);
	}


	private TupleExpr buildQuery(List<QueryModelNode> queryNodes,
			Set<Filter> filters) {
		List<Filter> chain = getFilterChain(filters);
		return getNewJoin(queryNodes, chain);
	}

	// chain filters together and return front and back of chain
	private static List<Filter> getFilterChain(Set<Filter> filters) {
		final List<Filter> filterTopBottom = Lists.newArrayList();
		Filter filterChainTop = null;
		Filter filterChainBottom = null;

		for (final Filter filter : filters) {
			if (filterChainTop == null) {
				filterChainTop = filter;
				filter.setParentNode(null);
			} else if (filterChainBottom == null) {
				filterChainBottom = filter;
				filterChainTop.setArg(filterChainBottom);
			} else {
				filterChainBottom.setArg(filter);
				filterChainBottom = filter;
			}
		}
		if (filterChainTop != null) {
			filterTopBottom.add(filterChainTop);
		}
		if (filterChainBottom != null) {
			filterTopBottom.add(filterChainBottom);
		}
		return filterTopBottom;
	}

	// build newJoin node given remaining joinArgs and chain of filters
	private static TupleExpr getNewJoin(List<QueryModelNode> args,
			List<Filter> filterChain) {
		TupleExpr newJoin;
		TupleExpr tempJoin;
		final List<TupleExpr> joinArgs = Lists.newArrayList();
		for (QueryModelNode q : args) {
			if (q instanceof TupleExpr) {
				joinArgs.add(0, (TupleExpr) q);
			} else {
				throw new IllegalArgumentException("Invalid query node!");
			}
		}

		if (joinArgs.size() > 1) {
			TupleExpr left = joinArgs.remove(0);
			TupleExpr right = joinArgs.remove(0);
			tempJoin = getJoin(left, right);
			for (int i = joinArgs.size() - 1; i >= 0; i--) {
				tempJoin = getJoin(tempJoin, joinArgs.get(i));
			}
			if (filterChain.size() == 0) {
				newJoin = tempJoin;
			} else if (filterChain.size() == 1) {
				newJoin = filterChain.get(0);
				((Filter) newJoin).setArg(tempJoin);
			} else {
				newJoin = filterChain.get(0);
				filterChain.get(1).setArg(tempJoin);
			}
		} else if (joinArgs.size() == 1) {
			tempJoin = joinArgs.get(0);
			if (filterChain.size() == 0) {
				newJoin = tempJoin;
			} else if (filterChain.size() == 1) {
				newJoin = filterChain.get(0);
				((Filter) newJoin).setArg(tempJoin);
			} else {
				newJoin = filterChain.get(0);
				filterChain.get(1).setArg(tempJoin);
			}
		} else {
			throw new IllegalStateException("JoinArgs size cannot be zero.");
		}
		return newJoin;
	}

	private static TupleExpr getJoin(TupleExpr oldJoin, TupleExpr newArg) {
		if (newArg instanceof FlattenedOptional) {
			return new LeftJoin(oldJoin,
					((FlattenedOptional) newArg).getRightArg());
		} else {
			return new Join(oldJoin, newArg);
		}
	}

	public static class TupleExprAndNodes {

		private TupleExpr te;
		private List<QueryModelNode> nodes;
		private Set<Filter> filters;

		public TupleExprAndNodes(TupleExpr te, List<QueryModelNode> nodes, Set<Filter> filters) {
			this.te = te;
			this.nodes = nodes;
			this.filters = filters;
		}

		public TupleExpr getTupleExpr() {
			return te;
		}

		public List<QueryModelNode> getNodes() {
			return nodes;
		}

		public Set<Filter> getFilters() {
			return filters;
		}

		@Override
		public String toString() {
			return "Query: " + te + "   Nodes: " + nodes;
		}


	}

}
