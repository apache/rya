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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rya.indexing.pcj.matching.QueryNodesToTupleExpr.TupleExprAndNodes;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.ValueExpr;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This class provides implementations of methods common to implementations
 * of the {@link QuerySegment} interface.
 *
 */
public abstract class AbstractQuerySegment implements QuerySegment {


	protected List<QueryModelNode> orderedNodes = new ArrayList<>();
	protected Set<QueryModelNode> unorderedNodes;
	protected Map<ValueExpr, Filter> conditionMap = Maps.newHashMap();

	/**
	 * Returns set view of nodes contained in the segment
	 */
	@Override
	public Set<QueryModelNode> getUnOrderedNodes() {
		return Collections.unmodifiableSet(unorderedNodes);
	}

	/**
	 * Returns a list view of nodes contained in this segment, where order is
	 * determined by the getJoinArgs method
	 *
	 * @param TupleExpr
	 *            from top to bottom.
	 */
	@Override
	public List<QueryModelNode> getOrderedNodes() {
		return Collections.unmodifiableList(orderedNodes);
	}

	/**
	 * Allows nodes to be reordered using {@link PCJMatcher} and set
	 * @param nodes - reordering of orderedNodes
	 */
	@Override
	public void setNodes(List<QueryModelNode> nodes) {
		Set<QueryModelNode> nodeSet = Sets.newHashSet(nodes);
		Preconditions.checkArgument(nodeSet.equals(unorderedNodes));
		orderedNodes = nodes;
		unorderedNodes = nodeSet;
	}

	/**
	 * @param query
	 *            - QuerySegment that this method checks for in this
	 *            JoinSegment
	 */
	@Override
	public boolean containsQuerySegment(QuerySegment query) {
		return unorderedNodes.containsAll(query.getUnOrderedNodes());
	}

	/**
	 * @return - a TupleExpr representing this JoinSegment
	 */
	@Override
	public TupleExprAndNodes getQuery() {
		List<QueryModelNode> nodeCopy = new ArrayList<>();
		for (QueryModelNode q : orderedNodes) {
			if (!(q instanceof ValueExpr)) {
				nodeCopy.add(q.clone());
			}
		}
		QueryNodesToTupleExpr qnt = new QueryNodesToTupleExpr(nodeCopy, getFilters());
		return qnt.getTupleAndNodes();
	}

	@Override
	public Set<Filter> getFilters() {
		Collection<Filter> filters = conditionMap.values();
		Set<Filter> filterSet = new HashSet<>();
		for (Filter filter : filters) {
			filterSet.add(filter.clone());
		}

		return filterSet;

	}


}
