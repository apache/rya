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

import java.util.List;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.rdftriplestore.inference.DoNotExpandSP;
import org.apache.rya.rdftriplestore.utils.FixedStatementPattern;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * An OptionalJoinSegment represents the portion of a {@link TupleExpr} that is
 * connected by Filters, LeftJoins, and Joins. All nodes in the portion of the
 * TupleExpr that are connected via these node types are gathered into an
 * ordered and an unordered list that can easily be compared with
 * {@link ExternalTupleSet} nodes for sub-query matching to use with Precomputed
 * Joins.
 *
 */
public class OptionalJoinSegment extends AbstractQuerySegment {

	public OptionalJoinSegment(Join join) {
		Preconditions.checkNotNull(join);
		createJoinSegment(join);
	}

	public OptionalJoinSegment(LeftJoin join) {
		Preconditions.checkNotNull(join);
		createJoinSegment(join);
	}

	public OptionalJoinSegment(Filter filter) {
		Preconditions.checkNotNull(filter);
		createJoinSegment(filter);
	}

	private void createJoinSegment(TupleExpr te) {
		orderedNodes = getJoinArgs(te, orderedNodes);
		unorderedNodes = Sets.newHashSet(orderedNodes);
	}

	/**
	 * This method matches the ordered nodes returned by
	 * {@link JoinSegment #getOrderedNodes()} for nodeToReplace with a subset of
	 * the ordered nodes for this JoinSegment. The order of the nodes for
	 * nodeToReplace must match the order of the nodes as a subset of
	 * orderedNodes
	 *
	 * @param nodeToReplace
	 *            - nodes to be replaced by pcj
	 * @param pcj
	 *            - pcj node that will replace specified query nodes
	 */
	@Override
	public boolean replaceWithPcj(QuerySegment nodeToReplace,
			ExternalTupleSet pcj) {
		Preconditions.checkNotNull(nodeToReplace != null);
		Preconditions.checkNotNull(pcj);
		if (!containsQuerySegment(nodeToReplace)) {
			return false;
		}
		List<QueryModelNode> nodeList = nodeToReplace.getOrderedNodes();
		int begin = orderedNodes.indexOf(nodeList.get(0));
		// TODO this assumes no duplicate nodes
		if (begin < 0
				|| begin + nodeList.size() > orderedNodes.size()
				|| !nodeList.equals(orderedNodes.subList(begin, begin
						+ nodeList.size()))) {
			return false;
		}
		orderedNodes.removeAll(nodeList);
		orderedNodes.add(begin, pcj);
		unorderedNodes.removeAll(nodeList);
		unorderedNodes.add(pcj);
		for (QueryModelNode q : nodeList) {
			if (q instanceof ValueExpr) {
				conditionMap.remove(q);
			}
		}
		return true;
	}

	/**
	 *
	 * @param tupleExpr
	 *            - the query object that will be traversed by this method
	 * @param joinArgs
	 *            - all nodes connected by Joins, LeftJoins, and Filters
	 * @return - List containing all nodes connected by Joins, LeftJoins, and
	 *         Filters. This List contains the {@link ValueExpr} in place of the
	 *         Filter and a {@link FlattenedOptional} in place of the LeftJoin
	 *         for ease of comparison with PCJ nodes.
	 */
	private List<QueryModelNode> getJoinArgs(TupleExpr tupleExpr,
			List<QueryModelNode> joinArgs) {

		if (tupleExpr instanceof Join) {
			if (!(((Join) tupleExpr).getLeftArg() instanceof FixedStatementPattern)
					&& !(((Join) tupleExpr).getRightArg() instanceof DoNotExpandSP)) {
				Join join = (Join) tupleExpr;
				getJoinArgs(join.getRightArg(), joinArgs);
				getJoinArgs(join.getLeftArg(), joinArgs);
			}
		} else if (tupleExpr instanceof LeftJoin) {
			LeftJoin lj = (LeftJoin) tupleExpr;
			joinArgs.add(new FlattenedOptional(lj));
			getJoinArgs(lj.getLeftArg(), joinArgs);
		} else if (tupleExpr instanceof Filter) {
			Filter filter = (Filter) tupleExpr;
			joinArgs.add(filter.getCondition());
			conditionMap.put(filter.getCondition(), filter);
			getJoinArgs(filter.getArg(), joinArgs);
		} else {
			joinArgs.add(tupleExpr);
		}
		return joinArgs;
	}

}
