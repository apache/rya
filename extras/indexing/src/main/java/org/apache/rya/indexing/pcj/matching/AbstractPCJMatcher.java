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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.pcj.matching.QueryNodesToTupleExpr.TupleExprAndNodes;

import org.openrdf.query.algebra.BinaryTupleOperator;
import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UnaryTupleOperator;

/**
 * This class provides implementations of methods common to all implementations of
 * the {@link PCJMatcher} interface.
 *
 */
public abstract class AbstractPCJMatcher implements PCJMatcher {

	protected QuerySegment segment;
	protected List<QueryModelNode> segmentNodeList;
	protected boolean tupleAndNodesUpToDate = false;
	protected TupleExpr tuple;
	protected Set<TupleExpr> unmatched;
	protected PCJToSegment pcjToSegment;
	protected Set<Filter> filters;

	/**
	 * @param - pcj - PremomputedJoin to be matched to a subset of segment
	 * @return - true if match occurs and false otherwise
	 */
	@Override
	public boolean matchPCJ(ExternalTupleSet pcj) {
		QuerySegment sgmnt = pcjToSegment.getSegment(pcj);
		if(sgmnt == null) {
			throw new IllegalArgumentException("PCJ must contain at east one Join or Left Join");
		}
		return matchPCJ(sgmnt, pcj);
	}

	/**
	 * In following method, order is determined by the order in which the
	 * node appear in the query.
	 * @return - an ordered view of the QueryModelNodes appearing tuple
	 *
	 */
	@Override
	public List<QueryModelNode> getOrderedNodes() {
		return Collections.unmodifiableList(segmentNodeList);
	}


	@Override
	public Set<Filter> getFilters() {
		if (!tupleAndNodesUpToDate) {
			updateTupleAndNodes();
		}
		return filters;
	}

	@Override
	public TupleExpr getQuery() {
		if (!tupleAndNodesUpToDate) {
			updateTupleAndNodes();
		}
		return tuple;
	}

	@Override
	public Set<TupleExpr> getUnmatchedArgs() {
		if (!tupleAndNodesUpToDate) {
			updateTupleAndNodes();
		}
		return unmatched;
	}


	private void updateTupleAndNodes() {
		TupleExprAndNodes tupAndNodes = segment.getQuery();
		tuple = tupAndNodes.getTupleExpr();
		filters = tupAndNodes.getFilters();
		unmatched = new HashSet<>();
		List<QueryModelNode> nodes = tupAndNodes.getNodes();
		for (QueryModelNode q : nodes) {
			if (q instanceof UnaryTupleOperator
					|| q instanceof BinaryTupleOperator) {
				unmatched.add((TupleExpr) q);
			}
		}
		tupleAndNodesUpToDate = true;
	}


	/**
	 * Interface for converting an {@link ExternalTupleSet} (PCJ) into a
	 * {@link QuerySegment}.
	 *
	 */
	interface PCJToSegment {
		public QuerySegment getSegment(ExternalTupleSet pcj);
	}

}
