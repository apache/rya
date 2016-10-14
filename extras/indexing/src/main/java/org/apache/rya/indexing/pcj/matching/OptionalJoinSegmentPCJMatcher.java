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
import java.util.List;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 *	This class matches PCJ queries to sub-queries of a given
 *	{@link OptionalJoinSegment}.  A match will occur when the
 *  {@link QueryModelNode}s of the PCJ can be grouped together
 *  in the OptionalJoinSegment and ordered to match the PCJ query.
 *
 */

public class OptionalJoinSegmentPCJMatcher extends AbstractPCJMatcher {

	public OptionalJoinSegmentPCJMatcher(Join join) {
		segment = new OptionalJoinSegment(join);
		segmentNodeList = new ArrayList<>(segment.getOrderedNodes());
		pcjToSegment = new PCJToOptionalJoinSegment();
	}

	public OptionalJoinSegmentPCJMatcher(LeftJoin join) {
		segment = new OptionalJoinSegment(join);
		segmentNodeList = new ArrayList<>(segment.getOrderedNodes());
		pcjToSegment = new PCJToOptionalJoinSegment();
	}

	public OptionalJoinSegmentPCJMatcher(Filter filter) {
		segment = new OptionalJoinSegment(filter);
		segmentNodeList = new ArrayList<>(segment.getOrderedNodes());
		pcjToSegment = new PCJToOptionalJoinSegment();
	}

	/**
	 * @param pcjNodes - {@link QuerySegment} to be replaced by PCJ
	 * @param pcj - PCJ to replace matchin QuerySegment
	 */
	@Override
	public boolean matchPCJ(QuerySegment pcjNodes, ExternalTupleSet pcj) {

		if(!segment.containsQuerySegment(pcjNodes)) {
			return false;
		}
		List<QueryModelNode> consolidatedNodes = groupNodesToMatchPCJ(getOrderedNodes(), pcjNodes.getOrderedNodes());
		if(consolidatedNodes.size() == 0) {
			return false;
		}

		//set segment nodes to the consolidated nodes to match pcj
		segment.setNodes(consolidatedNodes);
		boolean nodesReplaced = segment.replaceWithPcj(pcjNodes, pcj);

		//if pcj nodes replaced queryNodes, update segmentNodeList
		//otherwise restore segment nodes back to original pre-consolidated state
		if(nodesReplaced) {
			segmentNodeList = segment.getOrderedNodes();
			tupleAndNodesUpToDate = false;
		} else {
			segment.setNodes(segmentNodeList);
		}

		return nodesReplaced;
	}

	/**
	 *
	 * @param queryNodes - query nodes to be compared to pcj for matching
	 * @param pcjNodes - pcj nodes to match to query
	 * @return - query nodes with pcj nodes grouped together (if possible), otherwise return
	 * an empty list.
	 */
	private List<QueryModelNode> groupNodesToMatchPCJ(List<QueryModelNode> queryNodes, List<QueryModelNode> pcjNodes) {
		PCJNodeConsolidator pnc = new PCJNodeConsolidator(queryNodes, pcjNodes);
		boolean canConsolidate = pnc.consolidateNodes();
		if(canConsolidate) {
			return pnc.getQueryNodes();
		}
		return new ArrayList<QueryModelNode>();
	}


	/**
	 *	This class extracts the {@link OptionalJoinSegment} of PCJ query.
	 *
	 */
	static class PCJToOptionalJoinSegment extends QueryModelVisitorBase<RuntimeException> implements PCJToSegment {

		private OptionalJoinSegment segment;

		@Override
		public QuerySegment getSegment(ExternalTupleSet pcj) {
			segment = null;
			pcj.getTupleExpr().visit(this);
			return segment;
		}

		@Override
		public void meet(Join join) {
			segment = new OptionalJoinSegment(join);
		}

		@Override
		public void meet(Filter filter) {
			segment = new OptionalJoinSegment(filter);
		}

		@Override
		public void meet(LeftJoin node) {
			segment = new OptionalJoinSegment(node);
		}

	}


}
