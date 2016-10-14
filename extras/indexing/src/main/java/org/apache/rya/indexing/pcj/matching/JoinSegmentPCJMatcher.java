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

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 * This class is responsible for matching PCJ nodes with subsets of the
 * {@link QueryModelNode}s found in {@link JoinSegment}s. Each PCJ is reduced to
 * a bag of QueryModelNodes and set operations can be used to determine if the
 * PCJ is a subset of the JoinSegment. If it is a subset, the PCJ node replaces
 * the QueryModelNodes in the JoinSegment.
 *
 */

public class JoinSegmentPCJMatcher extends AbstractPCJMatcher {

	public JoinSegmentPCJMatcher(Join join) {
		segment = new JoinSegment(join);
		segmentNodeList = new ArrayList<>(segment.getOrderedNodes());
		pcjToSegment = new PCJToJoinSegment();
	}

	public JoinSegmentPCJMatcher(Filter filter) {
		segment = new JoinSegment(filter);
		segmentNodeList = new ArrayList<>(segment.getOrderedNodes());
		pcjToSegment = new PCJToJoinSegment();
	}

	/**
	 * @param pcjNodes
	 *            - {@link QueryModelNode}s to be replaced
	 * @param pcj
	 *            - the PCJ node to be compared to pcjNodes
	 */
	@Override
	public boolean matchPCJ(QuerySegment pcjNodes, ExternalTupleSet pcj) {

        if(PCJOptimizerUtilities.pcjContainsLeftJoins(pcj)) {
            return false;
        }

		boolean nodesReplaced = segment.replaceWithPcj(pcjNodes, pcj);
		if (nodesReplaced) {
			tupleAndNodesUpToDate = false;
			segmentNodeList = segment.getOrderedNodes();
		}

		return nodesReplaced;
	}

	/**
	 * This class extracts the {@link JoinSegment} from the {@link TupleExpr} of
	 * specified PCJ.
	 *
	 */
	static class PCJToJoinSegment extends
			QueryModelVisitorBase<RuntimeException> implements PCJToSegment {

		private JoinSegment segment;

		@Override
		public QuerySegment getSegment(ExternalTupleSet pcj) {
			segment = null;
			pcj.getTupleExpr().visit(this);
			return segment;
		}

		@Override
		public void meet(Join join) {
			segment = new JoinSegment(join);
		}

		@Override
		public void meet(Filter filter) {
			segment = new JoinSegment(filter);
		}

	}

}
