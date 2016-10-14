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
import java.util.Set;

import org.apache.rya.indexing.external.tupleSet.ExternalTupleSet;
import org.apache.rya.indexing.pcj.matching.QueryNodesToTupleExpr.TupleExprAndNodes;

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.QueryModelNode;

/**
 * A QuerySegment represents a subset of a query to be compared to PCJs for
 * query matching. The QuerySegment is represented as a List, where the order of
 * the nodes in the list is determined by a Visitor as it traverses the Segment
 * from top down, visiting right children before left.
 *
 */
public interface QuerySegment {

	/**
	 *
	 * @return - an unordered view of the {@link QueryModelNode}s in the segment
	 */
	public Set<QueryModelNode> getUnOrderedNodes();

	/**
	 *
	 * @return - an ordered view of the {@link QueryModelNode}s in the segment.
	 */
	public List<QueryModelNode> getOrderedNodes();

	public Set<Filter> getFilters();

	/**
	 *
	 * @param segment
	 *            - this method verifies whether the specified segment is
	 *            contained in this segment
	 * @return - true if contained and false otherwise
	 */
	public boolean containsQuerySegment(QuerySegment segment);

	/**
	 * Sets List of {@link QueryModelNode}s representing this QuerySegment
	 * to specified list

	 * @param nodes - nodes to set
	 */
	public void setNodes(List<QueryModelNode> nodes);

	/**
	 *
	 * @param nodeToReplace - QuerySegment representation of PCJ to match
	 * with subset of this QuerySegment
	 * @param PCJ - PCJ to replace matching QuerySegment nodes if match occurs
	 * @return - true if match occurs and false otherwise
	 */
	public boolean replaceWithPcj(QuerySegment nodeToReplace,
			ExternalTupleSet PCJ);

	public TupleExprAndNodes getQuery();

}
