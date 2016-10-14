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

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.QueryModelNode;
import org.openrdf.query.algebra.TupleExpr;

/**
 *	This interface provides a framework for matching PCJ {@link ExternalTupleSet}s
 *	to subsets of a given {@link QuerySegment}.
 *
 */
public interface PCJMatcher {

	/**
	 *
	 * @param pcjNodes - QuerySegment representation of PCJ to be used for matching
	 * @param pcj - {@link ExternalTupleSet} used to replace matching PCJ nodes when match occurs
	 * @return - true is match and replace occurs and false otherwise
	 */
	public boolean matchPCJ(QuerySegment pcjNodes, ExternalTupleSet pcj);

	/**
	 *
	 * @param pcj - {@link ExternalTupleSet} used to replace matching PCJ nodes when match occurs
	 * @return - true is match and replace occurs and false otherwise
	 */
	public boolean matchPCJ(ExternalTupleSet pcj);

	/**
	 * @return - TupleExpr constructed from {@link QuerySegment} with matched nodes
	 */
	public TupleExpr getQuery();

	/**
	 *
	 * @return - all {@link TupleExpr} that haven't been matched to a PCJ
	 */
	public Set<TupleExpr> getUnmatchedArgs();

	/**
	 *
	 * @return - provided ordered view of QuerySegment nodes
	 */
	public List<QueryModelNode> getOrderedNodes();

	/**
	 *
	 * @return - Set of {@link Filter}s of given QuerySegment
	 */
	public Set<Filter> getFilters();

}
