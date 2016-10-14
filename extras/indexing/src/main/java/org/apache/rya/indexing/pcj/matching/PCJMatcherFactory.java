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

import org.openrdf.query.algebra.Filter;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.LeftJoin;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;

/**
 *	This class takes in a given {@link Join}, {@Filter}, or {@link LeftJoin}
 *	and provides the appropriate {@link PCJMatcher} to match PCJs to the
 *	given query.
 *
 */

public class PCJMatcherFactory {

	public static PCJMatcher getPCJMatcher(Join join) {
		if (segmentContainsLeftJoins(join)) {
			return new OptionalJoinSegmentPCJMatcher(join);
		} else {
			return new JoinSegmentPCJMatcher(join);
		}
	}

	public static PCJMatcher getPCJMatcher(LeftJoin join) {
		return new OptionalJoinSegmentPCJMatcher(join);
	}

	public static PCJMatcher getPCJMatcher(Filter filter) {
		if (segmentContainsLeftJoins(filter)) {
			return new OptionalJoinSegmentPCJMatcher(filter);
		} else {
			return new JoinSegmentPCJMatcher(filter);
		}
	}

	private static boolean segmentContainsLeftJoins(TupleExpr tupleExpr) {

		if (tupleExpr instanceof Projection) {
			return segmentContainsLeftJoins(((Projection) tupleExpr).getArg());
		} else if (tupleExpr instanceof Join) {
			Join join = (Join) tupleExpr;
			return segmentContainsLeftJoins(join.getRightArg())
					|| segmentContainsLeftJoins(join.getLeftArg());
		} else if (tupleExpr instanceof LeftJoin) {
			return true;
		} else if (tupleExpr instanceof Filter) {
			return segmentContainsLeftJoins(((Filter) tupleExpr).getArg());
		} else {
			return false;
		}
	}
}
