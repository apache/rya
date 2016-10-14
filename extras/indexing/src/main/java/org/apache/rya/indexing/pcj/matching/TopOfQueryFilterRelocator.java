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
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.ValueExpr;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

/**
 *	Class consisting of a single utility method for relocating filters.
 *
 */
public class TopOfQueryFilterRelocator  {


	/**
	 *
	 * This method moves the Filters of a specified {@link TupleExpr}
	 * to the top of the TupleExpr.
	 *
	 * @param query - query whose filters will be relocated
	 * @return - TupleExpr with filters relocated to top
	 */
	public static TupleExpr moveFiltersToTop(TupleExpr query) {

		ProjectionAndFilterGatherer fg = new ProjectionAndFilterGatherer();
		query.visit(fg);
		List<ValueExpr> filterCond = new ArrayList<>(fg.filterCond);
		Projection projection = fg.projection;

		if(filterCond.size() == 0) {
			return query;
		}

		Filter first = new Filter();
		first.setCondition(filterCond.remove(0));
		Filter current = first;
		for(ValueExpr cond: filterCond) {
			Filter filter = new Filter(null, cond);
			current.setArg(filter);
			current = filter;
		}

		TupleExpr te = projection.getArg();
		projection.setArg(first);
		current.setArg(te);

		return query;

	}


	static class ProjectionAndFilterGatherer extends QueryModelVisitorBase<RuntimeException> {

		Set<ValueExpr> filterCond = new HashSet<>();
		Projection projection;


		@Override
		public void meet(Projection node) {
			this.projection = node;
			node.getArg().visit(this);
		}

		@Override
		public void meet(Filter node) {
			filterCond.add(node.getCondition());
			node.replaceWith(node.getArg());
		}

	}

}
