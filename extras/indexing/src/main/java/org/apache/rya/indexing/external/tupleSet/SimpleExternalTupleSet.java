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
package org.apache.rya.indexing.external.tupleSet;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryModelVisitor;

import com.google.common.base.Joiner;

import info.aduna.iteration.CloseableIteration;

/**
 *  This a testing class to create mock pre-computed join nodes in order to
 *  test the {@link PrecompJoinOptimizer} for query planning.
 */
public class SimpleExternalTupleSet extends ExternalTupleSet {

    /**
     * Constructs an instance of {@link SimpleExternalTupleSet}.
     *
     * @param tuple - An expression that represents the PCJ. (not null)
     */
	public SimpleExternalTupleSet(final Projection tuple) {
		this.setProjectionExpr(tuple);
		setSupportedVarOrders();
	}

	private void setSupportedVarOrders() {
	    final List<String> varOrders = new ArrayList<>();

	    String varOrder = "";
	    for(final String var : this.getTupleExpr().getAssuredBindingNames()) {
	        varOrder = varOrder.isEmpty() ? var : varOrder + VAR_ORDER_DELIM + var;
	        varOrders.add( varOrder );
	    }

	    this.setSupportedVariableOrderMap(varOrders);
	}

	@Override
	public <X extends Exception> void visit(final QueryModelVisitor<X> visitor)
			throws X {
		visitor.meetOther(this);
	}

	@Override
	public CloseableIteration<BindingSet, QueryEvaluationException> evaluate( final BindingSet bindings) throws QueryEvaluationException {
		// Intentionally does nothing.
		return null;
	}

	@Override
	public String getSignature() {
		return "(SimpleExternalTupleSet) "
				+ Joiner.on(", ")
						.join(this.getTupleExpr().getProjectionElemList()
								.getElements()).replaceAll("\\s+", " ");

	}
}