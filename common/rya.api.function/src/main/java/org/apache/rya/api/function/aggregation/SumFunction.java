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
package org.apache.rya.api.function.aggregation;

import static com.google.common.base.Preconditions.checkArgument;

import java.math.BigInteger;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.query.algebra.MathExpr.MathOp;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.util.MathUtil;
import org.openrdf.query.impl.MapBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Add to the {@link AggregationState}'s sum if the child Binding Set contains the binding name
 * that is being summed by the {@link AggregationElement}.
 */
@DefaultAnnotation(NonNull.class)
public final class SumFunction implements AggregationFunction {
    private static final Logger log = LoggerFactory.getLogger(SumFunction.class);

    @Override
    public void update(final AggregationElement aggregation, final AggregationState state, final VisibilityBindingSet childBindingSet) {
        checkArgument(aggregation.getAggregationType() == AggregationType.SUM, "The SumFunction only accepts SUM AggregationElements.");

        // Only add values to the sum if the child contains the binding that we are summing.
        final String aggregatedName = aggregation.getAggregatedBindingName();
        if(childBindingSet.hasBinding(aggregatedName)) {
            final MapBindingSet result = state.getBindingSet();
            final String resultName = aggregation.getResultBindingName();
            final boolean newBinding = !result.hasBinding(resultName);

            // Get the starting number for the sum.
            Literal sum;
            if(newBinding) {
                sum = new IntegerLiteralImpl(BigInteger.ZERO);
            } else {
                sum = (Literal) state.getBindingSet().getValue(resultName);
            }

            // Add the child binding set's value if it is a numeric literal.
            final Value childValue = childBindingSet.getValue(aggregatedName);
            if(childValue instanceof Literal) {
                final Literal childLiteral = (Literal) childValue;
                if (childLiteral.getDatatype() != null && XMLDatatypeUtil.isNumericDatatype(childLiteral.getDatatype())) {
                    try {
                        sum = MathUtil.compute(sum, childLiteral, MathOp.PLUS);
                    } catch (final ValueExprEvaluationException e) {
                        log.error("A problem was encountered while updating a Sum Aggregation. This binding set will be ignored: " + childBindingSet);
                        return;
                    }
                }
            }

            // Update the state to include the new sum.
            result.addBinding(resultName, sum);
        }
    }
}