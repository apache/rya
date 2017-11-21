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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.DecimalLiteralImpl;
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
 * Update the {@link AggregationState}'s average if the child Binding Set contains the binding name
 * that is being averaged by the {@link AggregationElement}.
 */
@DefaultAnnotation(NonNull.class)
public final class AverageFunction implements AggregationFunction {
    private static final Logger log = LoggerFactory.getLogger(AverageFunction.class);

    @Override
    public void update(final AggregationElement aggregation, final AggregationState state, final VisibilityBindingSet childBindingSet) {
        checkArgument(aggregation.getAggregationType() == AggregationType.AVERAGE, "The AverageFunction only accepts AVERAGE AggregationElements.");

        // Only update the average if the child contains the binding that we are averaging.
        final String aggregatedName = aggregation.getAggregatedBindingName();
        if(childBindingSet.hasBinding(aggregatedName)) {
            final MapBindingSet result = state.getBindingSet();
            final String resultName = aggregation.getResultBindingName();
            final boolean newBinding = !result.hasBinding(resultName);

            // Get the state of the average.
            final Map<String, AverageState> averageStates = state.getAverageStates();
            AverageState averageState = newBinding ? new AverageState() : averageStates.get(resultName);

            // Update the state of the average.
            final Value childValue = childBindingSet.getValue(aggregatedName);
            if(childValue instanceof Literal) {
                final Literal childLiteral = (Literal) childValue;
                if (childLiteral.getDatatype() != null && XMLDatatypeUtil.isNumericDatatype(childLiteral.getDatatype())) {
                    try {
                        // Update the sum.
                        final Literal oldSum = new DecimalLiteralImpl(averageState.getSum());
                        final BigDecimal sum = MathUtil.compute(oldSum, childLiteral, MathOp.PLUS).decimalValue();

                        // Update the count.
                        final BigInteger count = averageState.getCount().add( BigInteger.ONE );

                        // Update the BindingSet to include the new average.
                        final Literal sumLiteral = new DecimalLiteralImpl(sum);
                        final Literal countLiteral = new IntegerLiteralImpl(count);
                        final Literal average = MathUtil.compute(sumLiteral, countLiteral, MathOp.DIVIDE);
                        result.addBinding(resultName, average);

                        // Update the average state that is stored.
                        averageState = new AverageState(sum, count);
                        averageStates.put(resultName, averageState);
                    } catch (final ValueExprEvaluationException e) {
                        log.error("A problem was encountered while updating an Average Aggregation. This binding set will be ignored: " + childBindingSet);
                        return;
                    }
                }
            }
        }
    }
}