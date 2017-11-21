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

import org.apache.rya.api.model.VisibilityBindingSet;
import org.openrdf.model.Value;
import org.openrdf.query.algebra.evaluation.util.ValueComparator;
import org.openrdf.query.impl.MapBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Update the {@link AggregationState}'s min if the child binding Set contains the binding name that is being
 * mined by the {@link AggregationElement}.
 */
@DefaultAnnotation(NonNull.class)
public final class MinFunction implements AggregationFunction {

    private final ValueComparator compare = new ValueComparator();

    @Override
    public void update(final AggregationElement aggregation, final AggregationState state, final VisibilityBindingSet childBindingSet) {
        checkArgument(aggregation.getAggregationType() == AggregationType.MIN, "The MinFunction only accepts MIN AggregationElements.");

        // Only update the min if the child contains the binding that we are finding the min value for.
        final String aggregatedName = aggregation.getAggregatedBindingName();
        if(childBindingSet.hasBinding(aggregatedName)) {
            final MapBindingSet result = state.getBindingSet();
            final String resultName = aggregation.getResultBindingName();
            final boolean newBinding = !result.hasBinding(resultName);

            Value min;
            if(newBinding) {
                min = childBindingSet.getValue(aggregatedName);
            } else {
                final Value oldMin = result.getValue(resultName);
                final Value chidlMin = childBindingSet.getValue(aggregatedName);
                min = compare.compare(chidlMin, oldMin) < 0 ? chidlMin : oldMin;
            }

            result.addBinding(resultName, min);
        }
    }
}