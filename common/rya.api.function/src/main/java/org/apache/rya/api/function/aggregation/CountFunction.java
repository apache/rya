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
import static java.util.Objects.requireNonNull;

import java.math.BigInteger;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.openrdf.model.Literal;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.query.impl.MapBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Increments the {@link AggregationState}'s count if the child Binding Set contains the binding name
 * that is being counted by the {@link AggregationElement}.
 */
@DefaultAnnotation(NonNull.class)
public final class CountFunction implements AggregationFunction {
    @Override
    public void update(final AggregationElement aggregation, final AggregationState state, final VisibilityBindingSet childBindingSet) {
        checkArgument(aggregation.getAggregationType() == AggregationType.COUNT, "The CountFunction only accepts COUNT AggregationElements.");
        requireNonNull(state);
        requireNonNull(childBindingSet);

        // Only add one to the count if the child contains the binding that we are counting.
        final String aggregatedName = aggregation.getAggregatedBindingName();
        if(childBindingSet.hasBinding(aggregatedName)) {
            final MapBindingSet result = state.getBindingSet();
            final String resultName = aggregation.getResultBindingName();
            final boolean newBinding = !result.hasBinding(resultName);

            if(newBinding) {
                // Initialize the binding.
                result.addBinding(resultName, new IntegerLiteralImpl(BigInteger.ONE));
            } else {
                // Update the existing binding.
                final Literal count = (Literal) result.getValue(resultName);
                final BigInteger updatedCount = count.integerValue().add( BigInteger.ONE );
                result.addBinding(resultName, new IntegerLiteralImpl(updatedCount));
            }
        }
    }
}