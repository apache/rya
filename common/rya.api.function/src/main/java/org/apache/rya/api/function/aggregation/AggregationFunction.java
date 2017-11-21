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

import org.apache.rya.api.model.VisibilityBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A function that updates an {@link AggregationState}.
 */
@DefaultAnnotation(NonNull.class)
public interface AggregationFunction {

    /**
     * Updates an {@link AggregationState} based on the values of a child Binding Set.
     *
     * @param aggregation - Defines which function needs to be performed as well as any details required
     *   to do the aggregation work. (not null)
     * @param state - The state that will be updated. (not null)
     * @param childBindingSet - The Binding Set whose values will be used to update the state.
     */
    public void update(AggregationElement aggregation, AggregationState state, VisibilityBindingSet childBindingSet);
}