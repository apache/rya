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

import java.util.Optional;

import org.apache.rya.api.model.VisibilityBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Provides a mechanism for storing the updating {@link AggregationState} while using an {@link AggregationsEvaluator}.
 */
@DefaultAnnotation(NonNull.class)
public interface AggregationStateStore {

    /**
     * Stores a state. If this value updates a previously stored state, then it will overwrite the old value
     * with the new one.
     *
     * @param state - The state that will be stored. (not null)
     */
    public void store(AggregationState state);

    /**
     * Get the {@link AggregationState} that may be updatted using the provided binding set.
     *
     * @param bs - A binding set that defines which state to fetch. (not null)
     * @return The {@link AggregationState} that is updated by the binding set, if one has been stored.
     */
    public Optional<AggregationState> get(VisibilityBindingSet bs);
}