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

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.openrdf.query.impl.MapBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Keeps track information required to update and build the resulting Binding Set for a set of Group By values.
 */
@DefaultAnnotation(NonNull.class)
public final class AggregationState implements Serializable {
    private static final long serialVersionUID = 1L;

    // The visibility equation that encompasses all data the aggregation state is derived from.
    private String visibility;

    // A binding set that holds the current state of the aggregations.
    private final MapBindingSet bindingSet;

    // A map from result binding name to the state that derived that binding's value.
    private final Map<String, AverageState> avgStates;

    /**
     * Constructs an instance of {@link AggregationState}.
     */
    public AggregationState() {
        this.visibility = "";
        this.bindingSet = new MapBindingSet();
        this.avgStates = new HashMap<>();
    }

    /**
     * Constructs an instance of {@link AggregationState}.
     *
     * @param visibility - The visibility equation associated with the resulting binding set. (not null)
     * @param bindingSet - The Binding Set whose values are being updated. It holds the result for a set of
     *   Group By values. (not null)
     * @param avgStates - If the aggregation is doing an Average, this is a map from result binding name to
     *   average state for that binding.
     */
    public AggregationState(
            final String visibility,
            final MapBindingSet bindingSet,
            final Map<String, AverageState> avgStates) {
        this.visibility = requireNonNull(visibility);
        this.bindingSet = requireNonNull(bindingSet);
        this.avgStates = requireNonNull(avgStates);
    }

    /**
     * @return The visibility equation associated with the resulting binding set.
     */
    public String getVisibility() {
        return visibility;
    }

    /**
     * @param visibility - The visibility equation associated with the resulting binding set.
     */
    public void setVisibility(final String visibility) {
        this.visibility = requireNonNull(visibility);
    }

    /**
     * @return The Binding Set whose values are being updated. It holds the result for a set of Group By values.
     */
    public MapBindingSet getBindingSet() {
        return bindingSet;
    }

    /**
     * @return If the aggregation is doing an Average, this is a map from result binding name to
     *   average state for that binding.
     */
    public Map<String, AverageState> getAverageStates() {
        return avgStates;
    }

    @Override
    public int hashCode() {
        return Objects.hash(visibility, bindingSet, avgStates);
    }

    @Override
    public boolean equals(final Object o) {
        if(o instanceof AggregationState) {
            final AggregationState state = (AggregationState) o;
            return Objects.equals(visibility, state.visibility) &&
                    Objects.equals(bindingSet, state.bindingSet) &&
                    Objects.equals(avgStates, state.avgStates);
        }
        return false;
    }
}