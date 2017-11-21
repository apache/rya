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
package org.apache.rya.streams.kafka.processors.aggregation;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.rya.api.function.aggregation.AggregationState;
import org.apache.rya.api.function.aggregation.AggregationStateStore;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.openrdf.query.BindingSet;

import com.google.common.base.Joiner;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link KeyValueStore} implementation of {@link AggregationStateStore}.
 * </p>
 * This is a key/value store, so we need to store the {@link AggregationState} for each set of group by values
 * using a key that is composed with those values. We use the following pattern to accomplish this:
 * <pre>
 * [groupByVar1 value],[groupByVar2 value],...,[groupByVarN value]
 * </pre>
 */
@DefaultAnnotation(NonNull.class)
public class KeyValueAggregationStateStore implements AggregationStateStore {

    private final KeyValueStore<String, AggregationState> store;
    private final List<String> groupByVars;

    /**
     * Constructs an instance of {@link KeyValueAggregationStateStore}.
     *
     * @param store - The state store that will be used. (not null)
     * @param groupByVars - An ordered list of group by variable names. (not null)
     */
    public KeyValueAggregationStateStore(
            final KeyValueStore<String, AggregationState> store,
            final List<String> groupByVars) {
        this.store = requireNonNull(store);
        this.groupByVars = requireNonNull(groupByVars);
    }

    @Override
    public void store(final AggregationState state) {
        requireNonNull(state);

        // Aggregations group their states by their group by variables, so the key is the resulting binding
        // set's values for the group by variables.
        final String key = makeCommaDelimitedValues(groupByVars, state.getBindingSet());
        store.put(key, state);
    }

    @Override
    public Optional<AggregationState> get(final VisibilityBindingSet bs) {
        requireNonNull(bs);

        final String key = makeCommaDelimitedValues(groupByVars, bs);
        return Optional.ofNullable(store.get(key));
    }

    /**
     * A utility function that helps construct the keys used by {@link KeyValueAggregationStateStore}.
     *
     * @param vars - Which variables within the binding set to use for the key's values. (not null)
     * @param bindingSet - The binding set the key is being constructed from. (not null)
     * @return A comma delimited list of the binding values, leading with the side.
     */
    private static String makeCommaDelimitedValues(final List<String> vars, final BindingSet bindingSet) {
        requireNonNull(vars);
        requireNonNull(bindingSet);

        // Make a an ordered list of the binding set variables.
        final List<String> values = new ArrayList<>();
        for(final String var : vars) {
            values.add( bindingSet.hasBinding(var) ? bindingSet.getBinding(var).getValue().toString() : "" );
        }

        // Return a comma delimited list of those values.
        return Joiner.on(",").join(values);
    }
}