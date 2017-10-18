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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.visibility.VisibilitySimplifier;
import org.eclipse.rdf4j.query.algebra.AggregateOperator;
import org.eclipse.rdf4j.query.algebra.Group;
import org.eclipse.rdf4j.query.algebra.GroupElem;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.impl.MapBindingSet;

import com.google.common.collect.ImmutableMap;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A stateful evaluator that processes aggregation functions over variables that are grouped together.
 * </p>
 * The following aggregation functions are supported:
 * <ul>
 *   <li>Count</li>
 *   <li>Sum</li>
 *   <li>Average</li>
 *   <li>Min</li>
 *   <li>Max</li>
 * </ul>
 * </p>
 * The persistence of the aggregation's state is determined by the provided {@link AggregationStateStore}.
 */
@DefaultAnnotation(NonNull.class)
public class AggregationsEvaluator {

    private static final ImmutableMap<AggregationType, AggregationFunction> FUNCTIONS;
    static {
        final ImmutableMap.Builder<AggregationType, AggregationFunction> builder = ImmutableMap.builder();
        builder.put(AggregationType.COUNT, new CountFunction());
        builder.put(AggregationType.SUM, new SumFunction());
        builder.put(AggregationType.AVERAGE, new AverageFunction());
        builder.put(AggregationType.MIN, new MinFunction());
        builder.put(AggregationType.MAX, new MaxFunction());
        FUNCTIONS = builder.build();
    }

    private final AggregationStateStore aggStateStore;
    private final Collection<AggregationElement> aggregations;
    private final List<String> groupByVars;

    /**
     * Constructs an instance of {@link AggregationsEvaluator}.
     *
     * @param aggStateStore - The mechanism for storing aggregation state. (not null)
     * @param aggregations - The aggregation functions that will be computed. (not null)
     * @param groupByVars - The names of the binding whose values are used to group aggregation results. (not null)
     */
    public AggregationsEvaluator(
            final AggregationStateStore aggStateStore,
            final Collection<AggregationElement> aggregations,
            final List<String> groupByVars) {
        this.aggStateStore = requireNonNull(aggStateStore);
        this.aggregations = requireNonNull(aggregations);
        this.groupByVars = requireNonNull(groupByVars);
    }

    /**
     * Make an instance of {@link AggregationsEvaluator} based on a {@link Group} node.
     *
     * @param aggStateStore - The mechanism for storing aggregation state. (not null)
     * @param aggNode - Defines which aggregation functions need to be performed.
     * @param groupByVars - The names of the binding whose values are used to group aggregation results. (not null)
     * @return The evaluator that handles the node's aggregations.
     */
    public static  AggregationsEvaluator make(final AggregationStateStore aggStateStore, final Group aggNode, final List<String> groupByVars) {
        requireNonNull(aggStateStore);
        requireNonNull(aggNode);
        requireNonNull(groupByVars);

        // The aggregations that need to be performed are the Group Elements.
        final List<AggregationElement> aggregations = new ArrayList<>();
        for(final GroupElem groupElem : aggNode.getGroupElements()) {
            // Figure out the type of the aggregation.
            final AggregateOperator operator = groupElem.getOperator();
            final Optional<AggregationType> type = AggregationType.byOperatorClass( operator.getClass() );

            // If the type is one we support, create the AggregationElement.
            if(type.isPresent()) {
                final String resultBindingName = groupElem.getName();

                final AtomicReference<String> aggregatedBindingName = new AtomicReference<>();
                groupElem.visitChildren(new AbstractQueryModelVisitor<RuntimeException>() {
                    @Override
                    public void meet(final Var node) {
                        aggregatedBindingName.set( node.getName() );
                    }
                });

                aggregations.add( new AggregationElement(type.get(), aggregatedBindingName.get(), resultBindingName) );
            }
        }

        return new AggregationsEvaluator(aggStateStore, aggregations, groupByVars);
    }

    /**
     * Update the aggregation values using information found within {@code newBs}.
     *
     * @param newBs - A binding set whose values need to be incorporated within the aggregations. (not null)
     * @return A binding set containing the updated aggregation values.
     */
    public VisibilityBindingSet update(final VisibilityBindingSet newBs) {
        requireNonNull(newBs);

        // Load the old state if one was previously stored; otherwise initialize the state.
        final AggregationState state = aggStateStore.get(newBs).orElseGet(() -> {
            // Initialize a new state.
            final AggregationState newState = new AggregationState();

            // If we have group by bindings, their values need to be added to the state's binding set.
            final MapBindingSet bindingSet = newState.getBindingSet();
            for(final String groupByVar : groupByVars) {
                bindingSet.addBinding( newBs.getBinding(groupByVar) );
            }

            return newState;
        });

        // Update the visibilities of the result binding set based on the new result's visibilities.
        final String oldVisibility = state.getVisibility();
        final String updateVisibilities = VisibilitySimplifier.unionAndSimplify(oldVisibility, newBs.getVisibility());
        state.setVisibility(updateVisibilities);

        // Update the Aggregation State with each Aggregation function included within this group.
        for(final AggregationElement aggregation : aggregations) {
            final AggregationType type = aggregation.getAggregationType();
            final AggregationFunction function = FUNCTIONS.get(type);
            if(function == null) {
                throw new RuntimeException("Unrecognized aggregation function: " + type);
            }

            function.update(aggregation, state, newBs);
        }

        // Store the updated state. This will write on top of any old state that was present for the Group By values.
        aggStateStore.store(state);

        // Return the updated binding set from the updated state.
        return new VisibilityBindingSet(state.getBindingSet(), state.getVisibility());
    }
}