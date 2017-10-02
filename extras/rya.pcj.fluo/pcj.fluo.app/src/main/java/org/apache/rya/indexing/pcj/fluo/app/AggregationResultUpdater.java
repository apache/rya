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
package org.apache.rya.indexing.pcj.fluo.app;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.utils.VisibilitySimplifier;
import org.apache.rya.api.log.LogUtils;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.fluo.app.query.AggregationMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.AggregationMetadata.AggregationElement;
import org.apache.rya.indexing.pcj.fluo.app.query.AggregationMetadata.AggregationType;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.model.Literal;
import org.openrdf.model.Value;
import org.openrdf.model.datatypes.XMLDatatypeUtil;
import org.openrdf.model.impl.DecimalLiteralImpl;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.query.algebra.MathExpr.MathOp;
import org.openrdf.query.algebra.evaluation.ValueExprEvaluationException;
import org.openrdf.query.algebra.evaluation.util.MathUtil;
import org.openrdf.query.algebra.evaluation.util.ValueComparator;
import org.openrdf.query.impl.MapBindingSet;

import com.google.common.collect.ImmutableMap;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Updates the results of an Aggregate node when its child has added a new Binding Set to its results.
 */
@DefaultAnnotation(NonNull.class)
public class AggregationResultUpdater extends AbstractNodeUpdater {
    private static final Logger log = Logger.getLogger(AggregationResultUpdater.class);

    private static final AggregationStateSerDe AGG_STATE_SERDE = new ObjectSerializationAggregationStateSerDe();

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

    /**
     * Updates the results of an Aggregation node where its child has emitted a new Binding Set.
     *
     * @param tx - The transaction all Fluo queries will use. (not null)
     * @param childBindingSet - The Binding Set that was omitted by the Aggregation Node's child. (not null)
     * @param aggregationMetadata - The metadata of the Aggregation node whose results will be updated. (not null)
     * @throws Exception The update could not be successfully performed.
     */
    public void updateAggregateResults(
            final TransactionBase tx,
            final VisibilityBindingSet childBindingSet,
            final AggregationMetadata aggregationMetadata) throws Exception {
        requireNonNull(tx);
        requireNonNull(childBindingSet);
        requireNonNull(aggregationMetadata);

        log.trace(
                "Transaction ID: " + tx.getStartTimestamp() + "\n" +
                "Child Binding Set:\n" + childBindingSet + "\n");

        // The Row ID for the Aggregation State that needs to be updated is defined by the Group By variables.
        final String aggregationNodeId = aggregationMetadata.getNodeId();
        final VariableOrder groupByVars = aggregationMetadata.getGroupByVariableOrder();
        final Bytes rowId = makeRowKey(aggregationNodeId, groupByVars, childBindingSet);

        // Load the old state from the bytes if one was found; otherwise initialize the state.
        final Optional<Bytes> stateBytes = Optional.ofNullable( tx.get(rowId, FluoQueryColumns.AGGREGATION_BINDING_SET) );

        final AggregationState state;
        if(stateBytes.isPresent()) {
            // Deserialize the old state
            final byte[] bytes = stateBytes.get().toArray();
            state = AGG_STATE_SERDE.deserialize(bytes);
        } else {
            // Initialize a new state.
            state = new AggregationState();

            // If we have group by bindings, their values need to be added to the state's binding set.
            final MapBindingSet bindingSet = state.getBindingSet();
            for(final String variable : aggregationMetadata.getGroupByVariableOrder()) {
                bindingSet.addBinding( childBindingSet.getBinding(variable) );
            }
        }

        log.trace(
                "Transaction ID: " + tx.getStartTimestamp() + "\n" +
                "Before Update: " + LogUtils.clean(state.getBindingSet().toString()) + "\n");

        // Update the visibilities of the result binding set based on the child's visibilities.
        final String oldVisibility = state.getVisibility();
        final String updateVisibilities = VisibilitySimplifier.unionAndSimplify(oldVisibility, childBindingSet.getVisibility());
        state.setVisibility(updateVisibilities);

        // Update the Aggregation State with each Aggregation function included within this group.
        for(final AggregationElement aggregation : aggregationMetadata.getAggregations()) {
            final AggregationType type = aggregation.getAggregationType();
            final AggregationFunction function = FUNCTIONS.get(type);
            if(function == null) {
                throw new RuntimeException("Unrecognized aggregation function: " + type);
            }

            function.update(aggregation, state, childBindingSet);
        }

        log.trace(
                "Transaction ID: " + tx.getStartTimestamp() + "\n" +
                "After Update:" + LogUtils.clean(state.getBindingSet().toString()) + "\n" );

        // Store the updated state. This will write on top of any old state that was present for the Group By values.
        tx.set(rowId, FluoQueryColumns.AGGREGATION_BINDING_SET, Bytes.of(AGG_STATE_SERDE.serialize(state)));
    }

    /**
     * A function that updates an {@link AggregationState}.
     */
    public static interface AggregationFunction {

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

    /**
     * Increments the {@link AggregationState}'s count if the child Binding Set contains the binding name
     * that is being counted by the {@link AggregationElement}.
     */
    public static final class CountFunction implements AggregationFunction {
        @Override
        public void update(final AggregationElement aggregation, final AggregationState state, final VisibilityBindingSet childBindingSet) {
            checkArgument(aggregation.getAggregationType() == AggregationType.COUNT, "The CountFunction only accepts COUNT AggregationElements.");

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

    /**
     * Add to the {@link AggregationState}'s sum if the child Binding Set contains the binding name
     * that is being summed by the {@link AggregationElement}.
     */
    public static final class SumFunction implements AggregationFunction {
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

    /**
     * Update the {@link AggregationState}'s average if the child Binding Set contains the binding name
     * that is being averaged by the {@link AggregationElement}.
     */
    public static final class AverageFunction implements AggregationFunction {
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

    /**
     * Update the {@link AggregationState}'s max if the child binding Set contains the binding name that is being
     * maxed by the {@link AggregationElement}.
     */
    public static final class MaxFunction implements AggregationFunction {

        private final ValueComparator compare = new ValueComparator();

        @Override
        public void update(final AggregationElement aggregation, final AggregationState state, final VisibilityBindingSet childBindingSet) {
            checkArgument(aggregation.getAggregationType() == AggregationType.MAX, "The MaxFunction only accepts MAX AggregationElements.");

            // Only update the max if the child contains the binding that we are finding the max value for.
            final String aggregatedName = aggregation.getAggregatedBindingName();
            if(childBindingSet.hasBinding(aggregatedName)) {
                final MapBindingSet result = state.getBindingSet();
                final String resultName = aggregation.getResultBindingName();
                final boolean newBinding = !result.hasBinding(resultName);

                Value max;
                if(newBinding) {
                    max = childBindingSet.getValue(aggregatedName);
                } else {
                    final Value oldMax = result.getValue(resultName);
                    final Value childMax = childBindingSet.getValue(aggregatedName);
                    max = compare.compare(childMax, oldMax) > 0 ? childMax : oldMax;
                }

                result.addBinding(resultName, max);
            }
        }
    }

    /**
     * Update the {@link AggregationState}'s min if the child binding Set contains the binding name that is being
     * mined by the {@link AggregationElement}.
     */
    public static final class MinFunction implements AggregationFunction {

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

    /**
     * Reads/Writes instances of {@link AggregationState} to/from bytes.
     */
    public static interface AggregationStateSerDe {

        /**
         * @param state - The state that will be serialized. (not null)
         * @return The state serialized to a byte[].
         */
        public byte[] serialize(AggregationState state);

        /**
         * @param bytes - The bytes that will be deserialized. (not null)
         * @return The {@link AggregationState} that was read from the bytes.
         */
        public AggregationState deserialize(byte[] bytes);
    }

    /**
     * An implementation of {@link AggregationStateSerDe} that uses Java Serialization.
     */
    public static final class ObjectSerializationAggregationStateSerDe implements AggregationStateSerDe {

        @Override
        public byte[] serialize(final AggregationState state) {
            requireNonNull(state);

            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try(final ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(state);
            } catch (final IOException e) {
                throw new RuntimeException("A problem was encountered while serializing an AggregationState object.", e);
            }

            return baos.toByteArray();
        }

        @Override
        public AggregationState deserialize(final byte[] bytes) {
            requireNonNull(bytes);

            final AggregationState state;

            final ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            try(ValidatingObjectInputStream vois = new ValidatingObjectInputStream(bais)
            //// this is how you find classes that you missed in the vois.accept() list, below.
            // { @Override protected void invalidClassNameFound(String className) throws java.io.InvalidClassException {
            // System.out.println("vois.accept(" + className + ".class, ");};};
                        ) {
                // These classes are allowed to be deserialized. Others throw InvalidClassException.
                vois.accept(org.apache.rya.indexing.pcj.fluo.app.AggregationResultUpdater.AggregationState.class, //
                                org.apache.rya.indexing.pcj.fluo.app.AggregationResultUpdater.AverageState.class, //
                                java.util.HashMap.class, //
                                java.math.BigInteger.class, //
                                java.lang.Number.class, //
                                java.math.BigDecimal.class, //
                                org.openrdf.query.impl.MapBindingSet.class, //
                                java.util.LinkedHashMap.class, //
                                org.openrdf.query.impl.BindingImpl.class, //
                                org.openrdf.model.impl.URIImpl.class, //
                                org.openrdf.model.impl.LiteralImpl.class, //
                                org.openrdf.model.impl.DecimalLiteralImpl.class, //
                                org.openrdf.model.impl.IntegerLiteralImpl.class);
                vois.accept("[B"); // Array of Bytes
                final Object o = vois.readObject();
                if(o instanceof AggregationState) {
                    state = (AggregationState)o;
                } else {
                    throw new RuntimeException("A problem was encountered while deserializing an AggregationState object. Wrong class.");
                }
            } catch (final IOException | ClassNotFoundException e) {
                throw new RuntimeException("A problem was encountered while deserializing an AggregationState object.", e);
            }

            return state;
        }
    }

    /**
     * Keeps track information required to update and build the resulting Binding Set for a set of Group By values.
     */
    public static final class AggregationState implements Serializable {
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

    /**
     * The Sum and Count of the values that are being averaged. The average itself is derived from these values.
     */
    public static class AverageState implements Serializable {
        private static final long serialVersionUID = 1L;

        private final BigDecimal sum;
        private final BigInteger count;

        /**
         * Constructs an instance of {@link AverageState} where the count and sum start at 0.
         */
        public AverageState() {
            sum = BigDecimal.ZERO;
            count = BigInteger.ZERO;
        }

        /**
         * Constructs an instance of {@link AverageState}.
         *
         * @param sum - The sum of the values that are averaged. (not null)
         * @param count - The number of values that are averaged. (not null)
         */
        public AverageState(final BigDecimal sum, final BigInteger count) {
            this.sum = requireNonNull(sum);
            this.count = requireNonNull(count);
        }

        /**
         * @return The sum of the values that are averaged.
         */
        public BigDecimal getSum() {
            return sum;
        }

        /**
         * @return The number of values that are averaged.
         */
        public BigInteger getCount() {
            return count;
        }

        @Override
        public int hashCode() {
            return Objects.hash(sum, count);
        }

        @Override
        public boolean equals(final Object o) {
            if(o instanceof AverageState) {
                final AverageState state = (AverageState) o;
                return Objects.equals(sum, state.sum) &&
                        Objects.equals(count, state.count);
            }
            return false;
        }

        @Override
        public String toString() {
            return "Sum: " + sum + " Count: " + count;
        }
    }
}