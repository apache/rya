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

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Optional;

import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.utils.VisibilitySimplifier;
import org.apache.rya.api.function.aggregation.AggregationElement;
import org.apache.rya.api.function.aggregation.AggregationFunction;
import org.apache.rya.api.function.aggregation.AggregationState;
import org.apache.rya.api.function.aggregation.AggregationType;
import org.apache.rya.api.function.aggregation.AverageFunction;
import org.apache.rya.api.function.aggregation.AverageState;
import org.apache.rya.api.function.aggregation.CountFunction;
import org.apache.rya.api.function.aggregation.MaxFunction;
import org.apache.rya.api.function.aggregation.MinFunction;
import org.apache.rya.api.function.aggregation.SumFunction;
import org.apache.rya.api.log.LogUtils;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.fluo.app.query.AggregationMetadata;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
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
                vois.accept(AggregationState.class,
                                AverageState.class,
                                java.util.HashMap.class,
                                java.math.BigInteger.class,
                                java.lang.Number.class,
                                java.math.BigDecimal.class,
                                org.openrdf.query.impl.MapBindingSet.class,
                                java.util.LinkedHashMap.class,
                                org.openrdf.query.impl.BindingImpl.class,
                                org.openrdf.model.impl.URIImpl.class,
                                org.openrdf.model.impl.LiteralImpl.class,
                                org.openrdf.model.impl.DecimalLiteralImpl.class,
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
}