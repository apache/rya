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

import java.util.Comparator;
import java.util.List;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.rya.api.function.aggregation.AggregationState;
import org.apache.rya.api.function.aggregation.AggregationStateStore;
import org.apache.rya.api.function.aggregation.AggregationsEvaluator;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.ResultType;
import org.apache.rya.streams.kafka.processors.ProcessorResultFactory;
import org.apache.rya.streams.kafka.processors.RyaStreamsProcessor;
import org.apache.rya.streams.kafka.processors.RyaStreamsProcessorSupplier;
import org.openrdf.query.algebra.Group;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Supplies {@link AggregationProcessor} instances.
 */
@DefaultAnnotation(NonNull.class)
public class AggregationProcessorSupplier extends RyaStreamsProcessorSupplier {

    private final String stateStoreName;
    private final Group aggNode;

    /**
     * Constructs an instance of {@link AggregationProcessorSupplier}.
     *
     * @param stateStoreName - The name of the state store the processor will use. (not null)
     * @param aggNode - Defines which aggregations will be performed by the processor. (not null)
     * @param resultFactory - The factory that the supplied processors will use to create results. (not null)
     */
    public AggregationProcessorSupplier(
            final String stateStoreName,
            final Group aggNode,
            final ProcessorResultFactory resultFactory) {
        super(resultFactory);
        this.stateStoreName = requireNonNull(stateStoreName);
        this.aggNode = requireNonNull(aggNode);
    }

    @Override
    public Processor<Object, ProcessorResult> get() {
        return new AggregationProcessor(stateStoreName, aggNode, super.getResultFactory());
    }

    /**
     * Evaluates a {@link Group} node that contains a bunch of aggregations. Each aggregation will have a binding
     * within the resulting binding sets that contains the aggregation value.
     *
     * @see AggregationsEvaluator
     */
    @DefaultAnnotation(NonNull.class)
    public static class AggregationProcessor extends RyaStreamsProcessor {
        private static final Logger log = LoggerFactory.getLogger(AggregationProcessor.class);

        private final String stateStoreName;
        private final Group aggNode;

        private ProcessorContext context;
        private AggregationStateStore aggStateStore;
        private AggregationsEvaluator evaluator;

        /**
         * Constructs an instance of {@link AggregationProcessor}.
         *
         * @param stateStoreName - The name of the Kafka Streams state store that this processor will use. (not null)
         * @param aggNode - The group by node that configures how the aggregations will be performed. (not null)
         * @param resultFactory - The factory that will format this processor's final results for the downstream
         *   processor. (not null)
         */
        public AggregationProcessor(
                final String stateStoreName,
                final Group aggNode,
                final ProcessorResultFactory resultFactory) {
            super(resultFactory);
            this.stateStoreName = requireNonNull(stateStoreName);
            this.aggNode = requireNonNull(aggNode);
        }

        @Override
        public void init(final ProcessorContext context) {
            this.context = context;

            // Sort the group by vars so that they will be written to the state store in the same order every time.
            final List<String> groupByVars = Lists.newArrayList(aggNode.getGroupBindingNames());
            groupByVars.sort(Comparator.naturalOrder());

            // Get a reference to the state store that keeps track of aggregation state.
            final KeyValueStore<String, AggregationState> stateStore =
                    (KeyValueStore<String, AggregationState>) context.getStateStore( stateStoreName );
            aggStateStore = new KeyValueAggregationStateStore(stateStore, groupByVars);

            // Create the aggregation evaluator.
            evaluator = AggregationsEvaluator.make(aggStateStore, aggNode, groupByVars);
        }

        @Override
        public void process(final Object key, final ProcessorResult value) {
            // Aggregations can only be unary.
            if (value.getType() != ResultType.UNARY) {
                throw new RuntimeException("The ProcessorResult to be processed must be Unary.");
            }

            // Log the binding set that has been input.
            log.debug("\nINPUT:\nBinding Set: {}", value.getUnary().getResult());

            // Update the aggregations values.
            final VisibilityBindingSet resultBs = evaluator.update(value.getUnary().getResult());

            // Log the binding set that will be output.
            log.debug("\nOUTPUT:\nBinding Set: {}", resultBs);

            // Forward to the updated aggregation binding set to the downstream processors.
            context.forward(key, super.getResultFactory().make(resultBs));
        }

        @Override
        public void punctuate(final long timestamp) {
            // Do nothing.
        }

        @Override
        public void close() {
            // Do nothing.
        }
    }
}