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
package org.apache.rya.streams.kafka.processors.projection;

import static java.util.Objects.requireNonNull;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.rya.api.function.projection.MultiProjectionEvaluator;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.ResultType;
import org.apache.rya.streams.kafka.processors.ProcessorResultFactory;
import org.apache.rya.streams.kafka.processors.RyaStreamsProcessor;
import org.apache.rya.streams.kafka.processors.RyaStreamsProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Supplies {@link MultiProjectionProcessor} instances.
 */
@DefaultAnnotation(NonNull.class)
public class MultiProjectionProcessorSupplier extends RyaStreamsProcessorSupplier {

    private final MultiProjectionEvaluator multiProjection;

    /**
     * Constructs an instance of {@link MultiProjectionProcessorSupplier}.
     *
     * @param multiProjection - Defines the MultiProjection work that will be performed by supplied processors. (not null)
     * @param resultFactory - The factory that the supplied processors will use to create results. (not null)
     */
    public MultiProjectionProcessorSupplier(
            final MultiProjectionEvaluator multiProjection,
            final ProcessorResultFactory resultFactory) {
        super(resultFactory);
        this.multiProjection = requireNonNull(multiProjection);
    }

    @Override
    public Processor<Object, ProcessorResult> get() {
        return new MultiProjectionProcessor(multiProjection, super.getResultFactory());
    }

    /**
     * Evaluates {@link ProcessorResult}s against a {@link MultiProjectionEvaluator} and forwards its results
     * to a downstream processor.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class MultiProjectionProcessor extends RyaStreamsProcessor {
        private static final Logger log = LoggerFactory.getLogger(MultiProjectionProcessor.class);

        private final MultiProjectionEvaluator multiProjection;

        private ProcessorContext context;

        /**
         * Constructs an instance of {@link MultiProjectionProcessor}.
         *
         * @param multiProjection - Defines the MultiProjection work that will be performed by this processor. (not null)
         * @param resultFactory - The factory that the supplied processors will use to create results. (not null)
         */
        public MultiProjectionProcessor(
                final MultiProjectionEvaluator multiProjection,
                final ProcessorResultFactory resultFactory) {
            super(resultFactory);
            this.multiProjection = requireNonNull(multiProjection);
        }

        @Override
        public void init(final ProcessorContext context) {
            this.context = context;
        }

        @Override
        public void process(final Object key, final ProcessorResult value) {
            // projections can only be unary
            if (value.getType() != ResultType.UNARY) {
                throw new RuntimeException("The ProcessorResult to be processed must be Unary.");
            }

            // Apply the projection to the binding set and forward the results.
            final VisibilityBindingSet bs = value.getUnary().getResult();
            for(final VisibilityBindingSet newVisBs : multiProjection.project(bs)) {
                log.debug("\nOUTPUT:\n{}", newVisBs);
                context.forward(key, super.getResultFactory().make(newVisBs));
            }
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