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
package org.apache.rya.streams.kafka.processors.filter;

import static java.util.Objects.requireNonNull;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.rya.api.function.filter.FilterEvaluator;
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
 * Supplies {@link FilterProcessor} instances.
 */
@DefaultAnnotation(NonNull.class)
public class FilterProcessorSupplier extends RyaStreamsProcessorSupplier {
    private static final Logger log = LoggerFactory.getLogger(FilterProcessorSupplier.class);

    private final FilterEvaluator filter;

    /**
     * Constructs an instance of {@link FilterProcessorSupplier}.
     *
     * @param filter - Defines the filter the supplied processors will evaluate. (not null)
     * @param resultFactory - The factory that the supplied processors will use to create results. (not null)
     */
    public FilterProcessorSupplier(
            final FilterEvaluator filter,
            final ProcessorResultFactory resultFactory) {
        super(resultFactory);
        this.filter = requireNonNull(filter);
    }

    @Override
    public Processor<Object, ProcessorResult> get() {
        return new FilterProcessor(filter, super.getResultFactory());
    }

    /**
     * Evaluates {@link ProcessorResult}s against a {@link FilterEvaluator} and forwards the original result
     * to a downstream processor if it passes the filter's condition.
     */
    @DefaultAnnotation(NonNull.class)
    public static class FilterProcessor extends RyaStreamsProcessor {

        private final FilterEvaluator filter;
        private ProcessorContext context;

        /**
         * Constructs an instance of {@link FilterProcessor}.
         *
         * @param filter - Defines the filter the supplied processor will evaluate. (not null)
         * @param resultFactory - The factory that the processor will use to create results. (not null)
         */
        public FilterProcessor(
                final FilterEvaluator filter,
                final ProcessorResultFactory resultFactory) {
            super(resultFactory);
            this.filter = requireNonNull(filter);
        }

        @Override
        public void init(final ProcessorContext context) {
            this.context = context;
        }

        @Override
        public void process(final Object key, final ProcessorResult value) {
            // Filters can only be unary.
            if (value.getType() != ResultType.UNARY) {
                throw new RuntimeException("The ProcessorResult to be processed must be Unary.");
            }

            // If the value's binding set passes the filter, then forward it to the downstream processor.
            final VisibilityBindingSet bindingSet = value.getUnary().getResult();
            log.debug("\nINPUT:\n{}", bindingSet);
            if(filter.filter(bindingSet)) {
                log.debug("\nOUTPUT:\n{}", bindingSet);
                final ProcessorResult result = super.getResultFactory().make(bindingSet);
                context.forward(key, result);
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