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
import org.apache.rya.api.function.projection.ProjectionEvaluator;
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
 * Supplies {@link ProjectionProcessor} instances.
 */
@DefaultAnnotation(NonNull.class)
public class ProjectionProcessorSupplier extends RyaStreamsProcessorSupplier {

    private final ProjectionEvaluator projection;

    /**
     * Constructs an instance of {@link ProjectionProcessorSupplier}.
     *
     * @param projection - Defines the projection work that will be performed by supplied processors. (not null)
     * @param resultFactory - The factory that the supplied processors will use to create results. (not null)
     */
    public ProjectionProcessorSupplier(
            final ProjectionEvaluator projection,
            final ProcessorResultFactory resultFactory) {
        super(resultFactory);
        this.projection = requireNonNull(projection);
    }

    @Override
    public Processor<Object, ProcessorResult> get() {
        return new ProjectionProcessor(projection, super.getResultFactory());
    }

    /**
     * Evaluates {@link ProcessorResult}s against a {@link ProjectionEvaluator} and forwards its result
     * to a downstream processor.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class ProjectionProcessor extends RyaStreamsProcessor {
        private static final Logger log = LoggerFactory.getLogger(ProjectionProcessor.class);

        private final ProjectionEvaluator projection;

        private ProcessorContext context;

        /**
         * Constructs an instance of {@link ProjectionProcessor}.
         *
         * @param projection - Defines the projection work that will be performed by this processor. (not null)
         * @param resultFactory - The factory that the processor will use to create results. (not null)
         */
        public ProjectionProcessor(final ProjectionEvaluator projection, final ProcessorResultFactory resultFactory) {
            super(resultFactory);
            this.projection = requireNonNull(projection);
        }

        @Override
        public void init(final ProcessorContext context) {
            this.context = context;
        }

        @Override
        public void process(final Object key, final ProcessorResult result) {
            // projections can only be unary
            if (result.getType() != ResultType.UNARY) {
                throw new RuntimeException("The ProcessorResult to be processed must be Unary.");
            }

            // Apply the projection to the binding set.
            final VisibilityBindingSet bs = result.getUnary().getResult();
            final VisibilityBindingSet newVisBs = projection.project(bs);

            // Forward the result to the downstream processor.
            log.debug("\nOUTPUT:\n{}", newVisBs);
            context.forward(key, super.getResultFactory().make(newVisBs));
        }

        @Override
        public void punctuate(final long timestamp) {
            // Nothing to do.
        }

        @Override
        public void close() {
            // Nothing to do.
        }
    }
}