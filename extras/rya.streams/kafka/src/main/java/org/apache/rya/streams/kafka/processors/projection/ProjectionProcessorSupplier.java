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
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.ResultType;
import org.apache.rya.streams.kafka.processors.ProcessorResult.UnaryResult;
import org.apache.rya.streams.kafka.processors.ProcessorResultFactory;
import org.apache.rya.streams.kafka.processors.RyaStreamsProcessorSupplier;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.ProjectionElemList;
import org.openrdf.query.impl.MapBindingSet;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Supplies {@link ProjectionProcessor} instances.
 */
@DefaultAnnotation(NonNull.class)
public class ProjectionProcessorSupplier extends RyaStreamsProcessorSupplier {

    private final ProjectionElemList projectionElems;

    /**
     * Constructs an instance of {@link ProjectionProcessorSupplier}.
     *
     * @param projectionElems - The {@link ProjectionElemList} that defines which bindings get forwarded or changed. (not null)
     * @param resultFactory - The factory that the supplied processors will use to create results. (not null)
     */
    public ProjectionProcessorSupplier(
            final ProjectionElemList projectionElems,
            final ProcessorResultFactory resultFactory) {
        super(resultFactory);
        this.projectionElems = requireNonNull(projectionElems);
    }

    @Override
    public Processor<Object, ProcessorResult> get() {
        return new ProjectionProcessor(projectionElems, super.getResultFactory());
    }

    /**
     * Evaluates {@link ProcessorResult}s against a {@link Projection}.  Any results found in
     * the {@link ProjectionElemList} will be modified and/or forwarded.  A {@link ProjectionElemList} defines
     * a source and target name for a binding, so if a binding name appears in the source list of the {@link ProjectionElemList},
     * then it will be renamed with the associated target name.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class ProjectionProcessor implements Processor<Object, ProcessorResult> {

        private final ProjectionElemList projectionElems;
        private final ProcessorResultFactory resultFactory;

        private ProcessorContext context;

        /**
         * Constructs an instance of {@link ProjectionProcessor}.
         *
         * @param projectionElems - The projection elems that will determine what to do with the bindings. (not null)
         * @param resultFactory - The factory that the processor will use to create results. (not null)
         */
        public ProjectionProcessor(final ProjectionElemList projectionElems, final ProcessorResultFactory resultFactory) {
            this.projectionElems = requireNonNull(projectionElems);
            this.resultFactory = requireNonNull(resultFactory);
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

            final UnaryResult unary = result.getUnary();
            final VisibilityBindingSet bindingSet = unary.getResult();

            final MapBindingSet newBindingSet = new MapBindingSet(bindingSet.size());
            for (final ProjectionElem elem : projectionElems.getElements()) {
                if (bindingSet.hasBinding(elem.getSourceName())) {
                    newBindingSet.addBinding(elem.getTargetName(), bindingSet.getValue(elem.getSourceName()));
                }
            }

            // wrap the new binding set with the original's visibility.
            final VisibilityBindingSet newVisiSet = new VisibilityBindingSet(newBindingSet, bindingSet.getVisibility());
            final ProcessorResult resultValue = resultFactory.make(newVisiSet);
            context.forward(key, resultValue);
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