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
package org.apache.rya.streams.kafka.processors;

import static java.util.Objects.requireNonNull;

import java.util.Optional;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.rya.api.function.sp.StatementPatternMatcher;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.model.VisibilityStatement;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Supplies {@link StatementPatternProcessor} instances.
 */
@DefaultAnnotation(NonNull.class)
public class StatementPatternProcessorSupplier implements ProcessorSupplier<String, VisibilityStatement> {

    private final StatementPattern sp;
    private final ProcessorResultFactory resultFactory;

    /**
     * Constructs an instance of {@link StatementPatternProcessorSupplier}.
     *
     * @param sp - The statement pattern that the supplied processors will match against. (not null)
     * @param resultFactory - The factory that the supplied processors will use to create results. (not null)
     */
    public StatementPatternProcessorSupplier(
            final StatementPattern sp,
            final ProcessorResultFactory resultFactory) {
        this.sp = requireNonNull(sp);
        this.resultFactory = requireNonNull(resultFactory);
    }

    @Override
    public Processor<String, VisibilityStatement> get() {
        return new StatementPatternProcessor(sp, resultFactory);
    }

    /**
     * Evaluates {@link VisibilityStatement}s against a {@link StatementPattern}. Any who match the pattern
     * will forward a {@link VisibilityBindingSet} to the downstream processor.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class StatementPatternProcessor implements Processor<String, VisibilityStatement> {

        private static final Logger log = LoggerFactory.getLogger(StatementPatternProcessor.class);

        private final StatementPatternMatcher spMatcher;
        private final ProcessorResultFactory resultFactory;

        private ProcessorContext context;

        /**
         * Constructs an instance of {@link StatementPatternProcessor}.
         *
         * @param sp - The statement pattern that the processor will match statements against. (not null)
         * @param resultFactory - The factory that the processor will use to create results. (not null)
         */
        public StatementPatternProcessor(
                final StatementPattern sp,
                final ProcessorResultFactory resultFactory) {
            this.spMatcher = new StatementPatternMatcher( requireNonNull(sp) );
            this.resultFactory = requireNonNull(resultFactory);
        }

        @Override
        public void init(final ProcessorContext context) {
            this.context = context;
        }

        @Override
        public void process(final String key, final VisibilityStatement statement) {
            log.debug("\nINPUT:\n{}\n", statement);

            // Check to see if the Statement matches the Statement Pattern.
            final Optional<BindingSet> bs = spMatcher.match(statement);

            if(bs.isPresent()) {
                // If it does, wrap the Binding Set with the Statement's visibility expression and write it to the state store.
                final VisibilityBindingSet visBs = new VisibilityBindingSet(bs.get(), statement.getVisibility());

                // Wrap the binding set as a result and forward it to the downstream processor.
                final ProcessorResult resultValue = resultFactory.make(visBs);
                log.debug("\nOUTPUT:\n{}", visBs);
                context.forward(key, resultValue);
            }
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
