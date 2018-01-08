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
package org.apache.rya.streams.kafka.processors.join;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.rya.api.function.join.IterativeJoin;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.utils.CloseableIterator;
import org.apache.rya.streams.kafka.processors.ProcessorResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.BinaryResult;
import org.apache.rya.streams.kafka.processors.ProcessorResult.BinaryResult.Side;
import org.apache.rya.streams.kafka.processors.ProcessorResultFactory;
import org.apache.rya.streams.kafka.processors.RyaStreamsProcessor;
import org.apache.rya.streams.kafka.processors.RyaStreamsProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Supplies {@link JoinProcessor} instances.
 */
@DefaultAnnotation(NonNull.class)
public class JoinProcessorSupplier extends RyaStreamsProcessorSupplier {

    private final String stateStoreName;
    private final IterativeJoin join;
    private final List<String> joinVars;
    private final List<String> allVars;

    /**
     * Constructs an instance of {@link JoinProcessorSupplier}.
     *
     * @param stateStoreName - The name of the state store the processor will use. (not null)
     * @param join - The join function the supplied processor will use. (not null)
     * @param joinVars - The variables that the supplied processor will join over. (not null)
     * @param allVars - An ordered list of all the variables that may appear in resulting Binding Sets.
     *   This list must lead with the same variables and order as {@code joinVars}. (not null)
     * @param resultFactory - The factory that the supplied processors will use to create results. (not null)
     * @throws IllegalArgumentException Thrown if {@code allVars} does not start with {@code joinVars}.
     */
    public JoinProcessorSupplier(
            final String stateStoreName,
            final IterativeJoin join,
            final List<String> joinVars,
            final List<String> allVars,
            final ProcessorResultFactory resultFactory) throws IllegalArgumentException {
        super(resultFactory);
        this.stateStoreName = requireNonNull(stateStoreName);
        this.join = requireNonNull(join);
        this.joinVars = requireNonNull(joinVars);
        this.allVars = requireNonNull(allVars);

        if(!allVars.subList(0, joinVars.size()).equals(joinVars)) {
            throw new IllegalArgumentException("The allVars list must start with the joinVars list, but it did not. " +
                    "Join Vars: " + joinVars + ", All Vars: " + allVars);
        }
    }

    @Override
    public Processor<Object, ProcessorResult> get() {
        return new JoinProcessor(stateStoreName, join, joinVars, allVars, super.getResultFactory());
    }

    /**
     * Joins {@link VisibilityBindingSet}s against all binding sets that were emitted on the other side. This function
     * does not have an age off policy, so it will match everything that could have ever possibly matched, however this
     * may become prohibitive for joins that match a large volume of binding sets since this will indefinitely grow
     * within the state store.
     */
    @DefaultAnnotation(NonNull.class)
    public static class JoinProcessor extends RyaStreamsProcessor {

        private static final Logger log = LoggerFactory.getLogger(JoinProcessor.class);

        private final String stateStoreName;
        private final IterativeJoin join;
        private final List<String> joinVars;
        private final List<String> allVars;
        private final ProcessorResultFactory resultFactory;

        private ProcessorContext context;
        private JoinStateStore joinStateStore;

        /**
         * Constructs an instance of {@link JoinProcessor}.
         *
         * @param stateStoreName - The name of the state store the processor will use. (not null)
         * @param join - The join function that the processor will use. (not null)
         * @param joinVars - The variables that the processor will join over. (not null)
         * @param allVars - An ordered list of all the variables that may appear in resulting Binding Sets.
         *   This list must lead with the same variables and order as {@code joinVars}. (not null)
         * @param resultFactory - The factory that will format this processor's final results
         *   for the downstream processor. (not null)
         */
        public JoinProcessor(
                final String stateStoreName,
                final IterativeJoin join,
                final List<String> joinVars,
                final List<String> allVars,
                final ProcessorResultFactory resultFactory) {
            super(resultFactory);
            this.stateStoreName = requireNonNull(stateStoreName);
            this.join = requireNonNull(join);
            this.joinVars = requireNonNull(joinVars);
            this.allVars = requireNonNull(allVars);
            this.resultFactory = requireNonNull(resultFactory);

            if(!allVars.subList(0, joinVars.size()).equals(joinVars)) {
                throw new IllegalArgumentException("All vars must be lead by the join vars, but it did not. " +
                        "Join Vars: " + joinVars + ", All Vars: " + allVars);
            }
        }

        @Override
        public void init(final ProcessorContext context) {
            // Hold onto the context so that we can foward results.
            this.context = context;

            // Get a reference to the state store that keeps track of what can be joined with.
            final KeyValueStore<String, VisibilityBindingSet> stateStore =
                    (KeyValueStore<String, VisibilityBindingSet>) context.getStateStore( stateStoreName );
            joinStateStore = new KeyValueJoinStateStore( stateStore, joinVars, allVars );
        }

        @Override
        public void process(final Object key, final ProcessorResult value) {
            // Log the key/value that have been encountered.
            log.debug("\nINPUT:\nSide: {}\nBinding Set: {}", value.getBinary().getSide(), value.getBinary().getResult());

            // Must be a binary result.
            final BinaryResult binary = value.getBinary();

            // Store the new result in the state store so that future joins may include it.
            joinStateStore.store(binary);

            // Fetch the binding sets that the emitted value joins with.
            try(final CloseableIterator<VisibilityBindingSet> otherSide = joinStateStore.getJoinedValues(binary)) {
                // Create an iterator that performs the join operation.
                final Iterator<VisibilityBindingSet> joinResults = binary.getSide() == Side.LEFT ?
                        join.newLeftResult(binary.getResult(), otherSide) :
                        join.newRightResult(otherSide, binary.getResult());

                // Format each join result and forward it to the downstream processor.
                while(joinResults.hasNext()) {
                    final VisibilityBindingSet joinResult = joinResults.next();
                    final ProcessorResult resultValue = resultFactory.make(joinResult);
                    log.debug("\nOUTPUT:\n{}", joinResult);
                    context.forward(key, resultValue);
                }
            } catch (final Exception e) {
                final String msg = "Problem encountered while iterating over the other side's values within the state store.";
                log.error(msg, e);
                throw new RuntimeException(msg, e);
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