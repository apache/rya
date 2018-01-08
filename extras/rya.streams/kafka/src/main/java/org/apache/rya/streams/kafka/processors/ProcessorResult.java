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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.kafka.streams.processor.Processor;
import org.apache.rya.api.model.VisibilityBindingSet;

import com.google.common.base.Optional;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Represents a value that is emitted from a Rya Streams {@link Processor}. We can't just emit a
 * {@link VisibilityBindingSet} because some downstream processors require more information about
 * which upstream processor is emitting the result in order to do their work.
 * </p>
 * Currently there are only two types of processors:
 * <ul>
 *   <li>Unary Processor - A processor that only has a single upstream node feeding it input.</li>
 *   <li>Binary Processor - A processor that has two upstream nodes feeding it input.</li>
 * </ul>
 * If a processor is emitting to a unary processor, then use {@link #make(UnaryResult)} to create its
 * result. If it is emitting to a binary processor, then use {@link #make(BinaryResult)}.
 */
@DefaultAnnotation(NonNull.class)
public class ProcessorResult {

    private final ResultType type;
    private final Optional<UnaryResult> unary;
    private final Optional<BinaryResult> binary;

    /**
     * Constructs an instance of {@link ProcessorResult}. Private to force users to use the static factory methods.
     *
     * @param type - Indicates the type of result this object holds. (not null)
     * @param unary - The unary result if that is this object's type. (not null)
     * @param binary - The binary result if that is this object's type. (not null)
     */
    private  ProcessorResult(
            final ResultType type,
            final Optional<UnaryResult> unary,
            final Optional<BinaryResult> binary) {
        this.type = requireNonNull(type);
        this.unary= requireNonNull(unary);
        this.binary= requireNonNull(binary);
    }

    /**
     * @return Indicates the type of result this object holds.
     */
    public ResultType getType() {
        return type;
    }

    /**
     * @return The unary result if that is this object's type.
     * @throws IllegalStateException If this object's type is not {@link ResultType#UNARY}.
     */
    public UnaryResult getUnary() throws IllegalStateException {
        checkState(type == ResultType.UNARY, "The ResultType must be " + ResultType.UNARY + " to invoke this method, " +
                "but it is " + type + ".");
        return unary.get();
    }

    /**
     * @return The binary result if that is this object's type.
     * @throws IllegalStateException If this object's type is not {@link ResultType#BINARY}.
     */
    public BinaryResult getBinary() throws IllegalStateException {
        checkState(type == ResultType.BINARY, "The ResultType must be " + ResultType.BINARY + " to invoke this method, " +
                "but it is " + type + ".");
        return binary.get();
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, unary, binary);
    }

    @Override
    public boolean equals(final Object o) {
        if(o instanceof ProcessorResult) {
            final ProcessorResult other = (ProcessorResult) o;
            return Objects.equals(type, other.type) &&
                    Objects.equals(unary, other.unary) &&
                    Objects.equals(binary, other.binary);
        }
        return false;
    }


    /**
     * Creates a {@link ProcessorResult} using the supplied value.
     *
     * @param result - The result that will be held by the created object. (not null)
     * @return An object holding the provided result.
     */
    public static ProcessorResult make(final UnaryResult result) {
        requireNonNull(result);
        return new ProcessorResult(ResultType.UNARY, Optional.of(result), Optional.absent());
    }

    /**
     * Creates a {@link ProcessorResult} using the supplied value.
     *
     * @param result - The result that will be held by the created object. (not null)
     * @return An object holding the provided result.
     */
    public static ProcessorResult make(final BinaryResult result) {
        requireNonNull(result);
        return new ProcessorResult(ResultType.BINARY, Optional.absent(), Optional.of(result));
    }

    /**
     * Indicates the type of result held by a {@link ProcessorResult}.
     */
    public static enum ResultType {
        /**
         * The {@link ProcessorResult} holds a {@link UnaryResult}.
         */
        UNARY,

        /**
         * The {@link ProcessorResult} holds a {@link BinaryResult}.
         */
        BINARY;
    }

    /**
     * The result of a Rya Streams {@link Processor} whose downstream processor is unary.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class UnaryResult {
        private final VisibilityBindingSet result;

        /**
         * Constructs an instance of {@link UnaryResult}.
         *
         * @param result - The binding set that is being emitted to the downstream unary processor. (not null)
         */
        public UnaryResult(final VisibilityBindingSet result) {
            this.result = requireNonNull(result);
        }

        /**
         * @return The binding set that is being emitted to the downstream unary processor.
         */
        public VisibilityBindingSet getResult() {
            return result;
        }

        @Override
        public int hashCode() {
            return Objects.hash(result);
        }

        @Override
        public boolean equals(final Object o) {
            if(o instanceof UnaryResult) {
                final UnaryResult other = (UnaryResult) o;
                return Objects.equals(result, other.result);
            }
            return false;
        }
    }

    /**
     * The result of a Rya Streams {@link Processor} whose downstream processor is binary.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class BinaryResult {
        private final Side side;
        private final VisibilityBindingSet result;

        /**
         * Constructs an instance of {@link BinaryResult}.
         *
         * @param side - Which side of the downstream binary processor the result is being emitted to. (not null)
         * @param result - The binding set that is being emitted to the downstream binary processor. (not null)
         */
        public BinaryResult(final Side side, final VisibilityBindingSet result) {
            this.side = requireNonNull(side);
            this.result = requireNonNull(result);
        }

        /**
         * @return Which side of the downstream binary processor the result is being emitted to.
         */
        public Side getSide() {
            return side;
        }

        /**
         * @return The binding set that is being emitted to the downstream binary processor.
         */
        public VisibilityBindingSet getResult() {
            return result;
        }

        @Override
        public int hashCode() {
            return Objects.hash(side, result);
        }

        @Override
        public boolean equals(final Object o) {
            if(o instanceof BinaryResult) {
                final BinaryResult other = (BinaryResult) o;
                return Objects.equals(side, other.side) &&
                        Objects.equals(result, other.result);
            }
            return false;
        }

        /**
         * A label that is used to by the downstream binary prcoessor to distinguish which upstream processor
         * produced the {@link BinaryResult}.
         */
        public static enum Side {
            /**
             * The result is being emitted from the "left" upstream processor.
             */
            LEFT,

            /**
             * The result is being emitted from the "right" upstream processor.
             */
            RIGHT;
        }
    }
}