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
package org.apache.rya.streams.kafka.processors.output;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.streams.kafka.processors.ProcessorResult;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Supplies {@link BindingSetOutputFormatter} instances.
 */
@DefaultAnnotation(NonNull.class)
public class BindingSetOutputFormatterSupplier implements ProcessorSupplier<Object, ProcessorResult> {

    @Override
    public Processor<Object, ProcessorResult> get() {
        return new BindingSetOutputFormatter();
    }

    /**
     * Accepts {@link ProcessorResult}s and forwards just their {@link VisibilityBindingSet} so that it may be
     * written to a sink.
     */
    @DefaultAnnotation(NonNull.class)
    public static final class BindingSetOutputFormatter implements Processor<Object, ProcessorResult> {

        private ProcessorContext processorContext;

        @Override
        public void init(final ProcessorContext context) {
            processorContext = context;
        }

        @Override
        public void process(final Object key, final ProcessorResult value) {

            VisibilityBindingSet result = null;
            switch(value.getType()) {
                case UNARY:
                    result = value.getUnary().getResult();
                    break;

                case BINARY:
                    result = value.getBinary().getResult();
                    break;
            }

            if(result != null) {
                processorContext.forward(key, result);
            }
        }

        @Override
        public void punctuate(final long timestamp) {
            // Does nothing.
        }

        @Override
        public void close() {
            // Does nothing.
        }
    }
}